import asyncio
from typing import Any, Dict, Union

import zstandard as zstd
from confluent_kafka import Producer

from superstream.constants import _PRODUCER_CLIENT_TYPE, _SUPERSTREAM_CONNECTION_KEY
from superstream.core import Superstream
from superstream.utils import _try_convert_to_json, json_to_proto


class SuperstreamProducerInterceptor(Producer):
    def __init__(self, config: Dict):
        super().__init__(config)
        self._compression_type = "zstd"
        self._superstream_config_ = Superstream.init_superstream_props(config, _PRODUCER_CLIENT_TYPE)

    def produce(self, *args, **kwargs):
        topic_index = 0
        value_index = 1
        partition_index = 3
        headers_index = 6
        superstream: Superstream = self._superstream_config_.get(_SUPERSTREAM_CONNECTION_KEY)
        if not superstream:
            super().produce(*args, **kwargs)
            return

        topic = args[topic_index]
        partition = args or 0 if len(args) > partition_index else kwargs.get("partition", 0)
        superstream.update_topic_partitions(topic, partition)

        if not superstream.superstream_ready:
            super().produce(*args, **kwargs)
            return

        msg = args[value_index] if len(args) > value_index else kwargs.get("value")
        json_msg = _try_convert_to_json(msg)
        if json_msg is None:
            super().produce(*args, **kwargs)
            return

        serialized_msg, superstream_headers = self._serialize(json_msg)

        if superstream_headers is not None:
            if len(args) > headers_index:
                if args[headers_index] is None:
                    args[headers_index] = superstream_headers
                elif isinstance(args[headers_index], dict):
                    args[headers_index].update(superstream_headers)
                elif isinstance(args[headers_index], list):
                    args[headers_index].extend(list(superstream_headers.items()))
            else:
                if kwargs.get("headers") is None:
                    kwargs["headers"] = superstream_headers
                elif isinstance(kwargs["headers"], dict):
                    kwargs["headers"].update(superstream_headers)
                elif isinstance(kwargs["headers"], list):
                    kwargs["headers"].extend(list(superstream_headers.items()))

        if len(args) > value_index:
            args = args[:value_index] + (serialized_msg,) + args[value_index + 1 :]
        elif "value" in kwargs:
            kwargs["value"] = serialized_msg

        super().produce(*args, **kwargs)

    def _serialize(self, json_msg: str) -> Union[bytes, Dict[str, Any]]:
        superstream: Superstream = self._superstream_config_.get(_SUPERSTREAM_CONNECTION_KEY)
        byte_msg = json_msg.encode("utf-8")
        headers: Dict[str, Any] = {}

        superstream.client_counters.total_bytes_before_reduction += len(byte_msg)

        if superstream.producer_proto_desc:
            try:
                byte_msg = json_to_proto(byte_msg, superstream.producer_proto_desc)
                superstream.client_counters.total_messages_successfully_produce += 1
                headers = {"superstream_schema": superstream.producer_schema_id}
            except Exception as e:
                superstream.handle_error(f"error serializing data: {e}")
                superstream.client_counters.total_messages_failed_produce += 1

        else:
            try:
                if superstream.learning_factor_counter <= superstream.learning_factor:
                    asyncio.run(superstream.send_learning_message(byte_msg))
                    superstream.learning_factor_counter += 1
                elif not superstream.learning_request_sent:
                    asyncio.run(superstream.send_register_schema_req())
            except Exception as e:
                superstream.handle_error(f"error sending learning message: {e}")
        if superstream.compression_enabled:
            try:
                byte_msg = self.compress(byte_msg, self._compression_type)
                headers.update({"compression": self._compression_type})
            except Exception as e:
                superstream.handle_error(f"error compressing data: {e}")

        superstream.client_counters.total_bytes_after_reduction += len(byte_msg)
        return byte_msg, headers

    def compress(self, data, compression_type) -> bytes:
        if not compression_type:
            return data

        if compression_type == "zstd":
            return zstd.compress(data)
        raise Exception(f"Unsupported compression type: {compression_type}")
