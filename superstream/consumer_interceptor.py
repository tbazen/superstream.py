import asyncio
import sys
import time
from typing import Any, Dict, List, Optional

from superstream.constants import _CONSUMER_CLIENT_TYPE, _SUPERSTREAM_CONNECTION_KEY
from superstream.core import Superstream
from superstream.utils import proto_to_json


class SuperstreamConsumerInterceptor:
    def __init__(self, config: Dict):
        self._superstream_config_ = Superstream.init_superstream_props(config, _CONSUMER_CLIENT_TYPE)

    def __update_topic_partitions(self, message):
        superstream: Superstream = self._superstream_config_.get(_SUPERSTREAM_CONNECTION_KEY)
        if superstream is None:
            return
        topic = message.topic()
        partition = message.partition()
        superstream.update_topic_partitions(topic, partition)

    def poll(self, message) -> Any:
        if message is None:
            return message
        return self.__intercept(message)

    def consume(self, messages) -> Optional[List[Any]]:
        if messages is None:
            return messages
        return [self.__intercept(message) for message in messages]

    def __intercept(self, message: Any) -> Any:
        if not message:
            return message
        self.__update_topic_partitions(message)
        try:
            return asyncio.run(self.__deserialize(message))
        except Exception as e:
            print(f"error deserializing message: {e!s}")
            return message

    async def __deserialize(self, message: Any) -> Any:
        superstream: Superstream = self._superstream_config_.get(_SUPERSTREAM_CONNECTION_KEY)
        message_value = message.value()
        headers = message.headers()

        if not headers or not message_value:
            return message

        superstream.client_counters.total_bytes_after_reduction += len(message_value)
        schema_id = None
        for key, value in headers:
            if key == "superstream_schema":
                schema_id = value.decode("utf-8") if isinstance(value, bytes) else value
                break

        if not schema_id:
            if superstream.superstream_ready:
                superstream.client_counters.total_bytes_before_reduction += len(message_value)
                superstream.client_counters.total_messages_failed_consume += 1
            return message

        wait_time = 60
        check_interval = 5

        for _ in range(0, wait_time, check_interval):
            if superstream.superstream_ready:
                break
            time.sleep(check_interval)

        if not superstream.superstream_ready:
            sys.stderr.write(
                "superstream: cannot connect with superstream and consume message that was modified by superstream"
            )
            return message

        descriptor = superstream.consumer_proto_desc_map.get(schema_id)
        if not descriptor:
            await superstream.send_get_schema_request(schema_id)
            descriptor = superstream.consumer_proto_desc_map.get(schema_id)
            if not descriptor:
                await superstream.handle_error(f"error getting schema with id: {schema_id}")
                print("superstream: shcema not found")
                return message

        try:
            deserialized_msg = proto_to_json(message_value, descriptor)
            superstream.client_counters.total_bytes_before_reduction += len(deserialized_msg)
            superstream.client_counters.total_messages_successfully_consumed += 1
            message.set_value(deserialized_msg.encode("utf-8"))
            return message
        except Exception as e:
            await superstream.handle_error(f"error deserializing data: {e!s}")
            return message
