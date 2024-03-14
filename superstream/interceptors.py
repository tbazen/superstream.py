import asyncio
import time
from typing import Dict, List, Optional, Union

from confluent_kafka import Consumer, Message, Producer

import superstream.manager as manager
from superstream.client import _Client
from superstream.constants import _CONSUMER_CLIENT_TYPE, _PRODUCER_CLIENT_TYPE
from superstream.utils import _name, _try_convert_to_json, json_to_proto, proto_to_json


class _ConsumerInterceptor(Consumer):
    def __init__(self, client_id, config: Dict):
        if (config is None) or (isinstance(config, Dict) is False):
            raise Exception("superstream: invalid configuration object")
        super().__init__(config)
        self.__config__ = config
        self.__cid__ = client_id

    def poll(self, *args, **kwargs) -> Optional[Message]:
        message = super().poll(*args, **kwargs)
        if message is None:
            return message
        return self.__intercept(message)

    def consume(self, *args, **kwargs) -> Optional[List[Message]]:
        messages = super().consume(*args, **kwargs)
        if messages is None:
            return messages
        return [self.__intercept(message) for message in messages]

    def __intercept(self, message: Message) -> Optional[Message]:
        if message is None:
            return message
        client = manager._clients.get(self.__cid__)
        if client is None:
            return message
        try:
            return asyncio.run(self.__intercept_single(message, client))
        except Exception:
            return message

    async def __intercept_single(self, message: Message, client: _Client) -> Union[Message, Message]:
        if message is None:
            return message

        message_value = message.value()
        message_headers = message.headers()
        partition = message.partition()
        topic = message.topic()

        if message_headers is None or message_value is None:
            return message

        if not client.is_consumer:
            await client.send_client_type_update_req(_CONSUMER_CLIENT_TYPE)

        if client.config.consumer_group_topics_partitions is None:
            client.config.consumer_group_topics_partitions = {}

        if topic in client.config.consumer_group_topics_partitions:
            if partition not in client.config.consumer_group_topics_partitions[topic]:
                client.config.consumer_group_topics_partitions[topic].append(partition)
        else:
            client.config.consumer_group_topics_partitions[topic] = [partition]

        client.counters.total_bytes_after_reduction += len(message.value())

        for key, value in message_headers:
            if key == "superstream_schema":
                schema_id = value.decode("utf-8") if isinstance(value, bytes) else value
                if schema_id not in client.consumer_proto_desc_map:
                    if not client.get_schema_request_sent:
                        await client.send_get_schema_request(schema_id)

                    while schema_id not in client.consumer_proto_desc_map:
                        time.sleep(0.5)

                descriptor = client.consumer_proto_desc_map.get(schema_id)
                if descriptor is not None:
                    try:
                        json_msg = proto_to_json(message_value, descriptor)
                        bytes_msg = json_msg.encode("utf-8")
                        message.set_value(bytes_msg)
                        break
                    except Exception as e:
                        await client.handle_error(f"{_name(self.on_consume)} at {_name(proto_to_json)} {e!s}")
                        return message
                    client.counters.total_bytes_before_reduction += len(json_msg)
                    client.counters.total_messages_successfully_consumed += 1
                else:
                    await client.handle_error(f"{_name(self.on_consume)} schema not found")
                    return message
        initial_headers = [(key, value) for key, value in message_headers if key != "superstream_schema"]
        message.set_headers(initial_headers)
        client.counters.total_bytes_before_reduction += len(message.value())
        client.counters.total_messages_failed_consume += 1
        return message


class _ProducerInterceptor(Producer):
    def __init__(self, client_id, config: Dict):
        if (config is None) or (isinstance(config, Dict) is False):
            raise Exception("superstream: invalid configuration object")
        super().__init__(config)
        self.__config = config
        self.client_id = client_id

    def produce(self, *args, **kwargs):
        topic_index = 0
        value_index = 1
        partition_index = 3
        headers_index = 6
        try:
            client = manager._clients.get(self.client_id)
            if client is None:
                super().produce(*args, **kwargs)
                return

            msg = args[value_index] if len(args) > value_index else kwargs.get("value")
            json_msg = _try_convert_to_json(msg)
            if json_msg is None:
                super().produce(*args, **kwargs)
                return
            topic = args[topic_index]
            partition = args or 0 if len(args) > partition_index else kwargs.get("partition", 0)
            encoded_msg, superstream_headers = asyncio.run(self._intercept(topic, json_msg, client, partition))

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
                args = args[:value_index] + (encoded_msg,) + args[value_index + 1 :]
            elif "value" in kwargs:
                kwargs["value"] = encoded_msg
        except Exception as e:
            print("superstream: ", e)

        super().produce(*args, **kwargs)

    async def _intercept(self, topic: str, msg: str, client: _Client, partition: int = 0) -> (bytes, Optional[Dict]):
        if client.config.producer_topics_partitions is None:
            client.config.producer_topics_partitions = {}

        if topic in client.config.producer_topics_partitions:
            if partition not in client.config.producer_topics_partitions[topic]:
                client.config.producer_topics_partitions[topic].append(partition)
        else:
            client.config.producer_topics_partitions[topic] = [partition]

        if not client.is_producer:
            await client.send_client_type_update_req(_PRODUCER_CLIENT_TYPE)

        try:
            byte_msg = msg.encode("utf-8")
        except Exception as e:
            await client.handle_error(f"{_name(self._intercept)} at encoding message {e!s}")
            return msg, None

        client.counters.total_bytes_before_reduction += len(byte_msg)
        if client.producer_proto_desc is not None:
            try:
                proto_msg = json_to_proto(byte_msg, client.producer_proto_desc)
            except Exception as e:  # noqa: F841
                client.counters.total_bytes_after_reduction += len(byte_msg)
                client.counters.total_messages_failed_produce += 1
                return byte_msg, None

            headers = {"superstream_schema": client.producer_schema_id}
            client.counters.total_bytes_after_reduction += len(proto_msg)
            client.counters.total_messages_successfully_produce += 1
            return proto_msg, headers

        else:
            client.counters.total_bytes_after_reduction += len(byte_msg)
            client.counters.total_messages_failed_produce += 1
            if client.learning_factor_counter <= client.learning_factor:
                await client.send_learning_message(byte_msg)
                client.learning_factor_counter += 1
            elif (
                not client.learning_request_sent
                and client.learning_factor_counter >= client.learning_factor
                and client.producer_proto_desc is None
            ):
                await client.send_register_schema_req()

        return byte_msg, None
