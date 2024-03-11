import asyncio
import time
from typing import Dict, Optional

from confluent_kafka import Producer
from confluent_kafka.serialization import Serializer

import superstream.manager as manager
from superstream.client import _Client
from superstream.constants import _CONSUMER_CLIENT_TYPE, _PRODUCER_CLIENT_TYPE
from superstream.types import ClientConfig
from superstream.utils import _name, _try_convert_to_json, json_to_proto, proto_to_json


class _ProducerInterceptor:
    def __init__(self, client: _Client, fallback_serializer=None):
        self.client = client
        self.fallback_serializer = fallback_serializer

    async def on_send(self, msg):
        if self.client.config.producer_topics_partitions is None:
            self.client.config.producer_topics_partitions = {}

        if msg.topic in self.client.config.producer_topics_partitions:
            if msg.partition not in self.client.config.producer_topics_partitions[msg.topic]:
                self.client.config.producer_topics_partitions[msg.topic].append(msg.partition)
        else:
            self.client.config.producer_topics_partitions[msg.topic] = [msg.partition]

        if not self.client.is_producer:
            await self.client.send_client_type_update_req(_PRODUCER_CLIENT_TYPE)

        try:
            byte_msg = msg.value.encode()
        except Exception as e:
            await self.client.handle_error(f"{_name(self.on_send)} at encoding message {e!s}")
            return

        self.client.counters.total_bytes_before_reduction += len(byte_msg)

        if self.client.producer_proto_desc is not None:
            try:
                proto_msg = json_to_proto(byte_msg, self.client.producer_proto_desc)
            except Exception:
                self.client.counters.total_bytes_after_reduction += len(byte_msg)
                self.client.counters.total_messages_failed_produce += 1
                return
            else:
                msg.headers = {"superstream_schema": self.client.producer_schema_id}
                self.client.counters.total_bytes_after_reduction += len(proto_msg)
                self.client.counters.total_messages_successfully_produce += 1
                msg.value = proto_msg
        else:
            self.client.counters.total_bytes_after_reduction += len(byte_msg)
            self.client.counters.total_messages_failed_produce += 1
            if self.client.learning_factor_counter <= self.client.learning_factor:
                await self.client.send_learning_message(byte_msg)
                self.client.learning_factor_counter += 1
            elif (
                not self.client.learning_request_sent
                and self.client.learning_factor_counter >= self.client.learning_factor
                and self.client.producer_proto_desc is None
            ):
                await self.client.send_register_schema_req()


class _ConsumerInterceptor:
    def __init__(self, client: _Client):
        self.client = client

    async def on_consume(self, msg):
        if not self.client.is_consumer:
            await self.client.send_client_type_update_req(_CONSUMER_CLIENT_TYPE)

        if self.client.config.consumer_group_topics_partitions is None:
            self.client.config.consumer_group_topics_partitions = {}

        if msg.topic in self.client.config.consumer_group_topics_partitions:
            if msg.partition not in self.client.config.consumer_group_topics_partitions[msg.topic]:
                self.client.config.consumer_group_topics_partitions[msg.topic].append(msg.partition)
        else:
            self.client.config.consumer_group_topics_partitions[msg.topic] = [msg.partition]

        self.client.counters.total_bytes_after_reduction += len(msg.value)

        for key, value in msg.headers.items():
            if key == "superstream_schema":
                schema_id = value
                if schema_id not in self.client.consumer_proto_desc_map:
                    if not self.client.get_schema_request_sent:
                        await self.client.send_get_schema_request(schema_id)

                    while schema_id not in self.client.consumer_proto_desc_map:
                        time.sleep(0.5)

                descriptor = self.client.consumer_proto_desc_map.get(schema_id)
                if descriptor is not None:
                    try:
                        json_msg = proto_to_json(msg.value, descriptor)
                    except Exception as e:
                        await self.client.handle_error(f"{_name(self.on_consume)} at {_name(proto_to_json)} {e!s}")
                        return
                    else:
                        del msg.headers[key]
                        msg.value = json_msg
                        self.client.counters.total_bytes_before_reduction += len(json_msg)
                        self.client.counters.total_messages_successfully_consumed += 1
                else:
                    await self.client.handle_error(f"{_name(self.on_consume)} schema not found")
                    print("superstream: schema not found")
                return

        self.client.counters.total_bytes_before_reduction += len(msg.value)
        self.client.counters.total_messages_failed_consume += 1


def _configure_interceptors(config: ClientConfig, client: _Client):
    _configure_producer_interceptor(config, client)
    _configure_consumer_interceptor(config, client)


def _configure_producer_interceptor(config: ClientConfig, client: _Client):
    if config.producer_interceptors is not None:
        slice_copy = config.producer_interceptors.copy()
        for i, interceptor in enumerate(slice_copy):
            if isinstance(interceptor, _ProducerInterceptor):
                slice_copy.pop(i)

        config.producer_interceptors = slice_copy

        config.producer_interceptors.append(_ProducerInterceptor(client=client))
    else:
        config.producer_interceptors = [_ProducerInterceptor(client=client)]


def _configure_consumer_interceptor(config: ClientConfig, client: _Client):
    if config.consumer_interceptors is not None:
        slice_copy = config.consumer_interceptors.copy()

        for i, interceptor in enumerate(slice_copy):
            if isinstance(interceptor, _ConsumerInterceptor):
                slice_copy.pop(i)

        config.consumer_interceptors = slice_copy

        config.consumer_interceptors.append(_ConsumerInterceptor(client=client))
    else:
        config.consumer_interceptors = [_ConsumerInterceptor(client=client)]


def configure_serializer(client_id: int, serializer: Serializer):
    try:
        hash = serializer.__hash__()
        manager._client_object_cache[hash] = client_id
    except Exception as e:
        print("superstream: ", str(e))


class SuperstreamProducer(Producer):
    def __init__(self, client_id, conf: Dict):
        if (conf is None) or (isinstance(conf, Dict) is False):
            raise Exception("superstream: invalid configuration object")
        super().__init__(conf)
        self.__config = conf
        self.client_id = client_id

    def produce(self, *args, **kwargs):
        try:
            client = manager._clients.get(self.client_id)
            if client is None:
                super().produce(*args, **kwargs)
                return
            topic = args[0]
            msg = args[1]
            success, json_msg = _try_convert_to_json(msg)
            if not success or json_msg is None:
                super().produce(*args, **kwargs)
                return
            partition = kwargs.get("partition", 0)
            encoded_msg, superstream_headers = asyncio.run(self._intercept(topic, json_msg, client, partition))
            if superstream_headers is not None:
                if kwargs.get("headers") is None:
                    kwargs["headers"] = superstream_headers
                elif isinstance(kwargs["headers"], dict):
                    kwargs["headers"].update(superstream_headers)
                elif isinstance(kwargs["headers"], list):
                    kwargs["headers"].extend(list(superstream_headers.items()))
            super().produce(topic, encoded_msg, *args[2:], **kwargs)

        except Exception as e:
            print("superstream: ", e)

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
            except Exception as e:
                print(e)
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
