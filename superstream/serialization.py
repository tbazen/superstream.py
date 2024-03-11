import asyncio
import json
import time
from typing import Optional

from confluent_kafka.serialization import Deserializer, SerializationContext, Serializer

import superstream.manager as manager
from superstream.client import _Client
from superstream.constants import _CONSUMER_CLIENT_TYPE, _PRODUCER_CLIENT_TYPE
from superstream.utils import _name, json_to_proto, proto_to_json


class SuperstreamSerializer(Serializer):
    def __init__(self, fallback_serializer: Optional[Serializer] = None):
        self.fallback_serializer = fallback_serializer

    def __call__(self, obj, ctx: SerializationContext):
        hash = self.__hash__()
        client = None
        if hash in manager._client_object_cache and manager._client_object_cache[hash] in manager._clients:
            client = manager._clients[manager._client_object_cache[hash]]
        if client is None:
            return self.__fallback_serialize(obj, ctx)
        msg_str = obj if isinstance(obj, str) else json.dumps(obj)
        try:
            result = asyncio.run(self.__intercept_serialization(msg_str, ctx, client))
            return result
        except Exception as e:
            print(e)
            return msg_str.encode("utf-8")

    async def __intercept_serialization(self, msg: str, ctx: SerializationContext, client: _Client) -> bytes:
        partition = 0
        topic = ctx.topic
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
            await client.handle_error(f"{_name(self.on_send)} at encoding message {e!s}")
            return

        client.counters.total_bytes_before_reduction += len(byte_msg)
        if client.producer_proto_desc is not None:
            try:
                proto_msg = json_to_proto(byte_msg, client.producer_proto_desc)
            except Exception as e:
                print(e)
                client.counters.total_bytes_after_reduction += len(byte_msg)
                client.counters.total_messages_failed_produce += 1
                return

            if ctx.headers is None:
                ctx.headers = {"superstream_schema", client.producer_schema_id}
            elif isinstance(ctx.headers, dict):
                ctx.headers["superstream_schema"] = client.producer_schema_id
            elif isinstance(ctx.headers, list):
                ctx.headers.append({"superstream_schema", client.producer_schema_id})

            client.counters.total_bytes_after_reduction += len(proto_msg)
            client.counters.total_messages_successfully_produce += 1
            return proto_msg

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

        return byte_msg

    def __fallback_serialize(self, obj, ctx):
        if self.fallback_serializer is not None:
            return self.fallback_serializer(obj, ctx)
        if isinstance(obj, bytes) or isinstance(obj, str):
            return obj
        if isinstance(obj, dict):
            return json.dumps(obj).encode("utf-8")
        raise NotImplementedError


class SuperstreamDeserializer(Deserializer):
    def __init__(self, fallback_deserializer=None):
        self.fallback_deserializer = fallback_deserializer

    def __call__(self, data, ctx):
        hash = self.__hash__()
        client = None
        if hash in manager._client_object_cache and manager._client_object_cache[hash] in manager._clients:
            client = manager._clients[manager._client_object_cache[hash]]
        if client is None:
            return self.__fallback_deserialize(data, ctx)
        try:
            decoded = asyncio.run(self.__intercept_deserialization(data, ctx, client))
            if decoded is not None:
                return decoded
            if self.fallback_deserializer is not None:
                return self.fallback_deserializer(data, ctx)
            return data
        except Exception as e:
            print(e)
            if self.fallback_deserializer is not None:
                return self.fallback_deserializer(data, ctx)
            return data

    async def __intercept_deserialization(self, msg, ctx, client):
        if not client.is_consumer:
            await client.send_client_type_update_req(_CONSUMER_CLIENT_TYPE)

        partition = 0
        topic = ctx.topic

        if client.config.consumer_group_topics_partitions is None:
            client.config.consumer_group_topics_partitions = {}

        if topic in client.config.consumer_group_topics_partitions:
            if partition not in client.config.consumer_group_topics_partitions[topic]:
                client.config.consumer_group_topics_partitions[topic].append(partition)
        else:
            client.config.consumer_group_topics_partitions[topic] = [partition]

        client.counters.total_bytes_after_reduction += len(msg)

        if ctx.headers is None:
            return None

        for key, value in ctx.headers:
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
                        json_msg = proto_to_json(msg, descriptor)
                    except Exception as e:
                        await client.handle_error(f"{_name(self.on_consume)} at {_name(proto_to_json)} {e!s}")
                        return

                    msg = json_msg
                    client.counters.total_bytes_before_reduction += len(json_msg)
                    client.counters.total_messages_successfully_consumed += 1
                else:
                    await client.handle_error(f"{_name(self.on_consume)} schema not found")
                    return
        ctx.headers = [(key, value) for key, value in ctx.headers if key != "superstream_schema"]
        client.counters.total_bytes_before_reduction += len(msg)
        client.counters.total_messages_failed_consume += 1
        return json.loads(msg)
