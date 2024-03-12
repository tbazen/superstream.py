import asyncio
from typing import Dict, List, Optional, Union

import nats
from confluent_kafka import Consumer, Producer
from confluent_kafka.serialization import Serializer
from nats.aio.errors import ErrAuthorization

import superstream.manager as manager
from superstream.client import _Client
from superstream.constants import (
    _CLIENT_RECONNECTION_UPDATE_SUBJECT,
    _NATS_INFINITE_RECONNECT_ATTEMPTS,
    _SUPERSTREAM_INTERNAL_USERNAME,
)
from superstream.exceptions import ErrGenerateConnectionId
from superstream.interceptors import _Consumer, _Producer
from superstream.serialization import SuperstreamDeserializer
from superstream.types import ClientConfig, ClientReconnectionUpdateReq, Option
from superstream.utils import _name


async def _initialize_nats_connection(token: str, host: Union[str, List[str]]):
    global _broker_connection, _js_context, _nats_connection_id

    async def on_reconnected():
        global _nats_connection_id
        local_connection_id = ""
        for _, client in manager._clients.items():
            try:
                local_connection_id = await manager._generate_nats_connection_id()
                payload = (
                    ClientReconnectionUpdateReq(new_nats_connection_id=local_connection_id, client_id=client.client_id)
                    .model_dump_json()
                    .encode()
                )
                await manager._request(_CLIENT_RECONNECTION_UPDATE_SUBJECT, payload)
            except ErrGenerateConnectionId as e:
                await client._handle_error(
                    f"{_name(_initialize_nats_connection)} at {_name(manager._generate_nats_connection_id)}: {e!s}"
                )
                return
            except Exception as e:
                await client._handle_error(f"{_name(_initialize_nats_connection)} at broker connection request: {e!s}")
                return
        _nats_connection_id = local_connection_id

    try:
        manager._broker_connection = await nats.connect(
            max_reconnect_attempts=_NATS_INFINITE_RECONNECT_ATTEMPTS,
            reconnect_time_wait=1.0,
            user=_SUPERSTREAM_INTERNAL_USERNAME,
            password=token,
            servers=host,
            reconnected_cb=on_reconnected,
        )
        manager._js_context = manager._broker_connection.jetstream()
        manager._nats_connection_id = await manager._generate_nats_connection_id()
    except ErrAuthorization as e:
        raise Exception("error connecting with superstream: unauthorized") from e
    except Exception as e:
        raise Exception(f"error connecting with superstream: {e!s}") from e


async def init_async(token: str, host: str, config: Dict, opts: Option) -> ClientConfig:
    new_config = config

    client_type = "kafka"
    conf = _confluent_config_handler(client_type, config)
    new_client = _Client(config=conf, learning_factor=opts.learning_factor)

    if manager._broker_connection is None:
        try:
            await _initialize_nats_connection(token, host)
        except Exception as e:
            print("superstream: ", str(e))
            return new_config

    new_client.config.servers = opts.servers
    new_client.config.consumer_group_id = opts.consumer_group

    try:
        await new_client.register_client()
    except Exception as e:
        print("superstream: ", str(e))
        return new_config

    manager._clients[new_client.client_id] = new_client

    try:
        await new_client.subscribe_updates()
    except Exception as e:
        print("superstream: ", str(e))
        return new_config

    # ruff: noqa: RUF006
    asyncio.create_task(new_client.report_clients_update())

    # _configure_interceptors(new_config, new_client)
    return new_client.client_id


def _confluent_config_handler(client_type: str, confluent_config: dict) -> ClientConfig:
    required_acks = "WaitForAll"
    if "producer" in confluent_config and "required_acks" in confluent_config["producer"]:
        required_acks = {
            0: "NoResponse",
            1: "WaitForLocal",
            -1: "WaitForAll",
        }.get(confluent_config["producer"]["required_acks"], "")

    compression_level = "CompressionLevelDefault"
    if "producer" in confluent_config and "compression_level" in confluent_config["producer"]:
        compression_level = {
            0: "CompressionNone",
            1: "CompressionGZIP",
            2: "CompressionSnappy",
            3: "CompressionZSTD",
            0x08: "compressionCodecMask",
            5: "timestampTypeMask",
            -1000: "CompressionLevelDefault",
        }.get(confluent_config["producer"]["compression_level"], "CompressionLevelDefault")

    conf = ClientConfig(
        client_type=client_type,
        servers=confluent_config.get("bootstrap.servers") or "",
        producer_max_messages_bytes=confluent_config.get("message.max.bytes") or 1000000,
        producer_required_acks=required_acks,
        producer_timeout=confluent_config.get("request.timeout.ms") or 30000,
        producer_retry_max=confluent_config.get("message.send.max.retries") or 2147483647,
        producer_retry_backoff=confluent_config.get("retry.backoff.ms") or 100,
        producer_return_errors=True,
        producer_return_successes=False,
        producer_flush_max_messages=confluent_config.get("queue.buffering.max.messages") or 100000,
        producer_compression_level=compression_level,
        # consumer properties
        consumer_group_id=confluent_config.get("group.id") or "",
        consumer_fetch_min=confluent_config.get("fetch.min.bytes") or 1,
        consumer_fetch_default=confluent_config.get("fetch.message.max.bytes") or 1048576,
        consumer_retry_backoff=2,  # unknown default 2 seconds
        consumer_max_wait_time=confluent_config.get("fetch.wait.max.ms") or 500,
        consumer_max_processing_time=confluent_config.get("max_processing_time") or 500,  # unknown
        consumer_return_errors=True,
        consumer_offset_auto_commit_enable=True,
        consumer_offset_auto_commit_interval=1,
        consumer_offsets_initial=confluent_config.get("auto.offset.reset") or -1,  # unknown
        consumer_offsets_retry_max=3,
        consumer_group_session_timeout=confluent_config.get("group.min.session.timeout,ms") or 45000,
        consumer_group_heart_beat_interval=3,
        consumer_group_rebalance_timeout=60,
        consumer_group_rebalance_retry_max=4,
        consumer_group_rebalance_retry_back_off=2,
        consumer_group_rebalance_reset_invalid_offsets=True,
    )

    return conf


def configure_deserializer(client_id: int, deserializer: Serializer):
    hash = deserializer.__hash__()
    manager._client_object_cache[hash] = client_id
    return deserializer


def create_deserializer(client_id: int) -> Serializer:
    deserializer = SuperstreamDeserializer()
    configure_deserializer(client_id, deserializer)
    return deserializer


async def _init_producer(
    token: str, host: str, config: Dict, opts: Option, producer: Optional[Producer] = None
) -> Producer:
    if producer is None:
        raise Exception("superstream: producer should be provided")
    client_id = await init_async(token, host, config, opts)
    return _Producer(client_id, config)


async def _init_consumer(
    token: str, host: str, config: Dict, opts: Option, consumer: Optional[Consumer] = None
) -> Consumer:
    if consumer is None:
        raise Exception("superstream: consumer should be provided")
    client_id = await init_async(token, host, config, opts)
    return _Consumer(client_id, config)


def init(
    token: str,
    host: str,
    config: Dict,
    opts: Option,
    producer: Optional[Producer] = None,
    consumer: Optional[Consumer] = None,
) -> ClientConfig:
    if producer is None and consumer is None:
        raise Exception("superstream: either producer or consumer should be provided")

    if producer is not None:
        return asyncio.run(_init_producer(token, host, config, opts, producer))
    return asyncio.run(_init_consumer(token, host, config, opts, consumer))
