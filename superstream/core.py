import asyncio
import base64
import json
import os
import sys
import threading
import uuid
from typing import Any, Dict, List, Optional, Union

import nats
from google.protobuf.descriptor import Descriptor
from nats.aio.client import Client as NatsClient
from nats.errors import Error as NatsError
from nats.js import JetStreamContext

from superstream.constants import (
    _CLIENT_RECONNECTION_UPDATE_SUBJECT,
    _CLIENT_REGISTER_SUBJECT,
    _CLIENT_START_SUBJECT,
    _CLIENT_TYPE_UPDATE_SUBJECT,
    _CONSUMER_CLIENT_TYPE,
    _NATS_INFINITE_RECONNECT_ATTEMPTS,
    _PRODUCER_CLIENT_TYPE,
    _SDK_LANGUAGE,
    _SDK_VERSION,
    _SUPERSTREAM_CLIENTS_UPDATE_SUBJECT,
    _SUPERSTREAM_CONNECTION_KEY,
    _SUPERSTREAM_DEFAULT_LEARNING_FACTOR,
    _SUPERSTREAM_DEFAULT_TOKEN,
    _SUPERSTREAM_ERROR_SUBJECT,
    _SUPERSTREAM_GET_SCHEMA_SUBJECT,
    _SUPERSTREAM_HOST_KEY,
    _SUPERSTREAM_INTERNAL_USERNAME,
    _SUPERSTREAM_LEARNING_FACTOR_KEY,
    _SUPERSTREAM_LEARNING_SUBJECT,
    _SUPERSTREAM_METADATA_TOPIC,
    _SUPERSTREAM_REDUCTION_ENABLED_KEY,
    _SUPERSTREAM_REGISTER_SCHEMA_SUBJECT,
    _SUPERSTREAM_TAGS_KEY,
    _SUPERSTREAM_TOKEN_KEY,
    _SUPERSTREAM_UPDATES_SUBJECT,
)
from superstream.exceptions import ErrGenerateConnectionId
from superstream.factory import SuperstreamFactory
from superstream.types import (
    ClientReconnectionUpdateReq,
    ClientTypeUpdateReq,
    CompressionUpdate,
    GetSchemaReq,
    LearnedSchemaUpdate,
    RegisterReq,
    RegisterResp,
    SchemaUpdateReq,
    SuperstreamCounters,
    ToggleReductionUpdate,
    TopicsPartitionsPerProducerConsumer,
    Update,
)
from superstream.update_manager import SuperstreamUpdateManager
from superstream.utils import _name, compile_descriptor


class Superstream:
    learning_factor_counter: int
    learning_request_sent: bool
    get_schema_request_sent: bool
    consumer_proto_desc_map: Dict[str, Descriptor]

    # required properties
    account_name: str

    learning_factor: int
    token: str
    host: str
    configs: Dict[str, Any]
    reduction__enabled: bool
    client_type: str
    tags: str
    compression_enabled: bool
    client_counters: SuperstreamCounters
    client_hash: str
    topic_partitions: Dict[str, List[int]]

    broker_connection: NatsClient
    jetstream: JetStreamContext
    nats_connection_id: str

    superstream_ready = False
    can_start = False

    update_manager: SuperstreamUpdateManager
    producer_proto_desc: Optional[Descriptor]
    producer_schema_id: str

    start_sub: Any

    def __init__(
        self,
        token: str,
        host: str,
        learning_factor: int,
        configs: Dict[str, Any],
        reduction_enabled: bool,
        client_type: str,
        tags: str,
    ):
        self.learning_factor = learning_factor
        self.token = token
        self.host = host
        self.reduction_enabled = reduction_enabled
        self.client_type = client_type
        self.compression_enabled = os.getenv("SUPERSTREAM_COMPRESSION_ENABLED", "") == "true"
        self.configs = configs
        self.tags = tags
        self.topic_partitions = {}

        self.learning_factor_counter = 0
        self.learning_request_sent = False
        self.get_schema_request_sent = False
        self.producer_proto_desc = None

        self.consumer_proto_desc_map = {}
        self.client_counters = SuperstreamCounters(
            total_bytes_before_reduction=0,
            total_bytes_after_reduction=0,
            total_messages_successfully_produce=0,
            total_messages_successfully_consumed=0,
            total_messages_failed_produce=0,
            total_messages_failed_consume=0,
        )

    async def _request(self, subject: str, payload: bytes, timeout: float = 30, timeout_retries: int = 1):
        """
        Send a request to the broker and wait for the response.
        :param timeout: Time to wait for the response. Default is 30 seconds.
        """
        try:
            res = await self.broker_connection.request(subject, payload, timeout=timeout)
            return res
        except Exception as e:
            if "timeout" not in str(e).lower() or timeout_retries <= 0:
                raise e
            return await self._request(subject, payload, timeout=timeout, timeout_retries=timeout_retries - 1)

    async def _send_client_errors_to_backend(self, err_msg: str):
        await self._publish(_SUPERSTREAM_ERROR_SUBJECT, err_msg.encode())

    async def _publish(self, subject: str, payload: bytes):
        await self.broker_connection.publish(subject, payload)

    async def _js_publish(self, subject: str, payload: bytes):
        await self.jetstream.publish(subject, payload)

    async def handle_error(self, msg: str):
        if self.broker_connection is None or self.superstream_ready is False:
            return
        err_msg = f"[sdk: {_SDK_LANGUAGE}][version: {_SDK_VERSION}][tags: {self.tags}] {msg}"
        await self._send_client_errors_to_backend(err_msg)

    async def send_register_schema_req(self):
        if self.learning_request_sent:
            return
        try:
            await self._publish(_SUPERSTREAM_REGISTER_SCHEMA_SUBJECT % self.client_hash, b"")
            self.learning_request_sent = True
        except Exception as e:
            await self.handle_error(f"{_name(self.send_register_schema_req)} at publish {e!s}")

    async def send_learning_message(self, msg: bytes):
        try:
            await self._publish(_SUPERSTREAM_LEARNING_SUBJECT % self.client_hash, msg)
        except Exception as e:
            await self.handle_error(f"{_name(self.send_learning_message)} at publish {e!s}")

    async def send_client_type_update_req(self):
        if not self.client_type:
            return

        is_valid_client_type = self.client_type == _CONSUMER_CLIENT_TYPE or self.client_type == _PRODUCER_CLIENT_TYPE
        if not is_valid_client_type:
            return

        try:
            await self._publish(
                _CLIENT_TYPE_UPDATE_SUBJECT,
                ClientTypeUpdateReq(client_hash=self.client_hash, type=self.client_type).model_dump_json().encode(),
            )
        except Exception as e:
            await self.handle_error(f"{_name(self.send_client_type_update_req)} at publish {e!s}")

    async def report_clients_update(self):
        async def client_update_task():
            try:
                if not self.broker_connection or not self.superstream_ready:
                    return
                topic_partition_payload = TopicsPartitionsPerProducerConsumer(
                    producer_topics_partitions={}, consumer_group_topics_partitions={}
                )
                if self.topic_partitions:
                    topic_partition_payload = TopicsPartitionsPerProducerConsumer(
                        producer_topics_partitions=self.topic_partitions
                        if self.client_type == _PRODUCER_CLIENT_TYPE
                        else {},
                        consumer_group_topics_partitions=self.topic_partitions
                        if self.client_type == _CONSUMER_CLIENT_TYPE
                        else {},
                    )

                await self._publish(
                    _SUPERSTREAM_CLIENTS_UPDATE_SUBJECT % ("counters", self.client_hash),
                    self.client_counters.model_dump_json().encode(),
                )
                await self._publish(
                    _SUPERSTREAM_CLIENTS_UPDATE_SUBJECT % ("config", self.client_hash),
                    topic_partition_payload.model_dump_json().encode(),
                )

            except Exception as e:
                self.handle_error(f"{_name(self.report_clients_update)}: {e!s}")

        async def start_periodic_task(interval, update_task):
            async def task_wrapper():
                await update_task()
                threading.Timer(interval, task_wrapper).start()

            await task_wrapper()

        await start_periodic_task(60 * 10, client_update_task)

    def _compile_descriptor(
        self,
        descriptor: Union[str, bytes],
        msg_struct_name: str,
        file_name: str,
    ):
        try:
            return compile_descriptor(descriptor, msg_struct_name, file_name)
        except Exception as e:
            self.handle_error(f"{_name(self._compile_descriptor)} at compile_descriptor {e!s}")
            raise e

    async def send_get_schema_request(self, schema_id: str):
        self.get_schema_request_sent = True
        req_bytes = GetSchemaReq(schema_id=schema_id).model_dump_json().encode()

        try:
            msg = await self._request(_SUPERSTREAM_GET_SCHEMA_SUBJECT % self.client_hash, req_bytes, 30)
        except Exception as e:
            await self.handle_error(f"{_name(self.send_get_schema_request)} at request {e!s}")
            self.get_schema_request_sent = False
            raise e

        try:
            resp = SchemaUpdateReq.model_validate_json(msg.data)
        except Exception as e:
            await self.handle_error(f"{_name(self.send_get_schema_request)} at parse_raw {e!s}")
            self.get_schema_request_sent = False
            raise e

        desc = self._compile_descriptor(resp.desc, resp.master_msg_name, resp.file_name)
        if desc is not None:
            self.consumer_proto_desc_map[resp.schema_id] = desc
        else:
            await self.handle_error(f"{_name(self.send_get_schema_request)}: error compiling schema")
            self.get_schema_request_sent = False
            raise Exception("superstream: error compiling schema")

    async def register_client(self):
        kafka_id = self.consume_latest_connection_id()

        def normalize_to_superstreamconfig(configs):
            superstream_config = {}

            def map_if_present(config, config_key, superstream_config, superstream_key):
                if config_key in config:
                    if config_key == "bootstrap.servers":
                        value = config[config_key]
                        if isinstance(value, list):
                            superstream_config[superstream_key] = ", ".join(value)
                        else:
                            superstream_config[superstream_key] = value
                    else:
                        superstream_config[superstream_key] = config[config_key]

            map_if_present(configs, "max.request.size", superstream_config, "producer_max_messages_bytes")
            map_if_present(configs, "acks", superstream_config, "producer_required_acks")
            map_if_present(configs, "delivery.timeout.ms", superstream_config, "producer_timeout")
            map_if_present(configs, "retries", superstream_config, "producer_retry_max")
            map_if_present(configs, "retry.backoff.ms", superstream_config, "producer_retry_backoff")
            map_if_present(configs, "compression.type", superstream_config, "producer_compression_level")

            map_if_present(configs, "fetch.min.bytes", superstream_config, "consumer_fetch_min")
            map_if_present(configs, "fetch.max.bytes", superstream_config, "consumer_fetch_default")
            map_if_present(configs, "retry.backoff.ms", superstream_config, "consumer_retry_backoff")
            map_if_present(configs, "max.poll.interval.ms", superstream_config, "consumer_max_wait_time")
            map_if_present(configs, "max.poll.records", superstream_config, "consumer_max_processing_time")

            map_if_present(
                configs, "auto.commit.interval.ms", superstream_config, "consumer_offset_auto_commit_interval"
            )
            map_if_present(configs, "session.timeout.ms", superstream_config, "consumer_group_session_timeout")
            map_if_present(configs, "heartbeat.interval.ms", superstream_config, "consumer_group_heart_beat_interval")
            map_if_present(configs, "retry.backoff.ms", superstream_config, "consumer_group_rebalance_retry_back_off")

            map_if_present(configs, "group.id", superstream_config, "consumer_group_id")

            map_if_present(configs, "bootstrap.servers", superstream_config, "servers")

            return superstream_config

        if kafka_id:
            try:
                kafka_id = int(kafka_id)
            except Exception:
                kafka_id = 0
        else:
            kafka_id = 0

        register_req = RegisterReq(
            nats_connection_id=self.nats_connection_id,
            language=_SDK_LANGUAGE,
            version=_SDK_VERSION,
            learning_factor=self.learning_factor,
            config=normalize_to_superstreamconfig(self.configs),
            reduction_enabled=self.reduction_enabled,
            connection_id=kafka_id,
            tags=self.tags,
        )

        try:
            register_req_bytes = register_req.model_dump_json().encode()
        except Exception as e:
            raise Exception("superstream: error encoding register requests") from e

        try:
            resp = await self._request(_CLIENT_REGISTER_SUBJECT, register_req_bytes, 60 * 5)
            register_resp = RegisterResp.model_validate_json(resp.data)
        except Exception as e:
            raise Exception("superstream: error registering client") from e

        self.client_hash = register_resp.client_hash
        self.account_name = register_resp.account_name
        self.learning_factor = register_resp.learning_factor
        self.learning_factor_counter = 0
        self.learning_request_sent = False
        self.get_schema_request_sent = False
        self.consumer_proto_desc_map = {}

    def copy_auth_config(self) -> Dict[str, Any]:
        relevant_keys = [
            "security.protocol",
            "ssl.truststore.location",
            "ssl.truststore.password",
            "ssl.keystore.location",
            "ssl.keystore.password",
            "ssl.key.password",
            "ssl.endpoint.identification.algorithm",
            "sasl.mechanism",
            "sasl.jaas.config",
            "sasl.kerberos.service.name",
            "bootstrap.servers",
            "client.dns.lookup",
            "connections.max.idle.ms",
            "request.timeout.ms",
            "metadata.max.age.ms",
            "reconnect.backoff.ms",
            "reconnect.backoff.max.ms",
        ]

        relevant_props = {}
        configs = self.configs
        for key in relevant_keys:
            if key in configs:
                if key == "bootstrap.servers":
                    value = configs[key]
                    if isinstance(value, list):
                        relevant_props[key] = ", ".join(value)
                    elif isinstance(value, (list, tuple)):
                        relevant_props[key] = ", ".join(value)
                    else:
                        relevant_props[key] = value
                else:
                    relevant_props[key] = str(configs[key])
        return relevant_props

    def consume_latest_connection_id(self) -> str:
        retries = 2
        try:
            config = self.copy_auth_config()
            config.update(
                {
                    "group.id": f"superstream-consumer-group-{uuid.uuid4()}",
                    "enable.auto.commit": False,
                }
            )
            topic = _SUPERSTREAM_METADATA_TOPIC
            c = SuperstreamFactory.create_consumer(config)
            partitions = c.list_topics().topics[topic].partitions
            if not partitions:
                return "0"
            topic_partitions = [SuperstreamFactory.create_topic_partition(topic, partition) for partition in partitions]
            end_offsets = [c.get_watermark_offsets(tp) for tp in topic_partitions]
            topic_partitions = [
                SuperstreamFactory.create_topic_partition(topic, p, offset=end_offset[1] - 1)
                for p, end_offset in zip(partitions, end_offsets)
            ]

            c.assign(topic_partitions)
            while retries > 0:
                msg = c.poll(timeout=10.0)
                if msg is None or msg.error():
                    retries -= 1
                    continue

                return msg.value().decode("utf-8")

        except Exception as e:
            self.handle_error(f"{_name(self.consume_latest_connection_id)}: {e!s}")
            return "0"
        finally:
            if c:
                c.close()

    def _generate_nats_connection_id(self) -> str:
        try:
            cid = self.broker_connection.client_id
            server_name = self.broker_connection._server_info.get("server_name")
            return f"{server_name}:{cid}"
        except Exception as e:
            raise ErrGenerateConnectionId(e) from e

    async def _initialize_nats_connection(self, token: str, host: Union[str, List[str]]):
        async def on_reconnected():
            local_connection_id = ""
            try:
                local_connection_id = self._generate_nats_connection_id()
                payload = (
                    ClientReconnectionUpdateReq(
                        new_nats_connection_id=local_connection_id, client_hash=self.client_hash
                    )
                    .model_dump_json()
                    .encode()
                )
                await self._request(_CLIENT_RECONNECTION_UPDATE_SUBJECT, payload)
            except ErrGenerateConnectionId as e:
                await self._handle_error(
                    f"{_name(self._initialize_nats_connection)} at {_name(self._generate_nats_connection_id)}: {e!s}"
                )
                return
            except Exception as e:
                await self._handle_error(
                    f"{_name(self._initialize_nats_connection)} at broker connection request: {e!s}"
                )
                return
            self.nats_connection_id = local_connection_id

        async def error_cb(e: NatsError):
            """
            Error callback for NATS connection errors.
            It does nothing. Its purpose is to override the NATS default error callback that prints the error to the console on every reconnect attempt.
            """
            if e is None:
                return
            critical_errors = [
                "invalid checksum",
                "nats: maximum account",
                "authorization violation",
            ]
            for error in critical_errors:
                if error.lower() in str(e).lower():
                    raise e

        try:
            self.broker_connection = await nats.connect(
                max_reconnect_attempts=_NATS_INFINITE_RECONNECT_ATTEMPTS,
                reconnect_time_wait=1.0,
                user=_SUPERSTREAM_INTERNAL_USERNAME,
                password=token,
                servers=host,
                reconnected_cb=on_reconnected,
                error_cb=error_cb,
            )
            self.jetstream = self.broker_connection.jetstream()
            self.nats_connection_id = self._generate_nats_connection_id()
        except Exception as exception:
            if "nats: maximum account" in str(exception):
                raise Exception(
                    "Cannot connect with superstream since you have reached the maximum amount of connected clients"
                ) from exception
            elif "invalid checksum" in str(exception):
                raise Exception("Error connecting with superstream: unauthorized") from exception
            else:
                raise Exception(f"Error connecting with superstream: {exception!s}") from exception

    async def wait_for_start(self):
        subject = _CLIENT_START_SUBJECT % self.client_hash

        async def start_cb(msg):
            try:
                message_data = json.loads(msg.data)
                if "start" in message_data:
                    if message_data["start"]:
                        self.can_start = True
                    else:
                        err = message_data.get("error", "")
                        print(f"superstream: Could not start superstream: {err}")
                        raise Exception("Thread interrupted due to error in start message")
            except Exception as e:
                print(e)

        async def is_started():
            while not self.can_start:
                await asyncio.sleep(1)

        subscription = None
        try:
            await self.broker_connection.subscribe(subject, cb=start_cb)
            await asyncio.wait_for(is_started(), timeout=60)
            if not self.can_start:
                print("superstream: Could not connect to superstream for 10 minutes.")
        except Exception as e:
            print(f"superstream: Could not start superstream: {e}")
            raise e
        finally:
            if subscription is not None:
                await subscription.unsubscribe()

    def process_update(self, update: Update):
        LEARNED_SCHEMA = "LearnedSchema"
        TOGGLE_REDUCTION = "ToggleReduction"
        COMPRESSION_UPDATE = "CompressionUpdate"

        def learned_schema_handler(payload: bytes):
            schema_update = LearnedSchemaUpdate.model_validate_json(payload)
            descriptor = compile_descriptor(schema_update.desc, schema_update.master_msg_name, schema_update.file_name)
            if descriptor:
                self.producer_proto_desc = descriptor
                self.producer_schema_id = schema_update.schema_id
            else:
                raise Exception("superstream: error compiling descriptor")

        def toggle_reduction_handler(payload: bytes):
            reduction_update = ToggleReductionUpdate.model_validate_json(payload)
            self.reduction_enabled = reduction_update.enable_reduction

        def compression_update_handler(payload: bytes):
            compression_update = CompressionUpdate.model_validate_json(payload)
            self.compression_enabled = compression_update.enable_compression

        handlers = {
            LEARNED_SCHEMA: learned_schema_handler,
            TOGGLE_REDUCTION: toggle_reduction_handler,
            COMPRESSION_UPDATE: compression_update_handler,
        }
        try:
            update_type, update_bytes = update.type, base64.b64decode(update.payload)
            handler = handlers.get(update_type)
            if handler:
                handler(update_bytes)
        except Exception as e:
            self.handle_error(f"{_name(self.process_update)}: {e!s}")

    async def subscribe_to_updates(self):
        try:
            subject = _SUPERSTREAM_UPDATES_SUBJECT % self.client_hash
            self.update_manager = SuperstreamUpdateManager(
                client_hash=self.client_hash,
                error_handler=self.handle_error,
                process_update_cb=self.process_update,
            )

            # ruff: noqa: RUF006
            asyncio.create_task(self.update_manager.listen_update())

            self.update_manager.subscription = await self.broker_connection.subscribe(
                subject, cb=self.update_manager.update_handler
            )

        except Exception as e:
            print(f"superstream: Could not subscribe to updates: {e}")

    def update_topic_partitions(self, topic: str, partition: int):
        self.topic_partitions[topic] = self.topic_partitions.get(topic, [])
        if partition not in self.topic_partitions[topic]:
            self.topic_partitions[topic].append(partition)

    async def init(self):
        try:
            await self._initialize_nats_connection(self.token, self.host)
            await self.register_client()
            await self.wait_for_start()
            assert self.can_start, "superstream: Could not start superstream"

            await self.subscribe_to_updates()
            self.superstream_ready = True
            await self.report_clients_update()
            await self.send_client_type_update_req()
        except Exception as e:
            await self.handle_error(f"superstream: error initializing superstream: {e!s}")

    def init_superstream_props(props: Dict[str, Any], client_type: str) -> Dict[str, Any]:
        props = props.copy()
        try:
            superstream_host = os.getenv("SUPERSTREAM_HOST")
            assert superstream_host, "superstream host is required"

            superstream_token = os.getenv("SUPERSTREAM_TOKEN", _SUPERSTREAM_DEFAULT_TOKEN)
            learning_factor = os.getenv("SUPERSTREAM_LEARNING_FACTOR", _SUPERSTREAM_DEFAULT_LEARNING_FACTOR)
            learning_factor = int(learning_factor) if isinstance(learning_factor, str) else learning_factor
            reduction_enabled = os.getenv("SUPERSTREAM_REDUCTION_ENABLED", "") == "true"
            tags = os.getenv("SUPERSTREAM_TAGS", "")

            superstream = Superstream(
                token=superstream_token,
                host=superstream_host,
                learning_factor=learning_factor,
                configs=props,
                reduction_enabled=reduction_enabled,
                client_type=client_type,
                tags=tags,
            )

            def run_event_loop(loop):
                asyncio.set_event_loop(loop)
                try:
                    loop.run_forever()
                except Exception as e:
                    sys.stderr.write(f"superstream: error running event loop: {e!s}")
                finally:
                    loop.close()

            def init_in_background(task):
                new_loop = asyncio.new_event_loop()
                t = threading.Thread(target=run_event_loop, args=(new_loop,))
                t.start()
                asyncio.run_coroutine_threadsafe(task, new_loop)
                return new_loop, t

            init_in_background(superstream.init())

            props.update(
                {
                    _SUPERSTREAM_HOST_KEY: superstream_host,
                    _SUPERSTREAM_TOKEN_KEY: superstream_token,
                    _SUPERSTREAM_LEARNING_FACTOR_KEY: learning_factor,
                    _SUPERSTREAM_REDUCTION_ENABLED_KEY: reduction_enabled,
                    _SUPERSTREAM_TAGS_KEY: tags,
                    _SUPERSTREAM_CONNECTION_KEY: superstream,
                }
            )

        except Exception as e:
            sys.stderr.write(f"superstream: error initializing superstream: {e!s}")

        return props
