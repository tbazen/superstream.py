import asyncio
from typing import Dict, Optional, Union

from google.protobuf.descriptor import Descriptor

import superstream.manager as manager
from superstream.constants import (
    _CLIENT_REGISTER_SUBJECT,
    _CLIENT_TYPE_UPDATE_SUBJECT,
    _CONSUMER_CLIENT_TYPE,
    _PRODUCER_CLIENT_TYPE,
    _SDK_LANGUAGE,
    _SDK_VERSION,
    _SUPERSTREAM_CLIENTS_UPDATE_SUBJECT,
    _SUPERSTREAM_GET_SCHEMA_SUBJECT,
    _SUPERSTREAM_LEARNING_SUBJECT,
    _SUPERSTREAM_REGISTER_SCHEMA_SUBJECT,
    _SUPERSTREAM_UPDATES_SUBJECT,
)
from superstream.subscription import _ClientUpdateSubscription
from superstream.types import (
    ClientConfig,
    ClientCounters,
    ClientTypeUpdateReq,
    GetSchemaReq,
    RegisterReq,
    RegisterResp,
    SchemaUpdateReq,
    TopicsPartitionsPerProducerConsumer,
)
from superstream.utils import _name, compile_descriptor


class _Client:
    client_id: int
    account_name: str
    nats_connection_id: str
    is_consumer: bool
    is_producer: bool
    learning_factor: int
    learning_factor_counter: int
    learning_request_sent: bool
    get_schema_request_sent: bool
    producer_proto_desc: Optional[Descriptor]
    producer_schema_id: str
    consumer_proto_desc_map: Dict[str, Descriptor]
    counters: ClientCounters
    config: ClientConfig

    def __init__(self, config: ClientConfig, learning_factor: int):
        self.learning_factor = learning_factor
        self.config = config
        self.learning_factor_counter = 0
        self.learning_request_sent = False
        self.get_schema_request_sent = False
        self.producer_proto_desc = None

        self.consumer_proto_desc_map = {}
        self.is_consumer = False
        self.is_producer = False
        self.counters = ClientCounters(
            total_bytes_before_reduction=0,
            total_bytes_after_reduction=0,
            total_messages_successfully_produce=0,
            total_messages_successfully_consumed=0,
            total_messages_failed_produce=0,
            total_messages_failed_consume=0,
        )

    async def handle_error(self, msg: str):
        err_msg = f"[account name: {self.account_name}][clientID: {self.client_id}][sdk: {_SDK_LANGUAGE}][version: {_SDK_VERSION}]{msg}"
        await manager._send_client_errors_to_backend(err_msg)

    async def send_register_schema_req(self):
        if self.learning_request_sent:
            return
        try:
            await manager._js_publish(_SUPERSTREAM_REGISTER_SCHEMA_SUBJECT % self.client_id, b"")
            self.learning_request_sent = True
        except Exception as e:
            await self.handle_error(f"{_name(self.send_register_schema_req)} at publish {e!s}")

    async def send_learning_message(self, msg: bytes):
        try:
            await manager._js_publish(_SUPERSTREAM_LEARNING_SUBJECT % self.client_id, msg)
        except Exception as e:
            await self.handle_error(f"{_name(self.send_learning_message)} at publish {e!s}")

    async def send_client_type_update_req(self, client_type: str):
        if client_type == _CONSUMER_CLIENT_TYPE:
            self.is_consumer = True
        elif client_type == _PRODUCER_CLIENT_TYPE:
            self.is_producer = True

        payload = ClientTypeUpdateReq(client_id=self.client_id, type=client_type).json().encode()

        try:
            await manager._publish(_CLIENT_TYPE_UPDATE_SUBJECT, payload)
        except Exception as e:
            await self.handle_error(f"{_name(self.send_client_type_update_req)} at publish {e!s}")

    async def report_clients_update(self):
        while True:
            await asyncio.sleep(30)

            byte_counters = self.counters.model_dump_json().encode()

            try:
                await manager._publish(
                    _SUPERSTREAM_CLIENTS_UPDATE_SUBJECT % ("counters", self.client_id), byte_counters
                )
            except Exception as e:
                await self.handle_error(f"{_name(self.report_clients_update)} at publish to counters subject {e!s}")

            if self.config.consumer_group_topics_partitions is None:
                self.config.consumer_group_topics_partitions = {}

            topic_partitions = TopicsPartitionsPerProducerConsumer(
                producer_topics_partitions=self.config.producer_topics_partitions,
                consumer_group_topics_partitions=self.config.consumer_group_topics_partitions,
            )
            byte_config = topic_partitions.model_dump_json().encode("utf-8")
            try:
                await manager._publish(_SUPERSTREAM_CLIENTS_UPDATE_SUBJECT % ("config", self.client_id), byte_config)
            except Exception as e:
                await self.handle_error(f"{_name(self.report_clients_update)} at publish to config subject {e!s}")

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
            msg = await manager._request(_SUPERSTREAM_GET_SCHEMA_SUBJECT % self.client_id, req_bytes, 30)
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
        register_req = RegisterReq(
            nats_connection_id=manager._nats_connection_id,
            language=_SDK_LANGUAGE,
            version=_SDK_VERSION,
            learning_factor=self.learning_factor,
            config=self.config,
        )

        try:
            register_req_bytes = register_req.model_dump_json().encode()
        except Exception as e:
            raise Exception("superstream: error registering client") from e

        try:
            resp = await manager._request(_CLIENT_REGISTER_SUBJECT, register_req_bytes, 30)
        except Exception as e:
            raise Exception("superstream: error registering client") from e

        try:
            register_resp = RegisterResp.parse_raw(resp.data)
        except Exception as e:
            raise Exception("superstream: error registering client") from e

        self.client_id = register_resp.client_id
        self.account_name = register_resp.account_name
        self.learning_factor = register_resp.learning_factor
        self.learning_factor_counter = 0
        self.learning_request_sent = False
        self.get_schema_request_sent = False
        self.consumer_proto_desc_map = {}
        self.is_consumer = False
        self.is_producer = False
        self.counters = ClientCounters(
            total_bytes_before_reduction=0,
            total_bytes_after_reduction=0,
            total_messages_successfully_produce=0,
            total_messages_successfully_consumed=0,
            total_messages_failed_produce=0,
            total_messages_failed_consume=0,
        )

    async def subscribe_updates(self):
        self.client_update_sub = _ClientUpdateSubscription(client_id=self.client_id)

        # ruff : noqa : RUF006
        asyncio.create_task(self.client_update_sub.updates_handler())
        # await self.client_update_sub.updates_handler()

        try:
            self.client_update_sub.subscription = await manager._broker_connection.subscribe(
                _SUPERSTREAM_UPDATES_SUBJECT % self.client_id, cb=self.client_update_sub.subscription_handler
            )
        except Exception as e:
            raise Exception("superstream: error connecting with superstream ") from e
