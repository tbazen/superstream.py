import asyncio
import base64
from typing import Optional

from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg

import superstream.manager as manager
from superstream.types import SchemaUpdateReq, Update
from superstream.utils import _name

_LEARNED_SCHEMA = "LearnedSchema"


class _ClientUpdateSubscription:
    client_id: int
    subscription: Optional[NatsClient]
    update_queue: asyncio.Queue

    def __init__(self, client_id: int):
        self.client_id = client_id
        self.subscription = None
        self.update_queue = asyncio.Queue()

    async def updates_handler(self):
        clients = manager._clients
        while True:
            msg = await self.update_queue.get()
            if msg.type == _LEARNED_SCHEMA:
                if self.client_id in clients:
                    client = clients[self.client_id]
                    try:
                        decoded = base64.b64decode(msg.payload)
                        schema_update_req = SchemaUpdateReq.model_validate_json(decoded)
                    except Exception as e:
                        await client.handle_error(
                            f"{_name(self.updates_handler)}: error parsing schema update request: {e!s}"
                        )
                        continue
                    desc = client._compile_descriptor(
                        schema_update_req.desc, schema_update_req.master_msg_name, schema_update_req.file_name
                    )
                    if desc is not None:
                        client.producer_proto_desc = desc
                        client.producer_schema_id = schema_update_req.schema_id
                    else:
                        await client.handle_error(f"{_name(self.updates_handler)}: error compiling descriptor")

    async def subscription_handler(self, msg: Msg):
        clients = manager._clients
        try:
            update = Update.model_validate_json(msg.data)
        except Exception as e:
            await clients[self.client_id].handle_error(f"{_name(self.subscription_handler)}: {e!s}")
            return
        await self.update_queue.put(update)
