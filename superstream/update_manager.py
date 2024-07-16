import asyncio
from typing import Callable, Optional

from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg

from superstream.types import Update
from superstream.utils import _name


class SuperstreamUpdateManager:
    client_hash: int
    subscription: Optional[NatsClient]
    update_queue: asyncio.Queue
    error_handler: Callable[[str], None]
    process_update_cb: Callable[[Update], None]

    def __init__(self, client_hash: int, error_handler: Optional[Callable[[str], None]], process_update_cb: Callable):
        self.client_hash = client_hash
        self.subscription = None
        self.update_queue = asyncio.Queue()
        self.error_handler = error_handler
        self.process_update_cb = process_update_cb

    async def listen_update(self):
        while True:
            update = await self.update_queue.get()
            self.process_update_cb(update)

    async def update_handler(self, msg: Msg):
        try:
            update = Update.model_validate_json(msg.data)
        except Exception as e:
            await self.error_handler(f"{_name(self.update_handler)}: {e!s}")
            return
        await self.update_queue.put(update)
