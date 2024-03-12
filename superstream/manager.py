from typing import Any, Dict, Optional

from nats.aio.client import Client as NatsClient
from nats.js import JetStreamContext

from superstream.constants import _SUPERSTREAM_ERROR_SUBJECT
from superstream.exceptions import ErrGenerateConnectionId

_clients: Dict[int, Any] = {}
_broker_connection: NatsClient = None
_js_context: Optional[JetStreamContext] = None
_nats_connection_id: str = ""
_client_object_cache: Dict[int, int] = {}


async def _generate_nats_connection_id() -> str:
    try:
        client_id = _broker_connection.client_id
        server_name = _broker_connection.connected_url.hostname
        return f"{server_name}:{client_id}"
    except Exception as e:
        raise ErrGenerateConnectionId(e) from e


async def _send_client_errors_to_backend(err_msg: str):
    await _publish(_SUPERSTREAM_ERROR_SUBJECT, err_msg.encode())


async def _publish(subject: str, payload: bytes):
    await _broker_connection.publish(subject, payload)


async def _js_publish(subject: str, payload: bytes):
    await _js_context.publish(subject, payload)


async def _request(subject: str, payload: bytes, timeout: float = 30, timeout_retries: int = 1):
    """
    Send a request to the broker and wait for the response.
    :param timeout: Time to wait for the response. Default is 30 seconds.
    """
    try:
        res = await _broker_connection.request(subject, payload, timeout=timeout)
        return res
    except Exception as e:
        if "timeout" not in str(e).lower() or timeout_retries <= 0:
            raise e
        return await _request(subject, payload, timeout=timeout, timeout_retries=timeout_retries - 1)
