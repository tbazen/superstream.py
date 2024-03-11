import nest_asyncio  # noqa: I001

from superstream.connection import configure_deserializer, create_deserializer  # noqa: F401
from superstream.connection import create_producer  # noqa: F401
from superstream.connection import init  # noqa: F401

nest_asyncio.apply()
