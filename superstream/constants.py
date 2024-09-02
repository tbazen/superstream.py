import os

_SUPERSTREAM_INTERNAL_USERNAME = "superstream_internal"

# _SDK_VERSION = "3.5.113"
_SDK_VERSION = "1.0.11"
# _SDK_LANGUAGE = "python"
_SDK_LANGUAGE = "java"

_NATS_INFINITE_RECONNECT_ATTEMPTS = -1

_SUPERSTREAM_LEARNING_FACTOR_KEY = "superstream.learning.factor"
_SUPERSTREAM_TAGS_KEY = "superstream.tags"
_SUPERSTREAM_HOST_KEY = "superstream.host"
_SUPERSTREAM_TOKEN_KEY = "superstream.token"
_SUPERSTREAM_REDUCTION_ENABLED_KEY = "superstream.reduction.enabled"
_SUPERSTREAM_CONNECTION_KEY = "superstream.connection"
_SUPERSTREAM_INNER_CONSUMER_KEY = "superstream.inner.consumer"
_SUPERSTREAM_METADATA_TOPIC = "superstream.metadata"


class SuperstreamValues:
    MAX_TIME_WAIT_CAN_START = 60 * 10
    DEFAULT_SUPERSTREAM_TIMEOUT = 3
    OPTIMIZED_CONFIGURATION_KEY = "optimized_configuration"

    START_KEY = "start"
    ERROR_KEY = "error"


class SuperstreamSubjects:
    CLIENT_CONFIG_UPDATE = "internal.clientConfigUpdate"
    CLIENT_RECONNECTION_UPDATE = "internal_tasks.clientReconnectionUpdate"
    CLIENT_TYPE_UPDATE = "internal.clientTypeUpdate"
    REGISTER_CLIENT = "internal.registerClient"
    LEARN_SCHEMA = "internal.schema.learnSchema.%s"
    CLIENTS_UPDATE = "internal_tasks.clientsUpdate.%s.%s"
    CLIENT_ERRORS = "internal.clientErrors"
    GET_SCHEMA = "internal.schema.getSchema.%s"
    UPDATES = "internal.updates.%s"
    REGISTER_SCHEMA = "internal_tasks.schema.registerSchema.%s"
    START_CLIENT = "internal.startClient.%s"


class EnvVars:
    SUPERSTREAM_HOST = os.getenv("SUPERSTREAM_HOST")
    SUPERSTREAM_TOKEN = os.getenv("SUPERSTREAM_TOKEN", "no-auth")
    SUPERSTREAM_LEARNING_FACTOR: int = int(os.getenv("SUPERSTREAM_LEARNING_FACTOR", 20))
    SUPERSTREAM_TAGS: str = os.getenv("SUPERSTREAM_TAGS", "")
    SUPERSTREAM_DEBUG: bool = os.getenv("SUPERSTREAM_DEBUG", "False").lower() in ("true")
    SUPERSTREAM_RESPONSE_TIMEOUT: float = float(os.getenv("SUPERSTREAM_RESPONSE_TIMEOUT", 3))
    SUPERSTREAM_REDUCTION_ENABLED: bool = os.getenv("SUPERSTREAM_REDUCTION_ENABLED", "") == "true"
    SUPERSTREAM_COMPRESSION_ENABLED: bool = os.getenv("SUPERSTREAM_COMPRESSION_ENABLED", "") == "true"

    @staticmethod
    def is_compression_disabled():
        actual = os.getenv("SUPERSTREAM_COMPRESSION_ENABLED", "")
        return actual == "false"


class KafkaProducerConfigKeys:
    BOOTSTRAP_SERVERS = "bootstrap.servers"
    COMPRESSION_CODEC = "compression.codec"
    COMPRESSION_TYPE = "compression.type"
