import os


class SdkInfo:
    VERSION = "3.5.113"
    LANGUAGE = "java"


class NatsValues:
    INFINITE_RECONNECT_ATTEMPTS = -1


class SuperstreamKeys:
    LEARNING_FACTOR = "superstream.learning.factor"
    TAGS = "superstream.tags"
    HOST = "superstream.host"
    TOKEN = "superstream.token"
    REDUCTION_ENABLED = "superstream.reduction.enabled"
    CONNECTION = "superstream.connection"
    INNER_CONSUMER = "superstream.inner.consumer"
    METADATA_TOPIC = "superstream.metadata"


class SuperstreamValues:
    MAX_TIME_WAIT_CAN_START = 60 * 10
    DEFAULT_SUPERSTREAM_TIMEOUT = 3
    OPTIMIZED_CONFIGURATION_KEY = "optimized_configuration"
    INTERNAL_USERNAME = "superstream_internal"

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
