import base64
import json
from typing import Any, Optional, Union

from google.protobuf import descriptor_pb2, descriptor_pool, json_format
from google.protobuf.message_factory import GetMessageClass
from pydantic import BaseModel


def _name(obj):
    """
    Returns the name of an object, class or function as a string.
    """
    if callable(obj):
        return obj.__name__
    elif isinstance(obj, type):
        return obj.__name__
    return obj.__class__.__name__


def _try_convert_to_json(input) -> Optional[str]:
    """
    Tries to convert the input to a JSON string.
    :param input: The input to convert to JSON.
    :return: The JSON string or None if the input cannot be converted to JSON.
    """
    try:
        if isinstance(input, str):
            json.loads(input)
            return input
        return json.dumps(input)
    except Exception:
        return None


def json_to_proto(json_dict: Union[dict, str], desc) -> bytes:
    msg_desc = desc()
    if isinstance(json_dict, dict):
        json_format.ParseDict(json_dict, msg_desc)
    else:
        json_format.Parse(json_dict, msg_desc)
    return msg_desc.SerializeToString()


def proto_to_json(proto: bytes, desc) -> str:
    msg_desc: Any = desc()
    msg_desc.ParseFromString(proto)
    return json_format.MessageToJson(msg_desc)


def compile_descriptor(
    descriptor: Union[str, bytes],
    msg_struct_name: str,
    file_name: str,
) -> Any:
    descriptor = base64.b64decode(descriptor)
    desc_set = descriptor_pb2.FileDescriptorSet()
    desc_set.ParseFromString(descriptor)
    pool = descriptor_pool.DescriptorPool()
    file_desc = next((f for f in desc_set.file if f.name == file_name), None)
    if file_desc is None:
        raise ValueError(f"File {file_name} not found in descriptor")
    pool.Add(file_desc)
    pkg_name = file_desc.package
    msg_name = msg_struct_name
    if pkg_name != "":
        msg_name = file_desc.package + "." + msg_struct_name
    return GetMessageClass(pool.FindMessageTypeByName(msg_name))


def convert_escaped_json_string(escaped_json_string):
    json_node = json.loads(escaped_json_string)
    return json.dumps(json_node).replace('\\"', '"').replace("\\\\", "\\")


def convert_map(topic_partitions):
    result = {}
    for key, value in topic_partitions.items():
        result[key] = list(value)
    return result


def properties_to_map(properties):
    return {str(key): value for key, value in properties.items()}


class JsonToProtoResult(BaseModel):
    success: bool
    message_bytes: Optional[bytes]


class SerializationUtil:
    @staticmethod
    def json_to_proto(json_dict: Union[dict, str], desc) -> JsonToProtoResult:
        try:
            msg_desc = desc()
            if isinstance(json_dict, dict):
                json_format.ParseDict(json_dict, msg_desc)
            else:
                json_format.Parse(json_dict, msg_desc)
            return JsonToProtoResult(success=True, message_bytes=msg_desc.SerializeToString())
        except Exception:
            return JsonToProtoResult(success=False, message_bytes=None)

    @staticmethod
    def proto_to_json(proto: bytes, desc) -> str:
        msg_desc: Any = desc()
        msg_desc.ParseFromString(proto)
        return json_format.MessageToJson(msg_desc)

    @staticmethod
    def is_json_object(json_string):
        try:
            json.loads(json_string)
            return True
        except Exception:
            return False


class KafkaUtil:
    ProducerAndConsumerConfigKeys = [  # noqa: RUF012
        "builtin.features",
        "client.id",
        "metadata.broker.list",
        "bootstrap.servers",
        "message.max.bytes",
        "message.copy.max.bytes",
        "receive.message.max.bytes",
        "max.in.flight.requests.per.connection",
        "max.in.flight",
        "topic.metadata.refresh.interval.ms",
        "metadata.max.age.ms",
        "topic.metadata.refresh.fast.interval.ms",
        "topic.metadata.refresh.fast.cnt",
        "topic.metadata.refresh.sparse",
        "topic.metadata.propagation.max.ms",
        "topic.blacklist",
        "debug",
        "socket.timeout.ms",
        "socket.blocking.max.ms",
        "socket.send.buffer.bytes" "socket.receive.buffer.bytes",
        "socket.keepalive.enable",
        "socket.nagle.disable",
        "socket.max.fails",
        "broker.address.ttl",
        "broker.address.family",
        "socket.connection.setup.timeout.ms",
        "connections.max.idle.ms",
        "reconnect.backoff.jitter.ms",
        "reconnect.backoff.ms",
        "reconnect.backoff.max.ms",
        "statistics.interval.ms",
        "enabled_events",
        "error_cb",
        "throttle_cb",
        "stats_cb",
        "log_cb",
        "log_level",
        "log.queue",
        "log.thread.name",
        "enable.random.seed",
        "log.connection.close",
        "background_event_cb",
        "socket_cb",
        "connect_cb",
        "closesocket_cb",
        "open_cb",
        "resolve_cb",
        "opaque",
        "default_topic_conf",
        "internal.termination.signal",
        "api.version.request",
        "api.version.request.timeout.ms",
        "api.version.fallback.ms",
        "broker.version.fallback",
        "allow.auto.create.topics",
        "security.protocol",
        "ssl.cipher.suites",
        "ssl.curves.list",
        "ssl.sigalgs.list",
        "ssl.key.location",
        "ssl.key.password",
        "ssl.key.pem",
        "ssl_key",
        "ssl.certificate.location",
        "ssl.certificate.pem",
        "ssl_certificate",
        "ssl.ca.location",
        "ssl.ca.pem",
        "ssl_ca",
        "ssl.ca.certificate.stores",
        "ssl.crl.location",
        "ssl.keystore.location",
        "ssl.keystore.password",
        "ssl.providers",
        "ssl.engine.location",
        "ssl.engine.id",
        "ssl_engine_callback_data",
        "enable.ssl.certificate.verification",
        "ssl.endpoint.identification.algorithm",
        "ssl.certificate.verify_cb",
        "sasl.mechanisms",
        "sasl.mechanism",
        "sasl.kerberos.service.name",
        "sasl.kerberos.principal",
        "sasl.kerberos.kinit.cmd",
        "sasl.kerberos.keytab",
        "sasl.kerberos.min.time.before.relogin",
        "sasl.username",
        "sasl.password",
        "sasl.oauthbearer.config",
        "enable.sasl.oauthbearer.unsecure.jwt",
        "oauthbearer_token_refresh_cb",
        "sasl.oauthbearer.method",
        "sasl.oauthbearer.client.id",
        "sasl.oauthbearer.client.secret",
        "sasl.oauthbearer.scope",
        "sasl.oauthbearer.extensions",
        "sasl.oauthbearer.token.endpoint.url",
        "plugin.library.paths",
        "interceptors",
        "client.rack",
        "retry.backoff.ms",
        "retry.backoff.max.ms",
        "client.dns.lookup",
        "enable.metrics.push",
        "opaque",
    ]

    ProducerConfigKeys = [  # noqa: RUF012
        "transactional.id",
        "transaction.timeout.ms",
        "enable.idempotence",
        "enable.gapless.guarantee",
        "queue.buffering.max.messages",
        "queue.buffering.max.kbytes",
        "queue.buffering.max.ms",
        "linger.ms",
        "message.send.max.retries",
        "retries",
        "request.required.acks",
        "acks",
        "request.timeout.ms",
        "message.timeout.ms",
        "delivery.timeout.ms",
        "queuing.strategy",
        "produce.offset.report",
        "partitioner",
        "partitioner_cb",
        "msg_order_cmp",
        "compression.codec",
        "compression.type",
        "compression.level",
        "queue.buffering.backpressure.threshold",
        "compression.codec",
        "compression.type",
        "batch.num.messages",
        "batch.size",
        "delivery.report.only.error",
        "dr_cb",
        "dr_msg_cb",
        "sticky.partitioning.linger.ms",
    ]

    ConsumerConfigKeys = [  # noqa: RUF012
        "group.id",
        "group.instance.id",
        "partition.assignment.strategy",
        "session.timeout.ms",
        "heartbeat.interval.ms",
        "group.protocol.type",
        "group.protocol",
        "group.remote.assignor",
        "coordinator.query.interval.ms",
        "max.poll.interval.ms",
        "enable.auto.commit",
        "auto.commit.interval.ms",
        "enable.auto.offset.store",
        "queued.min.messages",
        "queued.max.messages.kbytes",
        "fetch.wait.max.ms",
        "fetch.queue.backoff.ms",
        "fetch.message.max.bytes",
        "max.partition.fetch.bytes",
        "fetch.max.bytes",
        "fetch.min.bytes",
        "fetch.error.backoff.ms",
        "offset.store.method",
        "isolation.level",
        "consume_cb",
        "rebalance_cb",
        "offset_commit_cb",
        "enable.partition.eof",
        "check.crcs",
        "auto.commit.enable",
        "enable.auto.commit",
        "auto.commit.interval.ms",
        "auto.offset.reset",
        "offset.store.path",
        "offset.store.sync.interval.ms",
        "offset.store.method",
        "consume.callback.max.messages",
    ]

    @staticmethod
    def is_valid_producer_key(key):
        return key in KafkaUtil.ProducerConfigKeys or key in KafkaUtil.ProducerAndConsumerConfigKeys

    @staticmethod
    def is_valid_consumer_key(key):
        return key in KafkaUtil.ConsumerConfigKeys or key in KafkaUtil.ProducerAndConsumerConfigKeys

    @staticmethod
    def copy_auth_config(configs):
        auth_keys = [
            "security.protocol",
            "ssl.truststore.location",
            "ssl.truststore.password",
            "ssl.keystore.location",
            "ssl.keystore.password",
            "ssl.key.password",
            "ssl.endpoint.identification.algorithm",
            "sasl.jaas.config",
            "sasl.kerberos.service.name",
            "sasl.username",
            "sasl.password",
            "sasl.mechanism",
            "ssl.ca.location",
        ]

        networking_keys = [
            "bootstrap.servers",
            "client.dns.lookup",
            "connections.max.idle.ms",
            "request.timeout.ms",
            "metadata.max.age.ms",
            "reconnect.backoff.ms",
            "reconnect.backoff.max.ms",
        ]
        relevant_keys = auth_keys + networking_keys

        relevant_props = {}
        for key in relevant_keys:
            if key in configs:
                if key == "bootstrap.servers":
                    value = configs[key]
                    if isinstance(value, list):
                        relevant_props[key] = ", ".join(value)
                    else:
                        relevant_props[key] = value
                else:
                    relevant_props[key] = str(configs[key])

        return relevant_props
