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
