import base64
import json
from typing import Any, Optional, Union

from google.protobuf import descriptor_pb2, descriptor_pool, json_format
from google.protobuf.message_factory import GetMessageClass


def _name(obj):
    """
    Returns the name of an object, class or function as a string.
    """
    if callable(obj):
        return obj.__name__
    elif isinstance(obj, type):
        return obj.__name__
    return obj.__class__.__name__


def _try_convert_to_json(input) -> (bool, Optional[str]):
    try:
        if isinstance(input, str):
            json.loads(input)
            return True, input
        return True, json.dumps(input)
    except Exception:
        return False, None


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
