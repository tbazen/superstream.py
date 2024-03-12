import json

from confluent_kafka import Producer

from superstream.utils import compile_descriptor, json_to_proto, proto_to_json

descriptor_64 = "CmwKEnRlc3RzY2hlbWFfMS5wcm90byJOCgRUZXN0EhYKBmZpZWxkMRgBIAEoCVIGZmllbGQxEhYKBmZpZWxkMhgCIAEoCVIGZmllbGQyEhYKBmZpZWxkMxgDIAEoBVIGZmllbGQzYgZwcm90bzM="
file_name = "testschema_1.proto"
msg_struct_name = "Test"
expected_json_dict = {
    "field1": "value",
    "field2": "value2",
    "field3": 123,
}
json_str = '{"field1": "value1","field2": "value2","field3": 123}'
proto_bytes = b"\n\x05value\x12\x06value2\x18{"


def test_compile_descriptor():
    global descriptor_64, msg_struct_name, file_name
    desc = compile_descriptor(descriptor_64, msg_struct_name, file_name)
    assert desc is not None


def test_json_to_proto():
    global descriptor_64, msg_struct_name, file_name, json_str
    msg_desc = compile_descriptor(descriptor_64, msg_struct_name, file_name)
    assert msg_desc is not None

    proto = json_to_proto(json_str, msg_desc)
    assert proto is not None


def test_proto_to_json():
    global descriptor_64, msg_struct_name, file_name, proto_bytes, expected_json_dict
    msg_desc = compile_descriptor(descriptor_64, msg_struct_name, file_name)
    assert msg_desc is not None

    json_str = proto_to_json(proto_bytes, msg_desc)
    assert json_str is not None
    assert json.loads(json_str) == expected_json_dict


def test_producer_props():
    props = {
        "bootstrap.servers": "localhost:9092",
        "client.id": "test_producer",
    }
    assert props is not None

    producer = Producer(props)
    print(producer)
    assert producer is not None
