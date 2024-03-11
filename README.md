# Superstream

## Installation

```sh
 pip install superstream
```

## Importing

```python
from superstream import create_deserializer, create_producer, init
from superstream.types import Option
```

## Producer

To use `Superstream` with kafka producer, first define the producer configurations:
  
```python
token = "<superstream-token>"
superstream_host = "<superstream-host>"
broker = "<kafka-broker>"
topic = "<kafka-topic>"
conf = {"bootstrap.servers": broker}
options = Option(host=superstream_host, learning_factor=10, servers=broker)
```

To initialize superstream, use `init`:

```python
result = init(token, superstream_host, conf, options)
```

Then create a new instance of kafka producer use `create_producer`:

```python
producer = create_producer(conf, result)
```

Finally, to produce messages to kafka, use `produce` function:

```python
person = {"name": "John Doe", "message": f"Hello, World!"}
producer.produce(
    topic,
    person,
    on_delivery=delivery_callback,
    headers={"key": "value"},
)
```

## Consumer

To use `superstream` with kafka consumer, first define the consumer configurations:

```python
token = "<superstream-token>"
superstream_host = "<superstream-host>"
group = "<kafka-consumer-group>"
topics = ["<kafka-topic>"]
broker = "<kafka-broker>"
conf = {
    "bootstrap.servers": broker,
    "group.id": group
}
options = Option(host=superstream_host, learning_factor=10, servers=broker)
```

Similar to producer, to initialize superstream, use `init`:

```python
result = init(token, superstream_host, conf, options)
```

Next, create a deserializer using `create_deserializer`:

```python
deserialize = create_deserializer(result, deserialize)
```

Finally, use the deserialize callable instance to deserialize consumed messages as shown below:

```python
consumer = Consumer(conf)
msg = consumer.poll(timeout=1.0)
if msg is not None:
    content = deserialize(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE, msg.headers()))
```
