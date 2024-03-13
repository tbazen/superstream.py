# Superstream

## Installation

```sh
 pip install superstream
```

## Importing

```python
import superstream
from superstream.types import Option
```

## Producer

To use `superstream` with kafka producer, first define the kafka and superstream configurations:
  
```python
token = "<superstream-token>"
superstream_host = "<superstream-host>"
broker = "<kafka-broker>"
topic = "<kafka-topic>"
config = {"bootstrap.servers": broker}
options = Option(learning_factor=10, servers=broker)
```

To initialize superstream, use `init` function and pass the producer instance as an argument:

```python
producer = Producer(config)
producer = superstream.init(token, superstream_host, config, options, producer=producer)
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
config = {
    "bootstrap.servers": broker,
    "group.id": group
}
options = Option(learning_factor=10, servers=broker)
```

To initialize superstream, use `init` function and pass the consumer instance as an argument:

```python
consumer = Consumer(config)
consumer = superstream.init(token, superstream_host, config, options, consumer=consumer)
```
