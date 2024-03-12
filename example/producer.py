from __future__ import annotations

import asyncio
import sys

from confluent_kafka import Producer

import superstream
from superstream.types import Option


async def main():
    try:
        token = "<superstream-token>"
        superstream_host = "<superstream-host>"
        broker = "<kafka-broker>"
        topic = "<kafka-topic>"
        config = {"bootstrap.servers": broker}
        options = Option(host=superstream_host, learning_factor=10, servers=broker)

        producer = Producer(config)
        producer = superstream.init(token, superstream_host, config, options, producer=producer)

        def delivery_callback(err, msg):
            if err:
                sys.stderr.write("%% Message failed delivery: %s\n" % err)
            else:
                sys.stderr.write("[%s: %d] %s\n" % (msg.topic(), msg.partition(), msg.value()))

        for index in range(10_000):
            person = {"name": "John", "message": f"Hello, World! {index}"}
            try:
                producer.produce(
                    topic,
                    person,
                    on_delivery=delivery_callback,
                    headers={"key": "value"},
                )
                producer.poll(0)
                await asyncio.sleep(0.5)

            except BufferError:
                sys.stderr.write(
                    "%% Local producer queue is full (%d messages awaiting delivery): try again\n" % len(producer)
                )
            except Exception as e:
                print(e)

        sys.stderr.write("%% Waiting for %d deliveries\n" % len(producer))
        producer.flush()
    except Exception:
        sys.stderr.write("%% Producer stopped\n")


if __name__ == "__main__":
    asyncio.run(main())
