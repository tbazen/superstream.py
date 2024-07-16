from __future__ import annotations

import asyncio
import json
import sys

from confluent_kafka import Producer


async def main():
    try:
        brokers = "<kafka-broker>"
        topic = "<kafka-topic>"
        config = {
            "bootstrap.servers": brokers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "",
            "sasl.password": "",
        }

        producer = Producer(config)

        def delivery_callback(err, msg):
            if err:
                sys.stderr.write("%% Message failed delivery: %s\n" % err)
            else:
                sys.stderr.write("[%s: %d] %s\n" % (msg.topic(), msg.partition(), msg.value()))

        for index in range(10_000):
            person = {"name": "John", "message": f"Hello, World! {index}"}
            message = json.dumps(person)
            try:
                producer.produce(
                    topic,
                    message,
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
