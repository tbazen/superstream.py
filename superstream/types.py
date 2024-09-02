import threading
from enum import Enum
from typing import Any, ClassVar, Dict, List, Optional, Self

from pydantic import BaseModel, model_validator

from superstream.std import SuperstreamStd


class ClientReconnectionUpdateReq(BaseModel):
    new_nats_connection_id: str
    client_hash: str


class ClientTypeUpdateReq(BaseModel):
    client_hash: str
    type: str


class ClientConfigUpdateReq(BaseModel):
    client_hash: str
    config: Dict[str, Any]


class Update(BaseModel):
    type: str
    payload: bytes


class SchemaUpdateReq(BaseModel):
    master_msg_name: str
    file_name: str
    schema_id: str
    desc: bytes


class GetSchemaReq(BaseModel):
    schema_id: str


class ClientConfig(BaseModel):
    client_type: str
    producer_max_messages_bytes: int
    producer_required_acks: str
    producer_timeout: int
    producer_retry_max: int
    producer_retry_backoff: int
    producer_return_errors: bool
    producer_flush_max_messages: int
    producer_compression_level: str
    consumer_fetch_min: int
    consumer_fetch_default: int
    consumer_max_wait_time: int
    consumer_return_errors: bool
    consumer_offset_auto_commit_enable: bool
    consumer_offset_auto_commit_interval: int
    consumer_offsets_initial: int
    consumer_group_session_timeout: int
    consumer_group_heart_beat_interval: int
    consumer_group_rebalance_timeout: int
    consumer_group_id: str
    servers: str
    producer_topics_partitions: Dict[str, List[int]] = {}
    consumer_group_topics_partitions: Dict[str, List[int]] = {}


class TopicsPartitionsPerProducerConsumer(BaseModel):
    connection_id: int
    producer_topics_partitions: Dict[str, List[int]]
    consumer_group_topics_partitions: Dict[str, List[int]]


# class SuperstreamCounters(BaseModel):
#     total_bytes_before_reduction: int
#     total_bytes_after_reduction: int
#     total_messages_successfully_produce: int
#     total_messages_successfully_consumed: int
#     total_messages_failed_produce: int
#     total_messages_failed_consume: int

#     def reset(self):
#         self.total_bytes_before_reduction = 0
#         self.total_bytes_after_reduction = 0
#         self.total_messages_successfully_produce = 0
#         self.total_messages_successfully_consumed = 0
#         self.total_messages_failed_produce = 0
#         self.total_messages_failed_consume = 0

#     def increment_total_bytes_before_reduction(self, bytes):
#         self.total_bytes_before_reduction += bytes

#     def increment_total_bytes_after_reduction(self, bytes):
#         self.total_bytes_after_reduction += bytes

#     def increment_total_messages_successfully_produce(self):
#         self.total_messages_successfully_produce += 1

#     def increment_total_messages_successfully_consumed(self):
#         self.total_messages_successfully_consumed += 1

#     def increment_total_messages_failed_produce(self):
#         self.total_messages_failed_produce += 1

#     def increment_total_messages_failed_consume(self):
#         self.total_messages_failed_consume += 1

#   @staticmethod
#   def create():
#     return SuperstreamCounters(
#         total_bytes_before_reduction=0,
#         total_bytes_after_reduction=0,
#         total_messages_successfully_produce=0,
#         total_messages_successfully_consumed=0,
#         total_messages_failed_produce=0,
#         total_messages_failed_consume=0,
#     )


class SuperstreamCounters(BaseModel):
    total_read_bytes_reduced: Optional[int] = 0
    total_write_bytes_reduced: Optional[int] = 0
    total_read_bytes: Optional[int] = 0
    metrics: Dict[str, float] = {}
    _lock: ClassVar[threading.Lock] = threading.Lock()

    def reset(self):
        with self._lock:
            self.total_read_bytes_reduced = 0
            self.total_write_bytes_reduced = 0

    def increment_total_read_bytes_reduced(self, bytes: int):
        with self._lock:
            self.total_read_bytes_reduced += bytes

    def increment_total_write_bytes_reduced(self, bytes: int):
        with self._lock:
            self.total_write_bytes_reduced += bytes

    def increment_total_read_bytes(self, bytes: int):
        with self._lock:
            self.total_read_bytes += bytes

    def get_total_read_bytes_reduced(self) -> int:
        with self._lock:
            return self.total_read_bytes_reduced

    def get_total_write_bytes_reduced(self) -> int:
        with self._lock:
            return self.total_write_bytes_reduced

    def get_total_read_bytes(self) -> int:
        with self._lock:
            return self.total_read_bytes

    def get_producer_compression_rate(self) -> float:
        rate = self.get_producer_compression_metric()
        if rate is None or rate != rate or rate > 1.0 or rate == 1.0 or rate < 0.0:
            return 0.0
        if 0.0 < rate < 1.0:
            return 1 - rate
        return 0.0

    def get_consumer_compression_rate(self) -> float:
        total_bytes_compressed_consumed = self.get_consumer_bytes_consumed_metric()
        total_read = self.get_total_read_bytes()
        if (
            total_bytes_compressed_consumed is None
            or total_bytes_compressed_consumed != total_bytes_compressed_consumed
            or total_bytes_compressed_consumed <= 0.0
            or total_read <= 0
        ):
            return 0.0
        if total_bytes_compressed_consumed > total_read:
            return 0.0
        return 1 - (total_bytes_compressed_consumed / total_read)

    def set_metrics(self, metrics: Dict[str, float]):
        self.metrics = metrics

    def get_producer_compression_metric(self) -> Optional[float]:
        if self.metrics is not None:
            return self.metrics.get("compression-rate-avg", 0.0)
        return 0.0

    def get_consumer_bytes_consumed_metric(self) -> Optional[float]:
        if self.metrics is not None:
            return self.metrics.get("bytes-consumed-total", 0.0)
        return 0.0


class ClientCounterUpdateRequest(BaseModel):
    total_read_bytes_reduced: int
    total_write_bytes_reduced: int
    connection_id: int

    @staticmethod
    def from_superstream_counters(counter: SuperstreamCounters, connection_id: int):
        backup_read_bytes = counter.get_total_read_bytes_reduced()
        backup_write_bytes = counter.get_total_write_bytes_reduced()
        producer_compression_rate = counter.get_producer_compression_rate()
        calculated_write_bytes = round(backup_write_bytes * producer_compression_rate)

        consumer_compression_rate = counter.get_consumer_compression_rate()
        calculated_read_bytes = round(backup_read_bytes * consumer_compression_rate)

        return ClientCounterUpdateRequest(
            total_read_bytes_reduced=calculated_read_bytes,
            total_write_bytes_reduced=calculated_write_bytes,
            connection_id=connection_id,
        )


class LearnedSchemaUpdate(BaseModel):
    master_msg_name: str
    file_name: str
    schema_id: str
    desc: bytes


class ToggleReductionUpdate(BaseModel):
    enable_reduction: bool


class CompressionUpdate(BaseModel):
    enable_compression: bool
    compression_type: Optional[str]


class RegisterResp(BaseModel):
    account_name: str
    learning_factor: int
    client_hash: str

    @model_validator(mode="after")
    def client_hash_validator(self) -> Self:
        std = SuperstreamStd()
        if not self.client_hash:
            std.write(f"superstream: client_hash is not a valid string: {self.client_hash}")
        if not self.account_name:
            std.write(f"superstream: account_name is not a valid string: {self.account_name}")

        return self


class RegisterReq(BaseModel):
    nats_connection_id: str
    language: str
    version: str
    learning_factor: int
    config: dict
    reduction_enabled: bool
    connection_id: int
    tags: str
    client_ip: str
    client_host: str


class SuperstreamClientType(str, Enum):
    PRODUCER = "producer"
    CONSUMER = "consumer"
