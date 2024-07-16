from typing import Any, Dict, List

from pydantic import BaseModel


class ClientReconnectionUpdateReq(BaseModel):
    new_nats_connection_id: str
    client_hash: str


class ClientTypeUpdateReq(BaseModel):
    client_hash: str
    type: str


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
    producer_topics_partitions: Dict[str, List[int]]
    consumer_group_topics_partitions: Dict[str, List[int]]


class SuperstreamCounters(BaseModel):
    total_bytes_before_reduction: int
    total_bytes_after_reduction: int
    total_messages_successfully_produce: int
    total_messages_successfully_consumed: int
    total_messages_failed_produce: int
    total_messages_failed_consume: int

    def reset(self):
        self.total_bytes_before_reduction = 0
        self.total_bytes_after_reduction = 0
        self.total_messages_successfully_produce = 0
        self.total_messages_successfully_consumed = 0
        self.total_messages_failed_produce = 0
        self.total_messages_failed_consume = 0

    def increment_total_bytes_before_reduction(self, bytes):
        self.total_bytes_before_reduction += bytes

    def increment_total_bytes_after_reduction(self, bytes):
        self.total_bytes_after_reduction += bytes

    def increment_total_messages_successfully_produce(self):
        self.total_messages_successfully_produce += 1

    def increment_total_messages_successfully_consumed(self):
        self.total_messages_successfully_consumed += 1

    def increment_total_messages_failed_produce(self):
        self.total_messages_failed_produce += 1

    def increment_total_messages_failed_consume(self):
        self.total_messages_failed_consume += 1


class LearnedSchemaUpdate(BaseModel):
    master_msg_name: str
    file_name: str
    schema_id: str
    desc: bytes


class ToggleReductionUpdate(BaseModel):
    enable_reduction: bool


class CompressionUpdate(BaseModel):
    enable_compression: bool
    compression_type: str


class RegisterResp(BaseModel):
    account_name: str
    learning_factor: int
    client_hash: str


class RegisterReq(BaseModel):
    nats_connection_id: str
    language: str
    version: str
    learning_factor: int
    config: Dict[str, Any]
    reduction_enabled: bool
    connection_id: int
    tags: str
