from typing import Dict, List

from pydantic import BaseModel, validator


class ClientReconnectionUpdateReq(BaseModel):
    new_nats_connection_id: str
    client_id: int


class ClientTypeUpdateReq(BaseModel):
    client_id: int
    type: str


class Update(BaseModel):
    Type: str
    Payload: bytes


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
    producer_return_successes: bool
    producer_flush_max_messages: int
    producer_compression_level: str
    consumer_fetch_min: int
    consumer_fetch_default: int
    consumer_retry_backoff: int
    consumer_max_wait_time: int
    consumer_max_processing_time: int
    consumer_return_errors: bool
    consumer_offset_auto_commit_enable: bool
    consumer_offset_auto_commit_interval: int
    consumer_offsets_initial: int
    consumer_offsets_retry_max: int
    consumer_group_session_timeout: int
    consumer_group_heart_beat_interval: int
    consumer_group_rebalance_timeout: int
    consumer_group_rebalance_retry_max: int
    consumer_group_rebalance_retry_back_off: int
    consumer_group_rebalance_reset_invalid_offsets: bool
    consumer_group_id: str
    servers: str
    producer_topics_partitions: Dict[str, List[int]] = {}
    consumer_group_topics_partitions: Dict[str, List[int]] = {}


class TopicsPartitionsPerProducerConsumer(BaseModel):
    producer_topics_partitions: Dict[str, List[int]]
    consumer_group_topics_partitions: Dict[str, List[int]]


class ClientCounters(BaseModel):
    total_bytes_before_reduction: int
    total_bytes_after_reduction: int
    total_messages_successfully_produce: int
    total_messages_successfully_consumed: int
    total_messages_failed_produce: int
    total_messages_failed_consume: int


class RegisterResp(BaseModel):
    client_id: int
    account_name: str
    learning_factor: int


class RegisterReq(BaseModel):
    nats_connection_id: str
    language: str
    version: str
    learning_factor: int
    config: ClientConfig


class Option(BaseModel):
    host: str
    learning_factor: int
    consumer_group: str
    servers: str

    def __init__(
        self,
        learning_factor: int = 0,
        consumer_group: str = "",
        servers: str = "",
    ):
        super().__init__(
            host="broker.superstream.dev",
            learning_factor=learning_factor,
            consumer_group=consumer_group,
            servers=servers,
        )

    @validator("learning_factor")
    def validate_learning_factor(cls, v):
        if v < 0 or v > 10_000:
            raise ValueError("learning factor should be in range of 0 to 10000")
        return v
