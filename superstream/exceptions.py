class ErrGenerateConnectionId(Exception):
    def __init__(self, e: Exception) -> None:
        super().__init__(f"error getting nats connection id: {e!s}")
        self.e = e
