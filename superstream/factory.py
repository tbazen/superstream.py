class SuperstreamFactory:
    _producer = None
    _consumer = None
    _topic_partition = None

    def __init__(self):
        raise Exception("This class is not meant to be instantiated")

    @staticmethod
    def set_topic_partition(topic_partition):
        SuperstreamFactory._topic_partition = topic_partition

    @staticmethod
    def set_producer(producer):
        SuperstreamFactory._producer = producer

    @staticmethod
    def set_consumer(consumer):
        SuperstreamFactory._consumer = consumer

    @staticmethod
    def create_producer(config):
        return SuperstreamFactory._producer(config)

    @staticmethod
    def create_consumer(config):
        return SuperstreamFactory._consumer(config)

    @staticmethod
    def create_topic_partition(*args, **kwargs):
        return SuperstreamFactory._topic_partition(*args, **kwargs)
