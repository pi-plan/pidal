import abc


class ConnectionDelegate(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def on_close():
        pass

    @abc.abstractmethod
    def on_send():
        pass

    @abc.abstractmethod
    def on_recv():
        pass
