class BaseGateway(object):

    def __init__(self):
        self.receive = None

    def bind(self, receive):
        self.receive = receive

    def send(self, to, mtype, body):
        raise NotImplementedError

    def connect(self):
        pass