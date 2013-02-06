"""
ZeroMQ PUB-SUB wrappers.
"""
from zmq.core import constants

import txzmq
from txzmq.connection import ZmqConnection

print txzmq.__file__

class ZmqPubConnection(ZmqConnection):
    """
    Publishing in broadcast manner.
    """
    socketType = constants.PUB

    def publish(self, message, tag=''):
        """
        Broadcast L{message} with specified L{tag}.

        @param message: message data
        @type message: C{str}
        @param tag: message tag
        @type tag: C{str}
        """
        self.send(tag + '\0' + message)


class ZmqXPubConnection(ZmqPubConnection):
    """
    Publishing in broadcast manner.
    """
    socketType = constants.XPUB

    def messageReceived(self, message):
        """
        Called on incoming message from ZeroMQ.

        @param message: message data
        """

        if len(message) == 1:
            message = message[0]

            if message[0] == '\1':
                self.subscribeReceived(message[1:])
            elif message[0] == '\0':
                self.unsubscribeReceived(message[1:])
            else:
                self.gotMessage(*reversed(message[0].split('\0', 1)))

        elif len(message) == 2:
            # compatibility receiving of tag as first part
            # of multi-part message
            self.gotMessage(message[1], message[0])
        else:
            raise Exception('')


    def subscribeReceived(self, tag):
        """
        Called on incoming message recevied by subscriber

        @param message: message data
        @param tag: message tag
        """
        raise NotImplementedError(self)

    def unsubscribeReceived(self, tag):
        """
        Called on incoming message recevied by subscriber

        @param message: message data
        @param tag: message tag
        """
        raise NotImplementedError(self)



class ZmqSubConnection(ZmqConnection):
    """
    Subscribing to messages.
    """
    socketType = constants.SUB

    def subscribe(self, tag):
        """
        Subscribe to messages with specified tag (prefix).

        @param tag: message tag
        @type tag: C{str}
        """
        self.socket.setsockopt(constants.SUBSCRIBE, tag)

    def unsubscribe(self, tag):
        """
        Unsubscribe from messages with specified tag (prefix).

        @param tag: message tag
        @type tag: C{str}
        """
        self.socket.setsockopt(constants.UNSUBSCRIBE, tag)

    def messageReceived(self, message):
        """
        Called on incoming message from ZeroMQ.

        @param message: message data
        """
        if len(message) == 2:
            # compatibility receiving of tag as first part
            # of multi-part message
            self.gotMessage(message[1], message[0])
        else:
            self.gotMessage(*reversed(message[0].split('\0', 1)))

    def gotMessage(self, message, tag):
        """
        Called on incoming message recevied by subscriber

        @param message: message data
        @param tag: message tag
        """
        raise NotImplementedError(self)
