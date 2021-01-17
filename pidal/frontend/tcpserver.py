from tornado.tcpserver import TCPServer

from pidal.connection_delegate import ConnectionDelegate


class ProxyTCPServer(TCPServer, ConnectionDelegate):
    pass
