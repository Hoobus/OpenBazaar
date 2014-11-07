import udt
import zmq
from socket import *
from threading import Thread

# This class will act as the conduit between zmq and the network/other nodes.
# The class exposes a series of listening sockets as follows:
# 1 - UDT server socket bound to udp/12345 by default for receiving connections from remote peers via UDT (reliable UDP)
# 2 - TCP server socket bound to tcp/12345 by default for receiving connections from remote peers via TCP
# 3 - ZMQ server socket bound to inproc://redirector for receiving ZMQ connections destined for remote peers
# New inbound ZMQ connections will result in the creation of a new UDT or TCP socket to the remote peer

class TransportRedirector(Thread):
    def __init__(self, listenport):
        Thread.__init__(self)
        self.listenport = listenport
        self.zmqctx = zmq.Context.instance() # This should probably be replaced with the OB context in Transport.py
        # create listeners
        # UDT server socket
        self.udtserversock = udt.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        # TCP server socket
        self.tcpserversock = socket.socket(AF_INET, SOCK_STREAM)
        # ZMQ server socket
        self.zmqserversocket = self.zmqctx.socket(zmq.REP)

    def run(self):
        print "Transport Redirector Starting"
        # Select (or Epoll) loop here
        # Check all listener peer connections for data, read data in, send data out via relevant peer connection
        # Check all peer sockets for incoming data, read in and send out via relevant listener peer connection
        print "Transport Redirector Exiting"
        # Clean up
