import udt
import zmq
import socket
import select
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
        self.listenport = listenport # This will be used for the UDT and TCP server sockets
        self.zmqctx = zmq.Context.instance() # TBC: This should probably be replaced with the zmq context in Transport.py
        self.isRunning = True   # TBC: Provide kill mechanism
    def run(self):
        print "Transport Redirector Starting"
        # Create UDT server socket #######
        self.udtserversock = udt.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        try:
            self.udtserversock.setsockopt(0, udt.UDT_REUSEADDR, True)
            self.udtserversock.setsockopt(0, udt.UDT_SNDSYN, False)
#            self.udtserversock.setsockopt(0, udt.UDT_RCVSYN, False)
            self.udtserversock.bind(('0.0.0.0',self.listenport)) # TBC: Bind to specific interface
            self.udtserversock.listen(10)
        except socket.error:
            print 'UDT server socket bind error'
        # Create TCP server socket ########
        self.tcpserversock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.tcpserversock.setblocking(0)
            self.tcpserversock.bind(('127.0.0.1',self.listenport))
            self.tcpserversock.listen(10)
        except socket.error:
            print 'TCP server socket bind error'
        # Create ZMQ server socket ########
        self.zmqserversock = self.zmqctx.socket(zmq.REP)
        try:
            self.zmqserversock.bind('inproc://transportredirector')
        except zmq.error.ZMQBindError:
            print 'ZMQ inproc server socket bind error'
        # /////////////////////////////////////////////////////////////
        udtepoll = udt.epoll()
        # set socket lists for select(s)
        zmqsocks = [self.zmqserversock] # TBC: this list will need to include local zmq peer sockets
        udtepoll.add_usock(self.udtserversock.fileno(), udt.UDT_EPOLL_IN) # TBC: this list will need to include udt peer sockets
        tcpsocks = [self.tcpserversock] # TBC: this list will need to include tcp peer sockets
        # /////////////////////////////////////////////////////////////
        while self.isRunning:
            # Select (or Epoll) main loop here - (select necessary for windows as epoll unavailable)
            # Check all listener peer connections for data, read data in, send data out via relevant peer connection
            # Check all peer sockets for incoming data, read in and send out via relevant listener peer connection
            # First check zmq
            zmqrlist, zmqwlist, zmqxlist = zmq.select(zmqsocks,zmqsocks,zmqsocks,0.1)
            for z in zmqrlist:
                print 'Zmq message ready to read - do stuff'
            # Next check udt
            udtevents = []
            try:
                udtevents = udt.epoll.epoll_wait(udtepoll,1)
            except: # TBC: Only pass expected non-blocking call errors
                pass
            for u in udtevents:
                print 'Udt data ready to read - do stuff'
            # Next check tcp
            tcprlist, tcpwlist, tcpxlist = select.select(tcpsocks,tcpsocks,tcpsocks,0.1)
            for t in tcprlist:
                print 'TCP data ready to read - do stuff'
        # ///////////////////////////////////////////////////////////////
        print "Transport Redirector Exiting"
        # Clean up
        self.udtserversock.close()
        self.tcpserversock.close()
        self.zmqctx.destroy()

if __name__ == '__main__':
    print 'Starting redirector'
    TransportRedirector(1345).start()