# Copyright (C) 2014 Seagate Technology.
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

#@author: Ignacio Corderi

import itertools
import logging
import socket
import time
from binascii import hexlify
from hashlib import sha1
import hmac
import struct
import common
import kinetic_pb2 as messages

LOG = logging.getLogger(__name__)

def calculate_hmac(secret, message):
    mac = hmac.new(secret, digestmod=sha1)

    def update(entity):
        if not entity:
            return
        if hasattr(entity, 'SerializeToString'):
            entity = entity.SerializeToString()
        #converting to big endian to be compatible with java implementation.
        mac.update(struct.pack(">I", len(entity)))
        mac.update(entity)

    # always add command
    update(message.command)

    # skip value if message.command.body has a tag

    # Value dropped from HMAC on Dec 17

#     try:
#         skipValue = bool(message.command.body.keyValue.tag)
#     except AttributeError:
#         skipValue = False
#
#     if not skipValue:
#         update(message.value)

    d = mac.digest()
    if LOG.isEnabledFor(logging.DEBUG):
        LOG.debug('message hmac: %s' % hexlify(d))
    return d

class BaseClient(object):

    # defaults
    HOSTNAME = 'localhost'
    PORT = 8123
    CHUNK_SIZE = 4096
    CLUSTER_VERSION = 0
    # development default
    USER_ID = 1
    CLIENT_SECRET = 'asdfasdf'
    CONNECT_TIMEOUT = 0.1
    SOCKET_TIMEOUT = 5.0

    def __init__(self, hostname=HOSTNAME, port=PORT, identity=USER_ID,
                 cluster_version=CLUSTER_VERSION, secret=CLIENT_SECRET,
                 chunk_size=CHUNK_SIZE,
                 connect_timeout=CONNECT_TIMEOUT, socket_timeout=SOCKET_TIMEOUT,
                 socket_address=None,
                 socket_port=0):
        self.hostname = hostname
        self.port = port
        self.identity = identity
        self.cluster_version = cluster_version
        self.secret = secret
        self.chunk_size = chunk_size
        self._socket = None
        self._buff = ''
        self.debug = False
        self._closed = True
        self.connect_timeout = connect_timeout
        self.socket_timeout = socket_timeout
        self.socket_address = socket_address
        self.socket_port = socket_port

    @property
    def socket(self):
        if not self._socket:
            raise NotConnected()
        return self._socket

    @property
    def isConnected(self):
        return not self._closed

    def build_socket(self):
       return socket.socket()

    def connect(self):
        if self._socket:
            raise common.AlreadyConnected("Client is already connected.")

        # Stage socket on a local variable first
        s = self.build_socket()
        s.settimeout(self.connect_timeout)
        if self.socket_address:
            LOG.debug("Client local port address bound to " + self.socket_address)
            s.bind((self.socket_address, self.socket_port))
        # if connect fails, there is nothing to clean up
        s.connect((self.hostname, self.port))
        s.settimeout(self.socket_timeout)

        # We are connected now, update attributes
        self._socket = s
        self._sequence = itertools.count()
        self.connection_id = int(time.time())
        self._closed = False

    def has_data_available(self):
        tmp = self._socket.recv(1, socket.MSG_PEEK)
        return len(tmp) > 0

    def close(self):
        self._closed = True
        if self._socket:
            try:
                self._socket.shutdown(socket.SHUT_RDWR)
                self._socket.close()
            except: pass # if socket wasnt connected, keep going
        self._buff = ''
        self._socket = None
        self.connection_id = None
        self._sequence = itertools.count()


    def update_header(self, message):
        """
        Updates the message header with connection specific information.
        The unique sequence is assigned by this method.

        :param message: message to be modified (message is modified in place)
        """
        header = message.command.header
        header.clusterVersion = self.cluster_version
        header.identity = self.identity
        header.connectionID = self.connection_id
        header.sequence = self._sequence.next()
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Header updated. Connection=%s, Sequence=%s" % (header.connectionID, header.sequence))

    def _send_delimited_v2(self, header, value):
        # build message (without value) to write
        out = header.SerializeToString()

        value_ln = 0
        if value:
            value_ln = len(value)

        # 1. write magic number
        # 2. write protobuf message message size, 4 bytes
        # 3. write attached value size, 4 bytes
        buff = struct.pack(">Bii",ord('F'), len(out), value_ln)
        self.socket.send(buff)

        # 4. write protobuf message byte[]
        self.socket.send(out)

        # 5 (optional) write attached value if any
        if value_ln  > 0:
            # write value
            to_send = len(value)
            i = 0
            while i < to_send:
                nbytes = self.socket.send(value[i:i + self.chunk_size])
                if not nbytes:
                    raise common.ServerDisconnect('Server send disconnect')
                i += nbytes


    def network_send(self, header, value):
        """
        Sends a raw message.
        The HMAC is calculated and added to the message.
        Important: update_header must be called before sending the message.

        :param message: the message to be sent
        """
        # fail fast on NotConnected
        self.socket

        header.hmac = calculate_hmac(self.secret, header)

        if self.debug:
            print header

#         LOG.debug("Sending message: %s" % message)
        self._send_delimited_v2(header, value)

        return header

    def toHexString(self, array):
        return ''.join('%02x ' % ord(byte) for byte in array)

    def bytearray_to_hex(self, array):
        return ''.join('%02x ' % byte for byte in array)

    def fast_read(self, toread):
        buf = bytearray(toread)
        view = memoryview(buf)
        while toread:
            nbytes = self.socket.recv_into(view, toread)
            if nbytes == 0:
                raise common.ServerDisconnect("Connection closed by peer")
            view = view[nbytes:]
            toread -= nbytes
        return buf

    def _recv_delimited_v2(self):
        # receive the leading 9 bytes

        msg = self.fast_read(9)

        magic, proto_ln, value_ln = struct.unpack_from(">bii", buffer(msg))

        if magic!= 70:
            LOG.warn("Magic number = {0}".format(self.bytearray_to_hex(buff)))
            raise common.KineticClientException("Invalid Magic Value!") # 70 = 'F'

        if self.debug:
            print "Proto.size={0}".format(proto_ln)

        # read proto message
        raw_proto = self.fast_read(proto_ln)

        if self.debug:
            print "Proto.read={0}".format(len(raw_proto))

        value = None
        if value_ln > 0:
            # read value
            value = self.fast_read(value_ln)

        proto = messages.Message()
        proto.ParseFromString(str(raw_proto))

        if self.debug:
            print proto

        return (proto, value)


    def _recv_delimited_v2_old(self):
        # receive the leading 9 bytes
        buff = ''
        while len(buff) < 9:
            buff += self.socket.recv(9 - len(buff))
        header = struct.unpack(">bii", buff)

        if header[0] != 70:
            LOG.warn("Header:{0}".format(self.toHexString(buff)))
            raise common.KineticClientException("Invalid Magic Value!") # 70 = 'F'

        if self.debug:
            print "Proto.size={0}".format(header[1])

        # read proto message
        raw_proto = ''
        while len(raw_proto) < header[1]:
            raw_proto += self.socket.recv(header[1] -  len(raw_proto))

        if self.debug:
            print "Proto.read={0}".format(len(raw_proto))

        if header[2] > 0:
            # read value
            buff = ''
            bytes_remaining = header[2]
            while bytes_remaining > 0:
                if bytes_remaining < self.chunk_size:
                    chunk = self.socket.recv(bytes_remaining)
                else:
                    chunk = self.socket.recv(self.chunk_size)
                if not chunk:
                    raise common.ServerDisconnect('Server recv disconnect')
                bytes_remaining -= len(chunk)
                buff += chunk

        resp = messages.Message()
        resp.ParseFromString(raw_proto)

        if self.debug:
            print resp

        if header[2] > 0:
            resp.value = buff

        return resp

    def network_recv(self):
        """
        Receives a raw Kinetic message from the network.

        return: the message received
        """

        resp = self._recv_delimited_v2()

        # update connectionId to whatever the drive said.
        self.connection_id = resp[0].command.header.connectionID
#         LOG.debug("Received response: %s" % resp)
        return resp

    def send(self, header, value):
       self.network_send(header, value)
       resp = self.network_recv()

       if (resp[0].command.status.code != messages.Message.Status.SUCCESS) :
            raise common.KineticMessageException(resp[0].command.status)

       return resp

    ### with statement support ###

    def __enter__(self):
        if not self.isConnected:
            self._temporaryConnection = True
            self.connect()
        else:
            self._temporaryConnection = False
        return self

    def __exit__(self, t, v, tb):
        if self._temporaryConnection:
            self.close()
        self._temporaryConnection = None

    ### Object overrides ###

    def __str__(self):
        return "{hostname}:{port}".format(hostname=self.hostname, port=self.port)

