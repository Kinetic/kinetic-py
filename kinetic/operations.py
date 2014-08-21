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

from common import Entry
from common import EntryMetadata
from common import KineticMessageException
import common
import kinetic_pb2 as messages
import logging

LOG = logging.getLogger(__name__)


def _check_status(command):
    if (command.status.code == messages.Command.Status.SUCCESS):
        return
    elif(command.status.code == messages.Command.Status.VERSION_FAILURE):
        raise common.ClusterVersionFailureException(command.status, command.header.clusterVersion)
    else:
        raise KineticMessageException(command.status)


def _buildMessage(messageType, key, data=None, version='', new_version='',
                  force=False, tag=None, algorithm=None, synchronization=None):
    m = messages.Command()
    m.header.messageType = messageType
    if len(key) > common.MAX_KEY_SIZE: raise common.KineticClientException("Key exceeds maximum size of {0} bytes.".format(common.MAX_KEY_SIZE))
    m.body.keyValue.key = key
    if data:
        if len(data) > common.MAX_VALUE_SIZE: raise common.KineticClientException("Value exceeds maximum size of {0} bytes.".format(common.MAX_VALUE_SIZE))

    if tag and algorithm:
        m.body.keyValue.tag = tag
        m.body.keyValue.algorithm = algorithm
    elif messageType == messages.Command.PUT:
        m.body.keyValue.tag = 'l337'
        m.body.keyValue.algorithm = 1 # nacho: should be change to a value over 100
    if synchronization:
        m.body.keyValue.synchronization = synchronization
    if version:
        m.body.keyValue.dbVersion = version
    if new_version:
        m.body.keyValue.newVersion = new_version
    if force:
        m.body.keyValue.force = True

    return (m,data)

class Noop(object):

    @staticmethod
    def build():
        m = messages.Command()
        m.header.messageType = messages.Command.NOOP
        return (m, None)

    @staticmethod
    def parse(m, value):
        return

    @staticmethod
    def onError(e):
        raise e

class Put(object):

    @staticmethod
    def build(key, data, version="", new_version="", **kwargs):
        return _buildMessage(messages.Command.PUT, key, data, version, new_version, **kwargs)

    @staticmethod
    def parse(m, value):
        return

    @staticmethod
    def onError(e):
        raise e

class Get(object):

    @staticmethod
    def build(key):
        return _buildMessage(messages.Command.GET, key)

    @staticmethod
    def parse(m, value):
        return Entry.fromResponse(m, value)

    @staticmethod
    def onError(e):
        if isinstance(e,KineticMessageException):
            if e.code and e.code == 'NOT_FOUND':
                return None
        raise e

class GetMetadata(object):

    @staticmethod
    def build(key):
        (m,_) = _buildMessage(messages.Command.GET, key)
        m.body.keyValue.metadataOnly = True
        return (m, None)

    @staticmethod
    def parse(m, value):
        return Get.parse(m, value)

    @staticmethod
    def onError(e):
        return Get.onError(e)

class Delete(object):

    @staticmethod
    def build(key, version="", **kwargs):
        return _buildMessage(messages.Command.DELETE, key, version=version, **kwargs)

    @staticmethod
    def parse(m, value):
        return True

    @staticmethod
    def onError(e):
        if isinstance(e,KineticMessageException):
            if e.code and e.code == 'NOT_FOUND':
                return False
        raise e

class GetNext(object):

    @staticmethod
    def build(key):
        return _buildMessage(messages.Command.GETNEXT, key)

    @staticmethod
    def parse(m, value):
        return Get.parse(m, value)

    @staticmethod
    def onError(e):
        return Get.onError(e)

class GetPrevious(object):

    @staticmethod
    def build(key):
        return _buildMessage(messages.Command.GETPREVIOUS, key)

    @staticmethod
    def parse(m, value):
        return Get.parse(m, value)

    @staticmethod
    def onError(e):
        return Get.onError(e)

class GetKeyRange(object):

    @staticmethod
    def build(startKey=None, endKey=None, startKeyInclusive=True, endKeyInclusive=True, maxReturned=200):
        if not startKey:
            startKey = ''
        if not endKey:
            endKey = '\xFF' * common.MAX_KEY_SIZE

        if len(startKey) > common.MAX_KEY_SIZE: raise common.KineticClientException("Start key exceeds maximum size of {0} bytes.".format(common.MAX_KEY_SIZE))
        if len(endKey) > common.MAX_KEY_SIZE: raise common.KineticClientException("End key exceeds maximum size of {0} bytes.".format(common.MAX_KEY_SIZE))

        m = messages.Command()
        m.header.messageType = messages.Command.GETKEYRANGE

        kr = m.body.range
        kr.startKey = startKey
        kr.endKey = endKey
        kr.startKeyInclusive = startKeyInclusive
        kr.endKeyInclusive = endKeyInclusive
        kr.maxReturned = maxReturned

        return (m, None)

    @staticmethod
    def parse(m, value):
        return [k for k in m.body.range.keys] # key is actually a set of keys

    @staticmethod
    def onError(e):
        raise e

class GetVersion(object):

    @staticmethod
    def build(key):
        (m,_) = _buildMessage(messages.Command.GETVERSION, key)
        return (m, None)

    @staticmethod
    def parse(m, value):
        return m.body.keyValue.dbVersion

    @staticmethod
    def onError(e):
        if isinstance(e,KineticMessageException):
            if e.code and e.code == 'NOT_FOUND':
                return None
        raise e

class P2pPush(object):

    @staticmethod
    def build(keys, hostname='localhost', port=8123, tls=False):
        m = messages.Command()
        m.header.messageType = messages.Command.PEER2PEERPUSH
        m.body.p2pOperation.peer.hostname = hostname
        m.body.p2pOperation.peer.port = port
        m.body.p2pOperation.peer.tls = tls

        operations = []
        for k in keys:
            op = None
            if isinstance(k, str):
                op = messages.Command.P2POperation.Operation(key=k)
            elif isinstance(k, common.P2pOp):
                op = messages.Command.P2POperation.Operation(key=k.key)
                if k.version:
                    op.version = k.version
                if k.newKey:
                    op.newKey = k.newKey
                if k.force:
                    op.force = k.force
            operations.append(op)

        m.body.p2pOperation.operation.extend(operations)

        return (m, None)

    @staticmethod
    def parse(m, value):
        return [op for op in m.body.p2pOperation.operation]

    @staticmethod
    def onError(e):
        raise e

class P2pPipedPush(object):

    @staticmethod
    def build(keys, targets):
        m = messages.Command()
        m.header.messageType = messages.Command.PEER2PEERPUSH
        m.body.p2pOperation.peer.hostname = targets[0].hostname
        m.body.p2pOperation.peer.port = targets[0].port
        if targets[0].tls:
            m.body.p2pOperation.peer.tls = targets[0].tls


        def rec(targets, op):
            if len(targets) > 0:
                target = targets[0]
                op.p2pop.peer.hostname = target.hostname
                op.p2pop.peer.port = target.port
                if target.tls:
                    op.p2pop.peer.tls = target.tls
                innerop = messages.Command.P2POperation.Operation(key=op.key)
                if op.version:
                    innerop.version = op.version
                if op.force:
                    innerop.force = op.force

                rec(targets[1:],innerop)

                op.p2pop.operation.extend([innerop])

        warn_newKey = False
        operations = []
        for k in keys:
            op = None
            if isinstance(k, str):
                op = messages.Command.P2POperation.Operation(key=k)
            elif isinstance(k, common.P2pOp):
                op = messages.Command.P2POperation.Operation(key=k.key)
                if k.version:
                    op.version = k.version
                if k.newKey:
                    warn_newKey = True
                if k.force:
                    op.force = k.force

            rec(targets[1:],op)

            operations.append(op)

        if warn_newKey:
            LOG.warn("Setting new key on piped push is not currently supported.")

        m.body.p2pOperation.operation.extend(operations)

        return (m, None)

    @staticmethod
    def parse(m, value):
        return [op for op in m.body.p2pOperation.operation]

    @staticmethod
    def onError(e):
        raise e

class PushKeys(object):

    @staticmethod
    def build(keys, hostname='localhost', port=8123, **kwargs):
        m = messages.Command()
        m.header.messageType = messages.Command.PEER2PEERPUSH
        m.body.p2pOperation.peer.hostname = hostname
        m.body.p2pOperation.peer.port = port

        m.body.p2pOperation.operation.extend([
            messages.Command.P2POperation.Operation(key=key) for key in keys
        ])

        return (m, None)

    @staticmethod
    def parse(m, value):
        return [op for op in m.body.p2pOperation.operation]

    @staticmethod
    def onError(e):
        raise e

class Flush(object):

    @staticmethod
    def build():
        m = messages.Command()
        m.header.messageType = messages.Command.FLUSHALLDATA

        return (m, None)

    @staticmethod
    def parse(m, value):
        return

    @staticmethod
    def onError(e):
        raise e


### Admin Operations ###

class GetLog(object):

    @staticmethod
    def build(types, device=None):
        m = messages.Command()
        m.header.messageType = messages.Command.GETLOG

        log = m.body.getLog
        log.types.extend(types) #type is actually a repeatable field

        if device:
            log.device.name = device

        return (m, None)

    @staticmethod
    def parse(m, value):
        if value:
            return (m.body.getLog, value)
        else:
            return m.body.getLog

    @staticmethod
    def onError(e):
        if isinstance(e,KineticMessageException):
            if e.code and e.code == 'NOT_FOUND':
                return None
        raise e

class Setup(object):

    @staticmethod
    def build(**kwargs):
        m = messages.Command()
        m.header.messageType = messages.Command.SETUP

        op = m.body.setup

        value = None

        if "newClusterVersion" in kwargs:
            op.newClusterVersion = kwargs["newClusterVersion"]

        if "instantSecureErase" in kwargs:
            op.instantSecureErase = kwargs["instantSecureErase"]

        if "pin" in kwargs and kwargs["pin"] is not None:
            op.pin = kwargs["pin"]

        if "setPin" in kwargs:
            op.setPin = kwargs["setPin"]

        if "firmware" in kwargs:
            op.firmwareDownload = True
            value = kwargs['firmware']

        return (m, value)

    @staticmethod
    def parse(m, value):
        return

    @staticmethod
    def onError(e):
        raise e

class Security(object):

    @staticmethod
    def build(acls):
        m = messages.Command()
        m.header.messageType = messages.Command.SECURITY

        proto_acls = []

        for acl in acls:
            proto_acl = messages.Command.Security.ACL(identity=acl.identity,
                                                      key=acl.key,
                                                      hmacAlgorithm=acl.hmacAlgorithm)

            proto_domains = []

            for domain in acl.domains:
                proto_d = messages.Command.Security.ACL.Scope(
                            TlsRequired=domain.tlsRequired)

                proto_d.permission.extend(domain.roles)

                if domain.offset:
                    proto_d.offset = domain.offset
                if domain.value:
                    proto_d.value = domain.value

                proto_domains.append(proto_d)

            proto_acl.scope.extend(proto_domains)
            proto_acls.append(proto_acl)

        m.body.security.acl.extend(proto_acls)

        return (m, None)

    @staticmethod
    def parse(m, value):
        return

    @staticmethod
    def onError(e):
        raise e
