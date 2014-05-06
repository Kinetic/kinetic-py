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

#@author: Clayg

import logging
import kinetic_pb2 as messages
import operations
from client import Client
from common import Entry
from collections import deque

LOG = logging.getLogger(__name__)

DEFAULT_PIPELINE_DEPTH = 16

class PipelinedClient(Client):

    def __init__(self, *args, **kwargs):
        super(PipelinedClient, self).__init__(*args, **kwargs)
        self.depth = kwargs.pop('depth', DEFAULT_PIPELINE_DEPTH)
        self._pending = set()

    @property
    def num_pending(self):
        return len(self._pending)

    ### Override BaseClient Methods ###

    def close(self):
        super(PipelinedClient, self).close()
        self._pending = set()

    def network_send(self, message, value):
        super(PipelinedClient, self).network_send(message, value)
        self._pending.add(message.command.header.sequence)
        LOG.debug('Pending sequence: %r' % self._pending)

    def network_recv(self):
        (resp,value) = super(PipelinedClient, self).network_recv()
        self._pending.remove(resp.command.header.ackSequence)
        LOG.debug('Pending sequence: %r' % self._pending)
        return (resp,value)

    ###

    def gets(self, keys, depth=None, **kwargs):
        """
        Retrieve multiple of values out of kinetic in an ordered fashion
        with pipelined requests.

        :param keys: a iterable of keys
        :param depth: the maximum number of outstanding requests

        :returns: a generator of Entry instances for the passed in keys
        """
        depth = kwargs.get('read_depth', depth) or self.depth
        seq = deque()
        ack_map = {}
        with self:
            for key in keys:
                while len(ack_map) + len(seq) >= depth:
                    # need to bring in some outstanding requests
                    (resp,value) = self.network_recv()
                    seq_id = resp.command.header.ackSequence
                    if seq_id != seq[0]:
                        ack_map[seq_id] = resp
                        try:
                            resp = ack_map.pop(seq[0])
                        except KeyError:
                            # maybe next time
                            continue
                    yield Entry.fromResponse(resp, value)
                    seq.popleft()
                # always send!
                m,v = operations.Get.build(key)
                self.update_header(m)
                self.network_send(m,v)
                seq.append(m.command.header.sequence)
            # drain remaining requests
            for next_id in seq:
                try:
                    (resp,value) = ack_map.pop(next_id)
                except KeyError:
                    for (resp,value) in self:
                        seq_id = resp.command.header.ackSequence
                        if seq_id == next_id:
                            # this is the one we're looking for!
                            break
                        # ain't it - throw it in the map
                        ack_map[seq_id] = (resp,value)
                yield Entry.fromResponse(resp, value)

    ### Iterator support ###

    def __iter__(self):
        return self

    def next(self):
        if self.num_pending <= 0:
            raise StopIteration()
        return self.network_recv()

    ###

    def puts(self, entries, depth=None, **kwargs):
        """
        Load a number of entries into kinetic with pipelined requests.

        :param entry_iter: an interable producing Entry objects
        :param depth: the maximum number of outstanding requests
        """
        depth = kwargs.get('write_depth', depth) or self.depth
        # TODO: try to yield an associated entry?
        # the interface feels better if rv is None, I guess i never liked the
        # rv from put either :\
        with self:
            for entry in entries:
                if self.num_pending >= depth:
                    self.network_recv() # TODO What is this for!? if there are operations pending the results will be lost
                m,v = operations.Put.build(entry.key,
                                           data=entry.value,
                                           new_version=entry.metadata.version)
                self.update_header(m)
                self.network_send(m,v)
            # drain remaining requests
            for r in self:
                pass

    def deletes(self, keys, depth=None):
        """
        Delete a number of keys out of kinetic with pipelined requests.

        :param keys: an iterable of keys

        If keys is a generator, it's yield will be sent a boolean indicating
        if a key was missing.

        :param depth: max number of outstanding requests
        """
        depth = depth or self.depth
        keys = iter(keys)
        if hasattr(keys, 'send'):
            # yield responses to keys if it's a generator
            iter_method = keys.send
        else:
            # otherwise, swallow deleted and consume
            iter_method = lambda deleted: keys.next()
        any_deleted = False
        missing = None
        with self:
            while True:
                try:
                    # TODO: with an ack_map we could send the missing key
                    key = iter_method(missing)
                except StopIteration:
                    break
                if self.num_pending >= depth:
                    (resp,value) = self.network_recv()
                    missing = Entry.fromResponse(resp, vaue) is None
                    any_deleted |= not missing
                #TODO(clayg): add support for versioned deletes
                # TODO: hey @clayg you are not going to support keys with versions?
                m,v = operations.Delete.build(key)
                self.update_header(m)
                self.network_send(m,v)
            # drain remaining requests
            for (resp,value) in self:
                any_deleted |= Entry.fromResponse(resp, value) is not None
        return any_deleted
