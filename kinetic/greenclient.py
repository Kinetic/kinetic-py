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

from collections import deque

from kinetic import operations
from baseasync import BaseAsync
import common

import eventlet
eventlet.monkey_patch()

DEFAULT_DEPTH = 16

class Response(object):

    def __init__(self):
        self.resp = eventlet.event.Event()
        self._hasError = False

    def setResponse(self, v):
        self.resp.send(v)

    def setError(self, e):
        self._hasError = True
        self.resp.send(e)

    def wait(self):
        resp = self.resp.wait()
        if self._hasError:
            raise resp
        else:
            return resp

    def ready(self):
        return self.resp.ready()

class GreenClient(BaseAsync):

    def __init__(self, *args, **kwargs):
        super(GreenClient, self).__init__(*args, **kwargs)
        self._running = False

    def connect(self):
        super(GreenClient, self).connect()
        self._dispatcher = eventlet.spawn(self._run)
        self._running = True

    def close(self, flush=True, timeout=None):
        if self._running:
            self._running = False
            with eventlet.Timeout(timeout, False):
                if flush:
                    self._dispatcher.wait()
            if not self._dispatcher.dead:
                self._dispatcher.kill()
        super(GreenClient, self).close()

    def _flush(self):
        while self._pending:
            self._wait()

    def wait(self):
        while self._pending:
            eventlet.sleep(0.05)

    def _run(self):
        while self._running:
            if len(self._pending) > 0:
                self._async_recv()
            else:
                eventlet.sleep(0.1)
        while len(self._pending) > 0:
            self._async_recv()

    def submit(self, op, *args, **kwargs):
        promise = Response()
        self._processAsync(op, promise.setResponse, promise.setError, *args, **kwargs)
        return promise

    def put(self, key, data, *args, **kwargs):
        return self.submit(operations.Put, key, data, *args, **kwargs)

    def get(self, key, *args, **kwargs):
        return self.submit(operations.Get, key, *args, **kwargs)

    def delete(self, key, *args, **kwargs):
        return self.submit(operations.Delete, key, *args, **kwargs)

    def getPrevious(self, *args, **kwargs):
        return self.submit(operations.GetPrevious, *args, **kwargs)

    def getKeyRange(self, *args, **kwargs):
        return self.submit(operations.GetKeyRange, *args, **kwargs)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, t, v, tb):
        self.close()

    def put_entries(self, entries, depth=DEFAULT_DEPTH):
        pending = deque()
        for entry in entries:
            if len(pending) >= depth:
                # we have to wait on something, may as well be the oldest?
                pending.popleft().wait()
            pending.append(self.put(entry.key, entry.value))
        for resp in pending:
            resp.wait()

    def get_keys(self, keys, depth=DEFAULT_DEPTH):
        pending = deque()
        for key in keys:
            if len(pending) >= depth:
                yield pending.popleft().wait()
            pending.append(self.get(key))
        for resp in pending:
            yield resp.wait()

    def delete_keys(self, keys, depth=DEFAULT_DEPTH):
        """
        Delete a number of keys out of kinetic with pipelined requests.

        :param keys: an iterable of keys

        If keys is a generator, it's yield will be sent a boolean indicating
        if a key was missing.

        :param depth: max number of outstanding requests
        """
        keys = iter(keys)
        if hasattr(keys, 'send'):
            # yield responses to keys if it's a generator
            iter_method = keys.send
        else:
            # otherwise, swallow deleted and consume
            iter_method = lambda deleted: keys.next()
        pending = deque()
        any_deleted = False
        missing = None
        while True:
            try:
                key = iter_method(missing)
            except StopIteration:
                break
            pending.append(self.delete(key))
            if len(pending) >= depth:
                missing = not pending.popleft().wait()
                any_deleted |= not missing
        # drain remaining requests
        for resp in pending:
            found = resp.wait()
            any_deleted |= found
        return any_deleted

    def push_keys(self, target, keys, batch=16):
        host, port = target.split(':')
        port = int(port)
        key_batch = []
        results = []
        for key in keys:
            key_batch.append(key)
            if len(key_batch) < batch:
                continue
            # send a batch
            resp = self.submit(operations.PushKeys, key_batch, host, port)
            results.extend(resp.wait())
            key_batch = []
        if key_batch:
            resp = self.submit(operations.PushKeys, key_batch, host, port)
            results.extend(resp.wait())
        return results
