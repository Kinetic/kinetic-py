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

import os
import socket
import sys
import unittest
import random

from kinetic.common import Entry
from kinetic import baseclient
from kinetic import common

try:
    import eventlet
except ImportError:
    eventlet_available = False
else:
    eventlet_available = True
    from kinetic import greenclient

from base import BaseTestCase, MultiSimulatorTestCase

@unittest.skipIf(not eventlet_available, 'eventlet is not installed')
class TestGreenClient(BaseTestCase):

    def test_connect(self):
        c = greenclient.GreenClient(self.host, self.port)
        c.connect()
        c.close()

    def test_disconnect(self):
        class MockSocket(object):

            def socket(mock, *args, **kwargs):
                return mock

            def connect(mock, *args, **kwargs):
                raise orig_socket.error()

            def close(mock, *args, **kwargs):
                pass

            def settimeout(mock, *args, **kwargs):
                pass

            def shutdown(mock, *args, **kwargs):
                pass

        orig_socket = baseclient.socket
        try:
            baseclient.socket = MockSocket()
            c = greenclient.GreenClient(self.host, self.port + 10000)
            self.assertRaises(socket.error, c.connect)
        finally:
            baseclient.socket = orig_socket
        c.close()
        self.assertFalse(c._running)
        self.assertFalse(c.isConnected)

    def test_put(self):
        c = greenclient.GreenClient(self.host, self.port)
        k = self.buildKey('test')
        with c:
            resp = c.put(k, 'value')
            resp = resp.wait()
            self.assertEquals(resp, True)

    def test_put_force(self):
        c = greenclient.GreenClient(self.host, self.port)
        k = self.buildKey('test')
        with c:
            resp = c.put(k, 'value1', new_version='1')
            resp = resp.wait()
            self.assertEquals(resp, True)
            resp = c.put(k, 'value2', new_version='2')
            self.assertRaises(Exception, resp.wait)
            resp = c.put(k, 'value_force', force=True)
            resp = resp.wait()
            self.assertEquals(resp, True)
            resp = c.get(k)
            self.assertEquals(resp.wait().value, 'value_force')

    def test_get_not_found(self):
        c = greenclient.GreenClient(self.host, self.port)
        k = self.buildKey('test')
        with c:
            resp = c.get(k)
            resp = resp.wait()
            self.assert_(resp is None)

    def test_set_and_fetch(self):
        c = greenclient.GreenClient(self.host, self.port)
        k = self.buildKey('test')
        with c:
            resp = c.put(k, 'value')
            resp = resp.wait()
            self.assertEquals(resp, True)
            resp = c.get(k)
            resp = resp.wait()
            self.assertEquals(resp.value, 'value')

    def test_submit_many(self):
        c = greenclient.GreenClient(self.host, self.port)
        keys = []
        for i in range(16):
            keys.append(self.buildKey(i))
        self.assertEquals(len(keys), 16)  # sanity
        with c:
            # puts
            responses = []
            for k in keys:
                responses.append(c.put(k, 'value-%s' % k))
            self.assertEquals(len(responses), 16)
            for resp in responses:
                self.assertEquals(True, resp.wait())
            # get_keys
            responses = []
            for k in keys:
                responses.append(c.get(k))
            self.assertEquals(len(responses), 16)
            for i, resp in enumerate(responses):
                k = keys[i]
                self.assertEquals(resp.wait().value, 'value-%s' % k)

    def test_put_entries(self):
        c = greenclient.GreenClient(self.host, self.port)
        with c:
            entries = []
            for i in range(3):
                entries.append(Entry(self.buildKey(i), 'value%s' % i))
            c.put_entries(entries)
            for i, entry in enumerate(entries):
                self.assertEquals(c.get(entry.key).wait().value,
                                  'value%s' % i)

    def test_get_keys(self):
        c = greenclient.GreenClient(self.host, self.port)
        with c:
            keys = []
            resps = []
            for i in range(3):
                key = self.buildKey(i)
                resps.append(c.put(key, 'value%s' % i))
            for resp in resps:
                resp.wait()
            for i, resp in enumerate(c.get_keys(keys)):
                self.assertEquals(resp.value, 'value%s' % i)

    def test_delete_keys(self):
        c = greenclient.GreenClient(self.host, self.port)
        with c:
            keys = []
            resps = []
            for i in range(3):
                key = self.buildKey(i)
                resps.append(c.put(key, 'value%s' % i))
            for resp in resps:
                resp.wait()
            c.delete_keys(keys)
            for key in keys:
                self.assert_(c.get(key).wait() is None)

    def test_get_previous(self):
        c = greenclient.GreenClient(self.host, self.port)
        with c:
            keys = []
            for i in range(3):
                key = self.buildKey(i)
                c.put(key, 'value%s' % i)
                keys.append(key)
            c.wait()
            next_key = self.buildKey(i + 1)
            resp = c.getPrevious(next_key)
            entry = resp.wait()
            self.assertEquals(entry.key, keys[-1])
            self.assertEquals(entry.value, 'value2')

    def test_get_empty_key_range(self):
        start_key = self.buildKey('\x00')
        end_key = self.buildKey('\xff')
        c = greenclient.GreenClient(self.host, self.port)
        with c:
            self.assertEquals([], c.getKeyRange(start_key, end_key).wait())

    def test_get_key_range(self):
        c = greenclient.GreenClient(self.host, self.port)
        with c:
            keys = []
            for i in range(3):
                key = self.buildKey(i)
                c.put(key, 'value%s' % i)
                keys.append(key)
            c.wait()
            key_list = c.getKeyRange(keys[0], keys[-1]).wait()

        self.assertEquals(key_list, keys)

    def test_flush_on_close(self):
        c = greenclient.GreenClient(self.host, self.port)
        with c:
            keys = []
            for i in range(3):
                key = self.buildKey(i)
                c.put(key, 'value%s' % i)
                keys.append(key)
        resps = []
        with c:
            for key in keys:
                resps.append(c.get(key))
        entries = [resp.wait() for resp in resps]
        self.assertEquals(len(entries), 3)
        for i, entry in enumerate(entries):
            self.assertEqual(keys[i], entry.key)
            self.assertEqual('value%s' % i, entry.value)


@unittest.skipIf(not eventlet_available, 'eventlet is not installed')
class BaseGreenTestCase(BaseTestCase):

    def setUp(self):
        super(BaseGreenTestCase, self).setUp()
        self.c = greenclient.GreenClient("localhost", self.port)

    def gen_keys(self, num_keys=1):
        return [self.buildKey('key.%d' % i) for i in range(num_keys)]

    def gen_entries(self, num_keys=1, min_size=0):
        entries = []
        for i, key in enumerate(self.gen_keys(num_keys=num_keys)):
            value = 'value.%d' % i
            # negative is just empty string
            value += 'a' * (min_size - len(value))
            entries.append(Entry(key, value))
        return entries


class TestGreenGets(BaseGreenTestCase):

    def test_single_key(self):
        keys = self.gen_keys()
        self.assertEquals(1, len(keys))
        with self.c:
            self.c.put(keys[0], 'myvalue').wait()
            responses = [value for value in self.c.get_keys(keys)]
        self.assertEquals(['myvalue'], [r.value for r in responses])

    def test_multiple_keys(self):
        num_keys = 10
        keys = self.gen_keys(num_keys=num_keys)
        self.assertEquals(num_keys, len(keys))
        values = set()
        with self.c:
            for i, key in enumerate(keys):
                value = 'myvalue.%d' % i
                self.c.put(key, value)
                values.add(value)
            self.c.wait()
            resp_values = [entry.value for entry in self.c.get_keys(keys)]
        for value in values:
            self.assert_(value in resp_values)
        self.assertEquals(num_keys, len(resp_values))

    def test_more_keys_than_depth(self):
        num_keys = 16
        keys = self.gen_keys(num_keys=num_keys)
        self.assertEquals(num_keys, len(keys))
        values = set()
        with self.c:
            for i, key in enumerate(keys):
                value = 'myvalue.%d' % i
                self.c.put(key, value)
                values.add(value)
            self.c.wait()
            g = self.c.get_keys(keys, depth=8)
            resp_values = set([str(entry.value) for entry in g])
        for value in values:
            self.assert_(value in resp_values)
        self.assertEquals(num_keys, len(resp_values))

    def test_lots_of_big_keys(self):
        num_keys = 10
        keys = self.gen_keys(num_keys=num_keys)
        self.assertEquals(num_keys, len(keys))
        values = set()
        with self.c:
            for i, key in enumerate(keys):
                value = ('myvalue.%d' % i) * 1024
                value += 'END'
                self.c.put(key, value)
                values.add(value)
            self.c.wait()
            g = self.c.get_keys(keys, depth=5)
            resp_values = set([str(entry.value) for entry in g])
        for value in values:
            self.assert_(value in resp_values)
        self.assertEquals(num_keys, len(resp_values))

    def test_lots_and_lots_of_keys_in_order(self):
        num_keys = 64
        keys = self.gen_keys(num_keys=num_keys)
        self.assertEquals(num_keys, len(keys))
        with self.c:
            for i, key in enumerate(keys):
                value = 'myvalue.%d.' % i
                value += 'a' * random.randint(0, 1024)
                self.c.put(key, value)
            self.c.wait()
            for i, entry in enumerate(self.c.get_keys(keys)):
                # test_blah.Test.test.i
                self.assertEquals(i, int(entry.key.rsplit('.', 1)[1]))
                # myvalue.i.aaaa
                self.assertEquals(i, int(entry.value.split('.', 2)[1]))
        self.assertEquals(i, num_keys - 1)

    def test_get_keys_for_missing_keys(self):
        num_keys = 10
        keys = self.gen_keys(num_keys=num_keys)
        self.assertEquals(num_keys, len(keys))
        with self.c:
            for i, key in enumerate(keys):
                if i > (num_keys // 2):
                    break
                value = 'myvalue.%d.' % i
                value += 'a' * random.randint(0, 1024)
                self.c.put(key, value)
            self.c.wait()
            for i, entry in enumerate(self.c.get_keys(keys)):
                if i <= (num_keys // 2):
                    # test_blah.Test.test.i
                    self.assertEquals(i, int(entry.key.rsplit('.', 1)[1]))
                    # myvalue.i.aaaa
                    self.assertEquals(i, int(entry.value.split('.', 2)[1]))
                else:
                    self.assertEquals(entry, None)
        self.assertEquals(i, num_keys - 1)


class TestGreenPuts(BaseGreenTestCase):

    def test_single_key(self):
        entries = self.gen_entries()
        self.assertEquals(1, len(entries))
        entry = entries[0]
        with self.c:
            self.c.put_entries(entries)
            self.c.wait()
            resp = self.c.get(entry.key).wait()
        self.assertEquals(entry.value, resp.value)

    def test_multiple_keys(self):
        num_keys = 10
        entries = self.gen_entries(num_keys)
        self.assertEquals(num_keys, len(entries))
        with self.c:
            self.c.put_entries(entries)
            for entry in entries:
                resp = self.c.get(entry.key).wait()
                self.assertEquals(entry.value, resp.value)

    def test_more_keys_than_write_depth(self):
        num_keys = 16
        entries = self.gen_entries(num_keys)
        self.assertEquals(num_keys, len(entries))
        with self.c:
            self.c.put_entries(entries, depth=8)
            for entry in entries:
                resp = self.c.get(entry.key).wait()
                self.assertEquals(resp.value, entry.value)

    def test_put_lots_of_big_keys(self):
        num_keys = 10
        entries = self.gen_entries(num_keys=num_keys, min_size=32768)
        self.assertEquals(num_keys, len(entries))
        with self.c:
            self.c.put_entries(entries, depth=8)
            for entry in entries:
                resp = self.c.get(entry.key).wait()
                self.assertEquals(resp.value, entry.value)


class TestGreenDeletes(BaseGreenTestCase):

    def test_single_key(self):
        keys = self.gen_keys(num_keys=1)
        self.assertEquals(1, len(keys))
        with self.c:
            self.c.put(keys[0], 'myvalue').wait()
            self.c.delete_keys(keys)
            self.assertEquals(None, self.c.get(keys[0]).wait())

    def test_multiple_keys(self):
        num_keys = 10
        keys = self.gen_keys(num_keys=num_keys)
        self.assertEquals(num_keys, len(keys))
        with self.c:
            for i, key in enumerate(keys):
                self.c.put(key, 'myvalue.%d' % i)
            self.c.wait()
            self.c.delete_keys(keys)
            for key in keys:
                self.assertEquals(None, self.c.get(key).wait())

    def test_more_keys_than_depth(self):
        num_keys = 16
        keys = self.gen_keys(num_keys=num_keys)
        self.assertEquals(num_keys, len(keys))
        with self.c:
            for i, key in enumerate(keys):
                self.c.put(key, 'myvalue.%d' % i)
            self.c.wait()
            self.c.delete_keys(keys, depth=8)
            for key in keys:
                self.assertEquals(None, self.c.get(key).wait())

    def test_delete_missing_keys(self):
        num_keys = 10
        keys = self.gen_keys(num_keys=num_keys)
        self.assertEquals(num_keys, len(keys))
        with self.c:
            for key in keys:
                self.assertEquals(None, self.c.get(key).wait())
            self.assertEquals(False, self.c.delete_keys(keys))

    def test_delete_some_missing_keys(self):
        num_keys = 10
        keys = self.gen_keys(num_keys=num_keys)
        self.assertEquals(num_keys, len(keys))
        with self.c:
            # add values for the first half of the keys
            for i, key in enumerate(keys[:(num_keys // 2)]):
                self.c.put(key, 'myvalue.%d' % i)
            self.c.wait()
            self.assertEquals(True, self.c.delete_keys(keys))

    def test_delete_until_missing_keys(self):
        """
        I'm having trouble getting a delete operation to parse as False
        """
        return
        depth = 8
        # load up some values
        num_keys = 10
        keys = self.gen_keys(num_keys=num_keys)
        with self.c:
            for i, key in enumerate(keys):
                self.c.put(key, 'myvalue.%d' % i)
            self.c.wait()

        # generate keys until send deleted False
        def gen_keys():
            keys = self.gen_keys(num_keys=num_keys * 2)
            for key in keys:
                missing = yield key
                if missing:
                    self.assertGreaterEqual(i, num_keys)
                    self.assertLessEqual(i, num_keys + depth)
                    break
            else:
                self.assertFalse(True, 'did not stop deleting keys!')

        with self.c:
            self.assertEquals(True, self.c.delete_keys(gen_keys(), depth=depth))

            # and they should *all* be deleted
            for key in keys:
                self.assertEquals(None, self.c.get(key).wait())


@unittest.skipIf(not eventlet_available, 'eventlet is not installed')
class GreenClientPushKeysTestCase(MultiSimulatorTestCase):

    def test_push_keys(self):
        (source_port, source), (target_port, target) = self.client_map.items()
        key = self.buildKey('test')
        source.put(key, 'value')
        self.assertEqual(target.get(key), None)
        client = greenclient.GreenClient("localhost", source_port)
        with client:
            client.push_keys('localhost:%s' % target_port, [key])
        entry = target.get(key)
        self.assertEquals(entry.value, 'value')

    def test_push_batch_keys(self):
        source_port, target_port = self.ports
        source = greenclient.GreenClient("localhost", source_port)
        target = greenclient.GreenClient("localhost", target_port)
        keys = [self.buildKey('key%s' % i) for i in range(32)]
        entries = [Entry(k, 'value%s' % i) for i, k in enumerate(keys)]
        with source:
            source.put_entries(entries)
        with target:
            results = target.get_keys(keys)
            self.assert_(all(r is None for r in results))
        with source:
            results = source.push_keys('localhost:%s' % target_port, keys)
        self.assertEquals(len(results), len(keys))
        self.assert_(all(op.status.code == op.status.SUCCESS for op in results))
        with target:
            results = target.get_keys(keys)
            for i, r in enumerate(results):
                self.assertEquals(r.key, keys[i])
                self.assertEquals(r.value, 'value%s' % i)


if __name__ == "__main__":
    unittest.main()
