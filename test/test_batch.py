# Copyright (C) 2015 Seagate Technology.
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

#@author: Paul Dardeau

import unittest

from kinetic import Client
from kinetic import Batch
from common import BatchAbortedExcpetion
from base import BaseTestCase


class BatchTestCase(BaseTestCase):

    def setUp(self):
        super(BatchTestCase, self).setUp()
        self.client = Client(self.host, self.port)
        self.client.connect()
        self.create_new_batch()

    def create_new_batch(self):
        self.batch = self.client.begin_batch()

    def test_batch_initial_state(self):
        self.assertFalse(self.batch.is_completed())
        self.assertEquals(self.batch.operation_count(), 0)

    def test_batch_operation_count(self):
        key1 = 'test_batch_operation_count_1'
        key2 = 'test_batch_operation_count_2'
        key3 = 'test_batch_operation_count_3'
        self.batch.put(key1, '')
        self.assertEquals(self.batch.operation_count(), 1)
        self.batch.put(key2, '')
        self.assertEquals(self.batch.operation_count(), 2)
        self.batch.delete(key3)
        self.assertEquals(self.batch.operation_count(), 3)

        self.batch.abort()

    def test_batch_is_completed(self):
        key1 = 'test_batch_is_completed_1'
        key2 = 'test_batch_is_completed_2'
        self.assertFalse(self.batch.is_completed())
        self.batch.put(key1, '')
        self.batch.delete(key2)
        self.assertFalse(self.batch.is_completed())
        self.batch.commit()
        self.assertTrue(self.batch.is_completed())

        # do it again, but abort this time
        self.create_new_batch()
        self.assertFalse(self.batch.is_completed())
        self.batch.put(key1, '')
        self.batch.delete(key2)
        self.assertFalse(self.batch.is_completed())
        self.batch.abort()
        self.assertTrue(self.batch.is_completed())

    def test_batch_abort(self):
        self.abort()  # abort with no operations in batch

        self.create_new_batch()
        key = 'key_should_not_exist'
        self.batch.put(key, '')
        self.abort()
        self.assertNone(self.client.get(key))

    def test_batch_commit(self):
        self.commit() # commit with no operations in batch

        self.create_new_batch()
        key = 'key_should_exist'
        self.batch.put(key, '')
        self.batch.commit()
        self.assertNotNone(self.client.get(key))

    def test_batch_delete_commit(self):
        # put an entry
        key = 'test_batch_delete_commit'
        self.client.put(key, '')

        self.batch.delete(key)
        self.batch.commit()
        self.assertNone(self.client.get(key))

    def test_batch_delete_abort(self):
        # put an entry
        key = 'test_batch_delete_abort'
        self.client.put(key, '')

        self.batch.delete(key)
        self.batch.abort()
        self.assertNotNone(self.client.get(key))

    def test_batch_put_commit(self):
        key = 'test_batch_put_commit'
        self.batch.put(key, '')
        self.batch.commit()
        self.assertNotNone(self.client.get(key))

    def test_batch_put_abort(self):
        key = 'test_batch_put_abort'
        self.batch.put(key, '')
        self.batch.abort()
        self.assertNone(self.client.get(key))

    def test_batch_multiple_put_commit(self):
        key1 = 'test_batch_multiple_put_commit_1'
        key2 = 'test_batch_multiple_put_commit_2'
        self.batch.put(key1, '')
        self.batch.put(key2, '')
        self.batch.commit()
        self.assertNotNone(self.client.get(key1))
        self.assertNotNone(self.client.get(key2))

    def test_batch_multiple_put_abort(self):
        key1 = 'test_batch_multiple_put_abort_1'
        key2 = 'test_batch_multiple_put_abort_2'
        self.batch.put(key1, '')
        self.batch.put(key2, '')
        self.batch.abort()
        self.assertNone(self.client.get(key1))
        self.assertNone(self.client.get(key2))

    def test_batch_multiple_delete_commit(self):
        key1 = 'test_batch_multiple_delete_commit_1'
        key2 = 'test_batch_multiple_delete_commit_2'
        self.client.put(key1, '')
        self.client.put(key2, '')

        self.batch.delete(key1)
        self.batch.delete(key2)
        self.batch.commit()
        self.assertNone(self.client.get(key1))
        self.assertNone(self.client.get(key2))

    def test_batch_multiple_delete_abort(self):
        key1 = 'test_batch_multiple_delete_abort_1'
        key2 = 'test_batch_multiple_delete_abort_2'
        self.client.put(key1, '')
        self.client.put(key2, '')

        self.batch.delete(key1)
        self.batch.delete(key2)
        self.batch.abort()
        self.assertNotNone(self.client.get(key1))
        self.assertNotNone(self.client.get(key2))

    def test_batch_mixed_commit(self):
        key1 = 'test_batch_mixed_commit_1'
        key2 = 'test_batch_mixed_commit_2'
        self.client.put(key1, '')

        self.batch.delete(key1)
        self.batch.put(key2, '')
        self.batch.commit()
        self.assertNone(self.client.get(key1))
        self.assertNotNone(self.client.get(key2))

    def test_batch_mixed_abort(self):
        key1 = 'test_batch_mixed_abort_1'
        key2 = 'test_batch_mixed_abort_2'
        self.client.put(key1, '')
        
        self.batch.delete(key1)
        self.batch.put(key2, '')
        self.batch.abort()
        self.assertNotNone(self.client.get(key1))
        self.assertNone(self.client.get(key2))

    def test_batch_reuse(self):
        key = 'test_batch_reuse'
        self.batch.put(key, '')
        self.batch.commit()

        self.assertRaises(common.BatchAbortedException, self.batch.delete(key))


if __name__ == '__main__':
    unittest.main()

