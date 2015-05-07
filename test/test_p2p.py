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

import unittest

from kinetic import operations
from kinetic import baseclient

from base import MultiSimulatorTestCase

class P2PTestCase(MultiSimulatorTestCase):

    def test_p2p_push(self):
        source, target = self.client_map.values()
        key = self.buildKey('test')
        source.put(key, 'value')
        self.assertEqual(target.get(key), None)
        resp = source.push([key],target.hostname, target.port)
        for op in resp:
            self.assertEquals(op.key, key)
            self.assertEquals(op.status.code, op.status.SUCCESS)
        entry = target.get(key)
        self.assertEquals(entry.value, 'value')


if __name__ == '__main__':
    unittest.main()
