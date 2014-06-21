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

#@author: Robert Cope

import unittest

from kinetic import Client
from kinetic import AdminClient
from kinetic import KineticMessageException
from base import BaseTestCase
from kinetic import common

class AdminClientTestCase(BaseTestCase):

    def setUp(self):
        super(AdminClientTestCase, self).setUp()
        self.adminClient = AdminClient(self.host, self.port)
        self.adminClient.connect()

    def tearDown(self):
        self.adminClient.close()

    def test_setSecurity(self):

        self.client.put(self.buildKey(1), "test_value_1")

        acl = common.ACL(identity=100)
        domain = common.Domain(roles=[common.Roles.READ])
        acl.domains = [domain]

        self.adminClient.setSecurity([acl])

        # Verify user 100 can only read
        read_only_client = Client(self.host, self.port, identity=100)
        read_only_client.get(self.buildKey(1))  # Should be OK.
        args = (self.buildKey(2), 'test_value_2')
        self.assertRaises(KineticMessageException, read_only_client.put, *args)
