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

import logging
from kinetic import baseclient
from kinetic import operations

class AdminClient(baseclient.BaseClient):

    def __init__(self, *args, **kwargs):
        if 'socket_timeout' not in kwargs:
            kwargs['socket_timeout'] = 60.0
        super(AdminClient, self).__init__(*args, **kwargs)


    # TODO(Nacho): this code is duplicated with client... not sure if its worth refactoring
    # it's pretty generic, maybe we can move it to the baseclient or something
    def _process(self, op, *args, **kwargs):
        header, value = op.build(*args, **kwargs)
        try:
            r = None
            with self:
                # update header
                self.update_header(header)
                # send message synchronously
                _, cmd, v = self.send(header, value)
            operations._check_status(cmd)
            return op.parse(cmd,v)
        except Exception as e:
            return op.onError(e)


    def getLog(self, *args, **kwargs):
        return self._process(operations.GetLog, *args, **kwargs)

    def setClusterVersion(self, *args, **kwargs):
        return self._process(operations.SetClusterVersion, *args, **kwargs)

    def updateFirmware(self, *args, **kwargs):
        return self._process(operations.UpdateFirmware, *args, **kwargs)

    def unlock(self, *args, **kwargs):
        return self._process(operations.UnlockDevice, *args, **kwargs)

    def lock(self, *args, **kwargs):
        return self._process(operations.LockDevice, *args, **kwargs)

    def erase(self, *args, **kwargs):
        return self._process(operations.EraseDevice, *args, **kwargs)

    def instantSecureErase(self, *args, **kwargs):
        return self._process(operations.SecureEraseDevice, *args, **kwargs)

    def setSecurity(self, *args, **kwargs):
        """
            Set the access control lists to lock users out of different permissions.
            Arguments: aclList -> A list of ACL (Access Control List) objects.
        """
        return self._process(operations.Security, *args, **kwargs)

