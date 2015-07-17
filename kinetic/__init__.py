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

# Logging
import logging
logging.basicConfig()

# Protocol version
from common import local

protocol_version = local.protocolVersion

from pkg_resources import get_distribution, DistributionNotFound
import os.path

try:
    _dist = get_distribution('kinetic')
    if not __file__.startswith(os.path.join(_dist.location, 'kinetic')):
        # not installed, but there is another version that *is*
        raise DistributionNotFound
except DistributionNotFound:
    __version__ = 'Please install this project with setup.py'
else:
    __version__ = _dist.version

#utils
from utils import buildRange

# clients
from greenclient import Client
from secureclient import SecureClient
from threadedclient import ThreadedClient

# common
from common import KeyRange
from common import Entry
from common import Peer

# exceptions
from common import KineticMessageException

# backward compatibility alliases
AsyncClient = Client
from kinetic.deprecated.adminclient import AdminClient
from kinetic import greenclient as client
# Fake old asyncclient module 
class AsyncClientCompat(object):
    AsyncClient = Client   
asyncclient = AsyncClientCompat()