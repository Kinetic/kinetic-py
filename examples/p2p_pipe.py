from kinetic import Client
from kinetic import Peer
import uuid

c1 = Client('localhost', 8123)
c1.connect()

key = str(uuid.uuid4())
value = str(uuid.uuid4())

print 'Writing { Key: %s, Value: %s } on first device' % (key, value)
c1.put(key,value)

peers = [ Peer(port=8124), Peer(port=8126)]

print 'Copying key from first to second device and telling him to copy to third'
c1.pipedPush([key], peers)

# Verify second device
c2 = Client('localhost', 8124)
c2.connect()
kv = c2.get(key)

print 'Read { Key: %s, Value: %s } from second device' % (key, value)

# Verify third device
c3 = Client('localhost', 8126)
c3.connect()
kv = c3.get(key)

print 'Read { Key: %s, Value: %s } from third device' % (key, value)
