from kinetic import Client
import uuid

c1 = Client('localhost', 8123)
c1.connect()

key = str(uuid.uuid4())
value = str(uuid.uuid4())

print 'Writing { Key: %s, Value: %s } on first device' % (key, value)
c1.put(key,value)

print 'Copying key from first to second device'
c1.push([key], 'localhost', 8124)

c2 = Client('localhost', 8124)
c2.connect()
kv = c2.get(key)

print 'Read { Key: %s, Value: %s } from second device' % (kv.key, kv.value)
