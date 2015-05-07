from kinetic import Client
c = Client('localhost', 8123)
c.connect()
c.put('message','hello world')
print c.get('message').value
