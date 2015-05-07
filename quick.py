import kinetic
from kinetic.deprecated import AdminClient

ac = AdminClient(use_ssl=True, port=8443)
ac.connect()

print 'This worked..'

c = kinetic.SecureClient()
c.connect()

print 'Yay!'
