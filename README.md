Introduction
============
This repository hosts the preliminary phase of the Kinetic Python Client source code. Improvements of this repo will be added in the near future to provide examples and documentation.

Initial Setup
=============

    git clone https://github.com/Seagate/kinetic-py.git
    git submodule init
    git submodule update

Running Tests
=============

    python test/

Installing from Source
======================

    sh compile_proto.sh
    python setup.py develop

Getting Started with Client API
========================

```python
from kinetic import Client
c = Client('localhost', 8123)
c.put('message','hello world')
print c.get('message').value
```
Should print out _hello_ _world_

License
-------

This project is licensed under GNU Lesser General Public License, version 2.1
* [Markdown](LICENSE/LGPL2.1.md) version
* [Original](LICENSE/LGPL2.1.txt) version
