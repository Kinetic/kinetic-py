Introduction
============
This repository hosts the preliminary phase of the Kinetic Python Client source code. Improvements of this repo will be added in the near future to provide examples and documentation.

Initial Setup
=============

    git clone https://github.com/Seagate/kinetic-py.git
    git submodule init
    git submodule update

Installing from Source
======================

    python setup.py develop

Running Tests
=============
The tests need a Kinetic device to run. You can use the simulator available at https://github.com/Seagate/kinetic-java.
To configure the test environment:

    export KINETIC_HOST=192.168.0.20
    export KINETIC_PORT=8123

Optionally you can point the tests to the simulator jar:

    export KINETIC_JAR=kinetic-simulator-<currentversion>-jar-with-dependencies.jar

Then to run the tests:

    python test/

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
