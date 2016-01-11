# **Kinetic-py**
[![Travis](https://img.shields.io/travis/Kinetic/kinetic-py.svg)](https://travis-ci.org/Kinetic/kinetic-py)
[![PyPI](https://img.shields.io/pypi/v/kinetic.svg)](https://pypi.python.org/pypi/kinetic/)
[![PyPI](https://img.shields.io/pypi/l/kinetic.svg)](https://github.com/Seagate/kinetic-py/blob/master/LICENSE/LGPL2.1.md)

Introduction
============
The [kinetic-protocol](https://github.com/Seagate/kinetic-protocol) python client.

## Requirements
- Requires Python 2.7.3 or higher. 
- Requires Python 2.7.9 on OSX to use SSL 

> **NOTE:** Python 3.x is not supported. 
 
Installing latest stable release
================================
    pip install kinetic


Installing from Source
======================

    git clone https://github.com/Seagate/kinetic-py.git
    cd kinetic-py
    python setup.py develop

> **NOTE:** for devices with old firmware code get version 0.7.3 of the libray
    git checkout 0.7.3

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

Getting Started with the basic client
=====================================

```python
from kinetic import Client
c = Client('localhost', 8123)
c.connect()
c.put('message','hello world')
print c.get('message').value
```
Should print out _hello world_

Troubleshooting during the installation
=======================================
On a brand new system, you might be missing a few things.
If you get an error saying setup tools not installed or missing.
Check the python [setuptools intallation guide](https://pypi.python.org/pypi/setuptools#installation-instructions).
If you needed to installed that, chances are you are missing some requirements to install and compile eventlet on your system.
On debian systems the quickest way is `sudo apt-get install python-eventlet`.


License
-------

This project is licensed under Mozilla Public License, v. 2.0
* [Original](LICENSE/Mozilla_Public_License_2.0.txt) version
