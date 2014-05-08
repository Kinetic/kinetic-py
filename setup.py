from setuptools import setup, find_packages

version = '0.6.0.3'

setup(
    name='kinetic',
    version=version,
    url='https://github.com/Seagate/kinetic-py',
    description="Python client for Kinetic drives",
    author='Ignacio Corderi',
    packages=find_packages(exclude=['test']),
    install_requires=['protobuf',
                      'eventlet'],
    entry_points = {
        'console_scripts': [
            'kineticc = kinetic.cmd:main',
            'kcmd = kinetic.cmd:main',
        ],
    },
)
