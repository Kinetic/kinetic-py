from setuptools import setup, find_packages

with open('requirements.txt', 'r') as f:
    requires = [x.strip() for x in f if x.strip()]

version = '0.8.3-dev'

setup(
    name='kinetic',
    version=version,
    url='https://github.com/Seagate/kinetic-py',
    description="Python client for Kinetic devices",
    author='Ignacio Corderi',
    packages=find_packages(exclude=['test']),
    install_requires=requires,
    entry_points = {
        'console_scripts': [
            'kineticc = kinetic.cmd:main'
        ],
    },
)
