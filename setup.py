from setuptools import setup, find_packages

with open('requirements.txt', 'r') as f:
    requires = [x.strip() for x in f if x.strip()]

version = '0.7.1'

setup(
    name='kinetic',
    version=version,
    url='https://github.com/Seagate/kinetic-py',
    description="Python client for Kinetic drives",
    author='Ignacio Corderi',
    packages=find_packages(exclude=['test']),
    install_requires=requires,
    entry_points = {
        'console_scripts': [
            'kineticc = kinetic.cmd:main',
            'kcmd = kinetic.cmd:main',
        ],
    },
)
