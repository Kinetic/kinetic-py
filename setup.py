from setuptools import setup, find_packages

with open('requirements.txt', 'r') as f:
    requires = [x.strip() for x in f if x.strip()]

version = '0.9.1'

setup(
    # overview    
    name = 'kinetic',
    description = "Python client for Kinetic devices",

    # technical info
    version = version,
    packages=find_packages(exclude=['test']),
    requires = requires,
    install_requires=requires,

    # features
    entry_points = {
        'console_scripts': [ 'kineticc = kinetic.cmd:main' ],
    },

    # copyright
    author='Ignacio Corderi',
    license='LGPLv2.1',

    # more info
    url = 'https://github.com/Seagate/kinetic-py',

    # categorization
    keywords = ('kinetic protocol api storage key/value seagate'),
    classifiers  = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 2 :: Only',
        'Topic :: Software Development :: Libraries :: Python Modules',
     ],
)
