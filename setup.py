from setuptools import setup, find_packages

with open('README.rst') as file:
    long_description = file.read()

setup(
    name = "simpledbf",
    version = "0.1.0",

    description = "A simple DBF file converter for Python3",
    url = "https://github.com/rnelsonchem/simpledbf",
    long_description = long_description,

    author = "Ryan Nelson",
    author_email = "rnelsonchem@gmail.com",

    license = "BSD",

    keywords = "DBF CSV Pandas SQLalchemy PyTables SQL HDF",

    packages = find_packages(),
    install_requires = [
        'pandas>=0.15.2',
        'tables>=3.1.1',
        'sqlalchemy>=0.9',
    ],

)

