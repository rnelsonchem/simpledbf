from setuptools import setup, find_packages

setup(
    name = "simpledbf",
    version = "0.1.0",

    description = "A simple DBF file converter for Python3",
    url = "https://github.com/rnelsonchem/simpledbf",

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

