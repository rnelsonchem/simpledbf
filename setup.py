from setuptools import setup, find_packages

with open('README.rst') as file:
    long_description = file.read()

setup(
    name = "simpledbf",
    version = "0.2.4",

    description = "Convert DBF files to CSV, DataFrames, HDF5 tables, and "\
            "SQL tables. Python3 compatible.",
    url = "https://github.com/rnelsonchem/simpledbf",
    long_description = long_description,

    author = "Ryan Nelson",
    author_email = "rnelsonchem@gmail.com",

    license = "BSD",
    classifiers = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers', 
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
    ],

    keywords = "DBF CSV Pandas SQLalchemy PyTables DataFrame SQL HDF",

    packages = find_packages(),

)

