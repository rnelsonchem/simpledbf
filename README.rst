simpledbf
#########

A Python3 compatible utility for converting `DBF version 5`_ files to CSV
files, Pandas DataFrames, SQL tables, or HDF5 tables. This code was designed
to be a very simple, fast and memory efficient conversion tool for legacy DBF
files. Therefore, it lacks many features (such as a DBF file writer) that
other packages might provide. `DBF version 7`_, the most recent
DBF file spec, is not currently supported by this package.

Bug fix and update requests can be filed at the `GitHub repo`_.

This code is derived from an  `ActiveState DBF example`_ that works with
Python2 and is distributed under a PSF license.

.. _DBF version 5: http://www.oocities.org/geoff_wass/dBASE/GaryWhite/
        dBASE/FAQ/qformt.htm
.. _ActiveState DBF example: http://code.activestate.com/recipes/
        362715-dbf-reader-and-writer/
.. _DBF version 7: http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm
.. _GitHub repo: https://github.com/rnelsonchem/simpledbf
        
Requirements
------------

This module was tested with the following Python and package versions. Python
is the only requirement if you only want to export to CSV. In that case,
comment out the other imports in the source code (only one file) to avoid
using the optional packages.

* Python >= 3.4

* Pandas >= 0.15.2

* PyTables >= 3.1

* SQLalchemy >= 0.9

Installation
------------

It's probably easiest to install this package using `pip`::

    $ pip install simpledbf

Or from GitHub::

    $ pip install git+https://github.com/rnelsonchem/simpledbf.git

Although this package is only one file, so you can just download it as is in
any folder of your choosing.

Example Usage
-------------

Load a DBF file
+++++++++++++++

This module currently only defines a single class, `Dbf5`, which is
instantiated with a DBF file name (can contain path info).

.. code::

    In : from simpledbf import Dbf5

    In : dbf = Dbf5('fake_file_name.dbf')

The `Dbf5` object will initially only read the header information from the
file, so you can inspect some of the properties. For example, `numrec` is the
number of records in the DBF file, and `fields` is a list of tuples with
information about the data columns. (See the DBF file spec for info on the
column type characters. The "DeletionFlag" column is not exported, but simply
checks if a record has been deleted.)

.. code::

    In : dbf.numrec
    Out: 10000

    In : dbf.fields
    Out: [('DeletionFlag', 'C', 1), ('col_1', 'C', 15), ('col_2', 'N', 2)]

The docstring for this object contains a complete listing of attributes and
their descriptions.

The `mem` method gives an approximate memory requirement for processing this
DBF file. (~2x the total file size.) In addition, all of the output methods in
this object take a `chunksize` keyword argument, which lets you split up the
processing of large files into smaller chunks, to limit the total memory usage
of the conversion process. When this keyword argument is passed into `mem`,
the approximate memory footprint of the chunk will also be given, which can be
useful when trying to determine the maximum chunksize your memory will allow.

.. code::

    In : dbf.mem()
    This total process would require more than 350.2 MB of RAM. 

    In : dbf.mem(chunksize=1000)
    Each chunk will require 4.793 MB of RAM.
    This total process would require more than 350.2 MB of RAM.

To CSV
++++++

To export the data to a CSV file, use the `to_csv` method, which takes the
name of a CSV file as an input. The default behavior is to append new data to
an existing file, so be careful. If `chunksize` is passed as a keyword
argument, the file buffer will be flushed after processing that many records.

.. code::

    In : dbf = Dbf5('fake_file_name.dbf')

    In : dbf.to_csv('junk.csv')

To DataFrame
++++++++++++ 
The `to_dataframe` method returns the DBF records as a Pandas DataFrame.
(Obviously, this method requires that Pandas is installed.) If the size of the
DBF file exceeds available memory, then passing the `chunksize` keyword
argument will return a generator function. This generator yields DataFrames of
len(<=chunksize) until all of the records have been processed.

.. code::

    In : dbf = Dbf5('fake_file_name.dbf')

    In : df = dbf.to_dataframe()
    # df is a DataFrame with all records

    In : dbf = Dbf5('fake_file_name.dbf')

    In : for df in dbf.to_dataframe(chunksize=10000)
    ....     do_cool_stuff(df)
    # Here a generator is returned

To an SQL Table
+++++++++++++++

The `to_pandassql` method will transfer the DBF entries to an SQL database
table of your choice. This method uses a combination of Pandas DataFrames and
SQLalchemy, so both of these packages must be installed. This method requires
an SQLalchemy engine string, which is used to initialize the database
connection. This will be limited to the SQL databases supported by SQLalchemy,
see the `SQLalchemy docs`_ for more info. (This has been tested with SQLite
and Postgresql.)

.. code::

    In : dbf = Dbf5('fake_file_name.dbf')

    In : dbf = dbf.to_pandassql('sqlite:///foo.db')

This method takes two optional arguments. `table` is the name of the table
you'd like to use. If this is not passed, your new table will have the same
name as the DBF file without file extension. Again, the default here is to
append to an existing table. If you want to start fresh, delete the existing
table before using this function. The `chunksize` keyword processes the DBF
file in chunks of records no larger than this size.

.. code::

    In : dbf = Dbf5('fake_file_name.dbf')

    In : dbf = dbf.to_pandassql('sqlite:///foo.db', table="fake_tbl",
    ....                        chunksize=100000)
    
.. _SQLalchemy docs: http://docs.sqlalchemy.org/en/rel_0_9/core/engines.html

To an HDF5 Table
++++++++++++++++

The `to_pandashdf` method will transfer the DBF entries to an HDF5 table of
your choice. This method uses a combination of Pandas DataFrames and PyTables,
so both of these packages must be installed. This method requires a file name
string for the HDF file you'd like to use. This file will be created if it
does not exist.  Again, the default is to append to an existing file of that
name, so be careful here. The HDF file will be created using the highest level
of compression (9) with the 'blosc' compression lib. This saves an enormous
amount of disk space, with little degradation of performance.

.. code::

    In : dbf = Dbf5('fake_file_name.dbf')

    In : dbf = dbf.to_pandashdf('fake.h5')

This method uses the same optional arguments, and corresponding defaults, as
`to_pandassql`. See above.

.. code::

    In : dbf = Dbf5('fake_file_name.dbf')

    In : dbf = dbf.to_pandassql('fake.h5', table="fake_tbl", chunksize=100000)


