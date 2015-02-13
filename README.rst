simpledbf
#########

A Python3 utility for converting simple DBF files (see `Limitations`_) to CSV
files, Pandas DataFrames, SQL tables, or HDF5 tables. (There is almost
complete `Python2 support`_ as well.) This code was designed to be very
simple, fast and memory efficient; therefore, it lacks many features (such as
writing DBF files) that other packages might provide. The conversion to CSV
and SQL is entirely written in Python, so no additional dependencies are
necessary. For other export formats, see `Requirements`_. 

Bug fixes, questions, and update requests are encouraged and can be filed at
the `GitHub repo`_. 

This code is derived from an  `ActiveState DBF example`_ that works with
Python2 and is distributed under a PSF license.

.. _ActiveState DBF example: http://code.activestate.com/recipes/
        362715-dbf-reader-and-writer/
.. _GitHub repo: https://github.com/rnelsonchem/simpledbf


.. _Limitations:

DBF File Limitations
--------------------

This package currently supports a subset of `dBase III through 5`_ DBF files.
In particular, support is missing for linked memo files (DBT files). This is
mostly due to a limitation in of the types of files available to the author.
Feel free to request an update if you can supply a DBF file with an associated
DBT file. `DBF version 7`_, the most recent DBF file spec, is not currently
supported by this package.

.. _dBase III through 5: http://ulisse.elettra.trieste.it/services/doc/
        dbase/DBFstruct.htm
.. _DBF version 7: http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm

.. _Python2 support:

Python 2 Support 
----------------

Except for HDF file export, this code should work fine with Python2. HDF files
created using simpledbf in Python3 are compatible with all Python2 HDF
packages, so in principle, you could make any HDF files in a temporary Python3
environment. If you are using the `Anaconda Python distribution`_
(recommended), then you can make a small Python3 working environment as
follows:

.. code::

    $ conda create -n dbf python=3 pip pandas pytables sqlalchemy
    # Lots of output...
    
    $ source activate dbf

    dbf>$ pip install simpledbf

    dbf>$ python my_py3_hdf_creation_script.py
    # This is using Python3

    dbf>$ source deactivate

    $ python my_py2_stuff_with_hdf.py
    # This is using Python2 again

The HDF file export is currently broken in Python2 due to a `limitation in
Pandas HDF export with unicode`_. This issue may be fixed future versions of
Pandas. 

.. _Anaconda Python distribution: http://continuum.io/downloads
.. _limitation in Pandas HDF export with unicode: http://pandas.pydata.org/
        pandas-docs/stable/io.html#datatypes

.. _Requirements:

Requirements
------------

This module was tested with the following package versions. Python is the only
requirement if you only want to export to a CSV file or to an SQL table via a
CSV output (see ``to_textsql`` below).

* Python >=3.4, >=2.7.9 (no HDF export with Py2)

* Pandas >= 0.15.2 (Required for DataFrame)

* PyTables >= 3.1 (with Pandas required for HDF tables)

* SQLalchemy >= 0.9 (with Pandas required for DataFrame-SQL tables)

Installation
------------

It's probably easiest to install this package using ``pip``::

    $ pip install simpledbf

Or from GitHub::

    $ pip install git+https://github.com/rnelsonchem/simpledbf.git

However, this package is currently a single file, so you can download the
``simpledbf.py`` file from Github and put it in any folder of your choosing.

Example Usage
#############

.. _Loading:

Load a DBF file
---------------

This module currently only defines a single class, ``Dbf5``, which is
instantiated with a DBF file name (can contain path info). An optional 'codec'
keyword argument that controls the codec for reading/writing files. The
default is 'utf-8'. See the documentation for Python's `codec standard library
module`_ for more codec options.

.. code::

    In : from simpledbf import Dbf5

    In : dbf = Dbf5('fake_file_name.dbf', codec='utf-8')

The ``Dbf5`` object will initially only read the header information from the
file, so you can inspect some of the properties. For example, ``numrec`` is
the number of records in the DBF file, and ``fields`` is a list of tuples with
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

The ``mem`` method gives an approximate memory requirement for processing this
DBF file. (~2x the total file size, which could be wildly inaccurate.) In
addition, all of the output methods in this object take a ``chunksize``
keyword argument, which lets you split up the processing of large files into
smaller chunks to limit the total memory usage of the conversion process. When
this keyword argument is passed into ``mem``, the approximate memory footprint
of the chunk will also be given, which can be useful when trying to determine
the maximum chunksize your memory will allow.

.. code::

    In : dbf.mem()
    This total process would require more than 350.2 MB of RAM. 

    In : dbf.mem(chunksize=1000)
    Each chunk will require 4.793 MB of RAM.
    This total process would require more than 350.2 MB of RAM.

.. _codec standard library module: https://docs.python.org/3.4/library/
        codecs.html 

Export the Data
---------------

For all export methods, once the dbf file has been exported, the internal file
object will be exhausted, so you will not be able to re-export the data. This
is the same behavior as a standard file object. To re-export data, first
recreate a new ``Dbf5`` instance using the same file name, which is the
procedure followed in the documentation below.
    
Note on Empty/Bad Data
++++++++++++++++++++++

This package attempts to convert blank strings and poorly formatted values to
an empty value of your choosing (almost, see below). This is controlled by the
``na`` keyword argument to all export functions. The default for CSV is an
empty string (''), and for all other exports, it is 'nan' which is converted
to ``float('nan')``. *NOTE* The exception here is that float/int columns always use
``float('nan')`` for all missing values for DBF->SQL->DF conversion purposes.
Pandas has very powerful methods and algorithms for `working with missing
data`_, including converting NaN to other values (e.g.  empty strings). 

.. _working with missing data: http://pandas.pydata.org/pandas-docs/stable/
        missing_data.html
        
To CSV
++++++

To export the data to a CSV file, use the ``to_csv`` method, which takes the
name of a CSV file as an input. The default behavior is to append new data to
an existing file, so be careful if the file already exists. If ``chunksize``
is passed as a keyword argument, the file buffer will be flushed after
processing that many records. (May not be necessary.)  The ``na`` keyword
changes the value used for missing/bad entries (default is ''). The keyword
``header`` is a boolean that controls writing of the column names as the first
row of the CSV file. The encoding of the resulting CSV file is determined by
the codec that is set when opening the DBF file, see `Loading`_. 

.. code::

    In : dbf = Dbf5('fake_file_name.dbf')

    In : dbf.to_csv('junk.csv')

If you are unhappy with the default CSV output of this module, Pandas also has
very `powerful CSV export capabilities`_ for DataFrames.

.. _powerful CSV export capabilities: http://pandas.pydata.org/pandas-docs/
        stable/io.html#writing-to-csv-format

To SQL (CSV based)
++++++++++++++++++

Most SQL databases can import CSV files directly into an available table. The
pure-Python ``to_textsql`` method creates a SQL file containing the
appropriate table creation SQL and the SQL-variant command needed for loading
the file. In addition, the header-less CSV file is also created. (It is up to
you to load run the SQL file. See below.) This function takes two mandatory
arguments.  First, the name of of the SQL text file that you'd like to create,
and second, the name of the CSV file you'd like to create. In addition, there
are a number of optional keyword arguments as well. ``sqltype`` controls the
output dialect. The default is 'sqlite', but 'postgres' is also accepted.
``table`` can be used to set the name of the SQL table that will be created.
By default, this will be the name of the DBF file without the file extension.
You should escape quote characters (") in the CSV file. This is controlled
with the ``escapeqoute`` keyword, which defaults to ``'"'``. (This changes '"'
in text strings to '""', which the SQL server should ignore.) The
``chunksize``, ``na``, and ``header`` keywords are used to control the CSV
file. See above.

Here's an example for SQLite:

.. code::

    In : dbf = Dbf5('fake_file_name.dbf')

    In : dbf.to_textsql('junk.sql', 'junk.csv')

    # Exit Python
    $ sqlite3 junk.db < junk.sql

Here's an example for Postgresql:

.. code::

    In : dbf = Dbf5('fake_file_name.dbf')

    In : dbf.to_textsql('junk.sql', 'junk.csv', sqltype='postgres')

    # Exit Python
    $ psql -U username -f junk.sql db_name

To DataFrame 
++++++++++++

The ``to_dataframe`` method returns the DBF records as a Pandas DataFrame.
Obviously, this method requires that Pandas is installed. If the size of the
DBF file exceeds available memory, then passing the ``chunksize`` keyword
argument will return a generator function. This generator yields DataFrames of
len(<=chunksize) until all of the records have been processed. The ``na``
keyword changes the value used for missing/bad entries (default is 'nan' which
inserts ``float('nan')``).

.. code::

    In : dbf = Dbf5('fake_file_name.dbf')

    In : df = dbf.to_dataframe()
    # df is a DataFrame with all records

    In : dbf = Dbf5('fake_file_name.dbf')

    In : for df in dbf.to_dataframe(chunksize=10000)
    ....     do_cool_stuff(df)
    # Here a generator is returned

.. _chunksize issue:

Issue with DataFrame Chunksize
++++++++++++++++++++++++++++++

When a DataFrame is constructed, it attempts to determine the dtype of each
column. If you chunk the DataFrame output, it turns out that the dtype for a
column can change. For example, if one chunk has a column with all strings,
the dtype will be ``np.objec``; however, if that column is full of
``float('nan')`` in the next chunk, then the dtype will be ``float``. This has
some consequences for writing to SQL and HDF tables as well. In principle,
this could be changed, but it is currently non-trivial to set the dtypes for
DataFrame columns on construction. Please file a PR through GitHub if this is
a big problem.

To an SQL Table using Pandas
++++++++++++++++++++++++++++

The ``to_pandassql`` method will transfer the DBF entries to an SQL database
table of your choice using a combination of Pandas DataFrames and SQLalchemy.
A valid `SQLalchemy engine string`_ argument is required to connect with the
database. Database support will be limited to those supported by SQLalchemy.
(This has been tested with SQLite and Postgresql.) Note, if you are
transferring a large amount of data, this method will be very slow. If you
have direct access to the SQL server, you might want to use the text-based SQL
export instead.

.. code::

    In : dbf = Dbf5('fake_file_name.dbf')

    In : dbf = dbf.to_pandassql('sqlite:///foo.db')

This method takes three optional arguments. ``table`` is the name of the table
you'd like to use. If this is not passed, your new table will have the same
name as the DBF file without file extension. Again, the default here is to
append to an existing table. If you want to start fresh, delete the existing
table before using this function. The ``chunksize`` keyword processes the DBF
file in chunks of records no larger than this size. The ``na`` keyword changes
the value used for missing/bad entries (default is 'nan' which inserts
``float('nan')``).

.. code::

    In : dbf = Dbf5('fake_file_name.dbf')

    In : dbf = dbf.to_pandassql('sqlite:///foo.db', table="fake_tbl",
    ....                        chunksize=100000)
    
.. _SQLalchemy engine string: http://docs.sqlalchemy.org/en/rel_0_9/core/
        engines.html

To an HDF5 Table
++++++++++++++++

The ``to_pandashdf`` method will transfer the DBF entries to an HDF5 table of
your choice. This method uses a combination of Pandas DataFrames and PyTables,
so both of these packages must be installed. This method requires a file name
string for the HDF file you'd like to use. This file will be created if it
does not exist.  Again, the default is to append to an existing file of that
name, so be careful here. The HDF file will be created using the highest level
of compression (9) with the 'blosc' compression lib. This saves an enormous
amount of disk space, with little degradation of performance. However, these
parameters can be set using the ``complib`` and ``complevel`` keyword
agruments, which are identical to the ones described in the `Pandas HDF
compression docs`_.

.. code::

    In : dbf = Dbf5('fake_file_name.dbf')

    In : dbf = dbf.to_pandashdf('fake.h5')

This method uses all of the same optional arguments, and corresponding
defaults, as ``to_pandassql`` (see above). However, the ``data_columns``
keyword argument is also available, which sets the columns that will be used
as data columns in the HDF table. Data columns can be used for advanced
searching and selection; however, there is some degredation of preformance for
large numbers of data columns. See the `Pandas data columns docs`_ for more
information on the behavior of this keyword.

.. code::

    In : dbf = Dbf5('fake_file_name.dbf')

    In : dbf = dbf.to_pandassql('fake.h5', table="fake_tbl", chunksize=100000)

See the `chunksize issue`_ for DataFrame export for information on a potential
problem you may encounter with chunksize.

.. _Pandas HDF compression docs: http://pandas.pydata.org/pandas-docs/stable/
        io.html#compression

.. _Pandas data columns docs: http://pandas.pydata.org/pandas-docs/stable/
        io.html#query-via-data-columns

Export all DBF Files to Same HDF File
+++++++++++++++++++++++++++++++++++++

Because HDF export use the original file name as the stored table name, it is
trivial to process a group of files into a single HDF file. Below is an
example for HDF export.

.. code:: 

    In : import os

    In : from simpledbf import Dbf5

    In : files = os.listdir('.')

    In : for f in files:
    ....     if f[-3:].lower() == 'dbf':
    ....         dbf = Dbf5(f)
    ....         dbf.to_pandashdf('all_data.h5')

The process is very similar for ``to_textsql`` or ``to_pandassql``. 
   


