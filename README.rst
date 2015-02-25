simpledbf
#########

*simpledbf* is a Python library for converting basic DBF files (see
`Limitations`_) to CSV files, Pandas DataFrames, SQL tables, or HDF5 tables.
This package is fully compatible with Python >=3.4, with almost complete
`Python 2.7 support`_ as well. The conversion to CSV and SQL (see
``to_textsql`` below) is entirely written in Python, so no additional
dependencies are necessary. For other export formats, see `Optional
Requirements`_.  This code was designed to be very simple, fast and memory
efficient for convenient interactive or batch file processing; therefore, it
lacks many features, such as the ability to write DBF files, that other
packages might provide. 

Bug fixes, questions, and update requests are encouraged and can be filed at
the `GitHub repo`_. 

This code is derived from an  `ActiveState DBF example`_ that works with
Python2 and is distributed under a PSF license.


.. _Optional Requirements:

Optional Requirements
---------------------

* Pandas >= 0.15.2 (Required for DataFrame)

* PyTables >= 3.1 (with Pandas required for HDF tables)

* SQLalchemy >= 0.9 (with Pandas required for DataFrame-SQL tables)

Installation
------------

The most recent release of *simpledbf* can be installed using ``pip`` or
``conda``, if you happen to be using the `Anaconda Python distribution`_.

Using ``conda``::

    $ conda install -c https://conda.binstar.org/rnelsonchem simpledbf

Using ``pip``::

    $ pip install simpledbf

The development version can be installed from GitHub::

    $ pip install git+https://github.com/rnelsonchem/simpledbf.git

As an alternative, this package only contains a single file, so in principle,
you could download the ``simpledbf.py`` file from Github and put it in any
folder of your choosing.


.. _Limitations:

DBF File Limitations
--------------------

This package currently supports a subset of `dBase III through 5`_ DBF files.
In particular, support is missing for linked memo (i.e. DBT) files. This is
mostly due to limitations in the types of files available to the author.  Feel
free to request an update if you can supply a DBF file with an associated memo
file. `DBF version 7`_, the most recent DBF file spec, is not currently
supported by this package.


.. _Python 2.7 support:

Python 2 Support 
----------------

Except for HDF file export, this code should work fine with Python >=2.7.
However, HDF files created in Python3 are compatible with all Python2 HDF
packages, so in principle, you could make any HDF files in a temporary Python3
environment. If you are using the `Anaconda Python distribution`_
(recommended), then you can make a small Python3 working environment as
follows:

.. code::

    $ conda create -n dbf python=3 pandas pytables sqlalchemy
    # Lots of output...
    
    $ source activate dbf

    dbf>$ conda install -c https://conda.binstar.org/rnelsonchem simpledbf

    dbf>$ python my_py3_hdf_creation_script.py
    # This is using Python3

    dbf>$ source deactivate

    $ python my_py2_stuff_with_hdf.py
    # This is using Python2 again

HDF file export is currently broken in Python2 due to a `limitation in Pandas
HDF export with unicode`_. This issue may be fixed future versions of
Pandas/PyTables.


Example Usage
#############

.. _Loading:

Load a DBF file
---------------

This module currently only defines a single class, ``Dbf5``, which is
instantiated with a DBF file name, which can contain path info as well. An
optional 'codec' keyword argument that controls the codec used for
reading/writing files. The default is 'utf-8'. See the documentation for
Python's `codec standard library module`_ for more codec options.

.. code::

    In : from simpledbf import Dbf5

    In : dbf = Dbf5('fake_file_name.dbf', codec='utf-8')

The ``Dbf5`` object initially only reads the header information from the file,
so you can inspect some of the properties. For example, ``numrec`` is the
number of records in the DBF file, and ``fields`` is a list of tuples with
information about the data columns. See the DBF file spec for info on the
column type characters. The "DeletionFlag" column is always present as a check
for deleted records; however, it is never exported during conversion.

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


Export the Data
---------------

The ``Ddb5`` object behaves like Python's file object in that it will be
"exhausted" after export. To re-export the DBF data to a different format,
first create a new ``Dbf5`` instance using the same file name. This procedure
is followed in the documentation below.

    
Note on Empty/Bad Data
++++++++++++++++++++++

This package attempts to convert most blank strings and poorly formatted
values to an empty value of your choosing. This is controlled by the ``na``
keyword argument to all export functions. The default for CSV is an empty
string (''), and for all other exports, it is 'nan' which converts empty/bad
values to ``float('nan')``. *NOTE* The exception here is that float/int
columns always use ``float('nan')`` for all missing values for
DBF->SQL->DataFrame conversion purposes. Pandas has very powerful functions
for `working with missing data`_, including converting NaN to other values
(e.g.  empty strings). 

        
To CSV
++++++

Use the ``to_csv`` method to export the data to a CSV file. This method
requires the name of a CSV file as an input. The default behavior is to append
new data to an existing file, so be careful if the file already exists. The
``chunksize`` keyword argument controls the frequency that  the file buffer
will be flushed, which may not be necessary. The ``na`` keyword changes the
value used for missing/bad entries (default is ''). The keyword ``header`` is
a boolean that controls writing of the column names as the first row of the
CSV file. The encoding of the resulting CSV file is determined by the codec
that is set when opening the DBF file, see `Loading`_. 

.. code::

    In : dbf = Dbf5('fake_file_name.dbf')

    In : dbf.to_csv('junk.csv')

If you are unhappy with the default CSV output of this module, Pandas also has
very `powerful CSV export capabilities`_ for DataFrames.


To SQL (CSV-based)
++++++++++++++++++

Most SQL databases can create tables directly from local CSV files. The
pure-Python ``to_textsql`` method creates two files: 1) a header-less CSV file
containing the DBF contents, and 2) a SQL file containing the appropriate
table creation and CSV import code. It is up to you to run the SQL file as a
separate step. This function takes two mandatory arguments, which are simply
the names of the SQL and CSV files, respectively. In addition, there are a
number of optional keyword arguments as well. ``sqltype`` controls the output
dialect. The default is 'sqlite', but 'postgres' is also accepted.  ``table``
sets the name of the SQL table that will be created. By default, this will be
the name of the DBF file without the file extension. You should escape quote
characters (") in the CSV file. This is controlled with the ``escapeqoute``
keyword, which defaults to ``'"'``. (This changes '"' in text strings to '""',
which the SQL server should ignore.) The ``chunksize``, ``na``, and ``header``
keywords are used to control the CSV file. See above.

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

The ``to_dataframe`` method returns the DBF records as a Pandas DataFrame.  If
the size of the DBF file exceeds available memory, then passing the
``chunksize`` keyword argument will return a generator function. This
generator yields DataFrames of len(<=chunksize) until all of the records have
been processed. The ``na`` keyword changes the value used for missing/bad
entries (default is 'nan' which inserts ``float('nan')``).

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
the dtype will be ``np.object``; however, if in the next chunk that same
column is full of ``float('nan')``, the resulting dtype will be set as
``float``. This has some consequences for writing to SQL and HDF tables as
well. In principle, this behavior could be changed, but it is currently
non-trivial to set the dtypes for DataFrame columns on construction. Please
file a PR through GitHub if this is a big problem.


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

    In : dbf.to_pandassql('sqlite:///foo.db')

This method accepts three optional arguments. ``table`` is the name of the
table you'd like to use. If this is not passed, your new table will have the
same name as the DBF file without file extension. Again, the default here is
to append to an existing table. If you want to start fresh, delete the
existing table before using this function. The ``chunksize`` keyword processes
the DBF file in chunks of records no larger than this size. The ``na`` keyword
changes the value used for missing/bad entries (default is 'nan' which inserts
``float('nan')``).

.. code::

    In : dbf = Dbf5('fake_file_name.dbf')

    In : dbf.to_pandassql('sqlite:///foo.db', table="fake_tbl",
    ....                    chunksize=100000)
    

To an HDF5 Table
++++++++++++++++

The ``to_pandashdf`` method transfers the DBF entries to an HDF5 table of your
choice. This method uses a combination of Pandas DataFrames and PyTables, so
both of these packages must be installed. This method requires a file name
string for the HDF file, which will be created if it does not exist.  Again,
the default behavior is to append to an existing file of that name, so be
careful here.  The HDF file will be created using the highest level of
compression (9) with the 'blosc' compression lib. This saves an enormous
amount of disk space, with little degradation of performance; however, this
compression library is non-standard, which can cause problems with other HDF
libraries. Compression options are controlled use the ``complib`` and
``complevel`` keyword arguments, which are identical to the ones described in
the `Pandas HDF compression docs`_.

.. code::

    In : dbf = Dbf5('fake_file_name.dbf')

    In : dbf.to_pandashdf('fake.h5')

This method uses the same optional arguments, and corresponding defaults, as
``to_pandassql`` (see above). A example with ``chunksize`` is shown below. In
addition, a ``data_columns`` keyword argument is also available, which sets
the columns that will be used as data columns in the HDF table. Data columns
can be used for advanced searching and selection; however, there is some
degredation of preformance for large numbers of data columns. See the `Pandas
data columns docs`_ for a more detailed explanation.

.. code::

    In : dbf = Dbf5('fake_file_name.dbf')

    In : dbf.to_pandashdf('fake.h5', table="fake_tbl", chunksize=100000)

See the `chunksize issue`_ for DataFrame export for information on a potential
problem you may encounter with chunksize.


Batch Export
++++++++++++

Batch file export is trivial using *simpledbf*. For example, the following
code processes all DBF files in the current directory into separate tables in
a single HDF file.

.. code:: 

    In : import os

    In : from simpledbf import Dbf5

    In : files = os.listdir('.')

    In : for f in files:
    ....     if f[-3:].lower() == 'dbf':
    ....         dbf = Dbf5(f)
    ....         dbf.to_pandashdf('all_data.h5')

   
.. External Hyperlinks

.. _ActiveState DBF example: http://code.activestate.com/recipes/
        362715-dbf-reader-and-writer/
.. _GitHub repo: https://github.com/rnelsonchem/simpledbf
.. _dBase III through 5: http://ulisse.elettra.trieste.it/services/doc/
        dbase/DBFstruct.htm
.. _DBF version 7: http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm
.. _Anaconda Python distribution: http://continuum.io/downloads
.. _limitation in Pandas HDF export with unicode: http://pandas.pydata.org/
        pandas-docs/stable/io.html#datatypes
.. _codec standard library module: https://docs.python.org/3.4/library/
        codecs.html 
.. _working with missing data: http://pandas.pydata.org/pandas-docs/stable/
        missing_data.html
.. _powerful CSV export capabilities: http://pandas.pydata.org/pandas-docs/
        stable/io.html#writing-to-csv-format
.. _SQLalchemy engine string: http://docs.sqlalchemy.org/en/rel_0_9/core/
        engines.html
.. _Pandas HDF compression docs: http://pandas.pydata.org/pandas-docs/stable/
        io.html#compression
.. _Pandas data columns docs: http://pandas.pydata.org/pandas-docs/stable/
        io.html#query-via-data-columns



