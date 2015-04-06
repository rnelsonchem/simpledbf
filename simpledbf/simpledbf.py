import struct
import datetime
import os
import codecs

# Check for optional dependencies.
try:
    import pandas as pd
except:
    print("Pandas is not installed. No support for DataFrames, HDF, or SQL.")
else:
    try:
        import tables as tb
    except:
        print("PyTables is not installed. No support for HDF output.")
    try:
        import sqlalchemy as sql
    except:
        print("SQLalchemy is not installed. No support for SQL output.")

sqltypes = {
        'sqlite': {'str':'TEXT', 'float':'REAL', 'int': 'INTEGER', 
            'date':'TEXT', 'bool':'INTEGER', 
            'end': '.mode csv {table}\n.import {csvname} {table}',
            'start': 'CREATE TABLE {} (\n',
            'index': '"index" INTEGER PRIMARY KEY ASC',
            },
        'postgres': {'str': 'text', 'float': 'double precision', 
            'int':'bigint', 'date':'date', 'bool':'boolean',
            'end': '''\copy "{table}" from '{csvname}' delimiter ',' csv''',
            'start': 'CREATE TABLE "{}" (\n',
            'index': '"index" INTEGER PRIMARY KEY',
            },
        }

class DbfBase(object):
    '''
    Base class for DBF file processing objects.

    Do not instantiate this class. This provides some of the common functions
    for other subclasses.
    '''
    def _chunker(self, chunksize):
        '''Return a list of chunk ints from given chunksize.

        Parameters
        ----------
        chunksize : int
            The maximum chunk size 

        Returns
        -------
        list of ints
            A list of chunks necessary to break up a given file. These will
            all be equal to `chunksize`, except for the last value, which is
            the remainder (<= `chunksize).
        '''
        num = self.numrec//chunksize
        # Chunksize bigger than numrec
        if num == 0:
            return [self.numrec,]
        else:
            chunks = [chunksize,]*num
            remain = self.numrec%chunksize
            if remain != 0:
                chunks.append(remain) 
            return chunks

    def _na_set(self, na):
        '''Set the value used for missing/bad data.

        Parameters
        ----------
        na : various types accepted
            The value that will be used to replace missing or malformed
            entries. Right now this accepts pretty much anything, and that
            value will be used as a replacement. (May not do what you expect.)
            However, the strings 'na' or 'nan' (case insensitive) will insert
            float('nan'), the string 'none' (case insensitive) or will insert
            the Python object `None`.  Float/int columns are always
            float('nan') regardless of this setting.
        '''
        if na.lower() == 'none':
            self._na = None
        elif na.lower() in ('na', 'nan'):
            self._na = float('nan')
        else:
            self._na = na
        
    def mem(self, chunksize=None):
        '''Print the memory usage for processing the DBF File.

        Parameters
        ----------
        chunksize : int, optional
            The maximum chunk size that will be used to process this file.

        Notes
        -----
        This method will print the maximum amount of RAM that will be
        necessary to process and load the DBF file. (This is ~2x the file
        size.) However, if the optional chunksize is passed, this function
        will also print memory usage per chunk as well, which can be useful
        for efficiently chunking and processing a file.
        '''
        if chunksize: 
            if chunksize > self.numrec:
                print("Chunksize larger than number of recs.")
                print("Chunksize set to {:d}.".format(self.numrec))
            else:
                smallmem = 2.*(self.fmtsiz*chunksize/1024**2)
                chkout = "Each chunk will require {:.4g} MB of RAM."
                print(chkout.format(smallmem))
        memory = 2.*(self.fmtsiz*self.numrec/1024**2)
        out = "This total process would require more than {:.4g} MB of RAM."
        print(out.format(memory))      

    def to_csv(self, csvname, chunksize=None, na='', header=True):
        '''Write DBF file contents to a CSV file.

        Parameters
        ----------
        csvname : string
            The name of the CSV file that will be created. By default, the
            file will be opened in 'append' mode. This won't delete an already
            existing file, but it will add new data to the end. May not be
            what you want.

        chunksize : int, optional
            If this is set, the contents of the file buffer will be flushed
            after processing this many records. May be useful for very large
            files that exceed the available RAM.

        na : various types accepted, optional
            The value that will be used to replace missing or malformed
            entries. Right now this accepts pretty much anything, and that
            value will be used as a replacement. (May not do what you expect.)
            However, the strings 'na' or 'nan' (case insensitive) will insert
            float('nan'), the string 'none' (case insensitive) or will insert
            the Python object `None`. Default for CSV is an empty string ('');
            however, float/int columns are always float('nan').

        header : boolean, optional
            Write out a header line with the column names. Default is True. 
        '''
        self._na_set(na)
        # set index column; this is only True when used with to_textsql()
        self._idx = False
        csv = codecs.open(csvname, 'a', encoding=self._enc)
        if header:
            column_line = ','.join(self.columns)
            csv.write(column_line + '\n')

        # Build up a formatting string for output. 
        outs = []
        for field in self.fields:
            if field[0] == "DeletionFlag":
                # Add an index column placeholder
                if self._idx:
                    outs.append('{}')
                else:
                    continue
            # Wrap strings in quotes
            elif field[1] in 'CDL':
                outs.append('"{}"')
            elif field[1] in 'NF':
                outs.append('{}')
        # Make the outline unicode or it won't write out properly for UTF-8
        out_line = u','.join(outs) + '\n'
        
        count = 0
        for n, result in enumerate(self._get_recs()):
            if self._idx:
                out_string = out_line.format(n, *result)
            else:
                out_string = out_line.format(*result)
            
            csv.write(out_string)
            count += 1
            if count == chunksize:
                csv.flush()
                count = 0
        csv.close()

    def to_textsql(self, sqlname, csvname, sqltype='sqlite', table=None,
            chunksize=None, na='', header=False, escapequote='"'):
        '''Write a SQL input file along with a CSV File.

        This function generates a header-less CSV file along with an SQL input
        file. The SQL file creates the database table and imports the CSV
        data. This works sqlite and postgresql.

        Parameters
        ----------
        sqlname : str
            Name of the SQL text file that will be created.

        csvname : str
            Name of the CSV file to be generated. See `to_csv`.

        sqltype : str, optional
            SQL dialect to use for SQL file. Default is 'sqlite'. Also accepts
            'postgres' for Postgresql.

        table : str or None, optional
            Table name to generate. If None (default), the table name will be
            the name of the DBF input file without the file extension.
            Otherwise, the given string will be used.
        
        chunksize : int, option
            Number of chunks to process CSV creation. Defalut is None. See
            `to_csv`.

        na : various types accepted, optional
            Type to use for missing values. Default is ''. See `to_csv`.

        header : bool, optional
            Write header to the CSV output file. Default is False. Some SQL
            engines try to process a header line as data, which can be a
            problem.

        escapequote : str, optional
            Use this character to escape quotes (") in string columns. The
            default is `'"'`. For sqlite and postgresql, a double quote
            character in a text string is treated as a single quote. I.e. '""'
            is converted to '"'.
        '''
        # Create an index column
        self._idx = True
        # Set the quote escape
        self._esc = escapequote
        # Get a dictionary of type conversions for a particular sql dialect
        sqldict = sqltypes[sqltype]
        # Create table name if not given
        if not table:
            table = self.dbf[:-4] # strip trailing ".dbf"
        # Write the csv file
        self.to_csv(csvname, chunksize=chunksize, na=na, header=header)

        # Write the header for the table creation.
        sql = codecs.open(sqlname, 'w', encoding=self._enc)
        head = sqldict['start']
        sql.write(head.format(table))

        # Make an output string and container for all strings.
        out_str = '"{}" {}'
        outs = []
        for field in self.fields:
            name, typ, size = field
            # Skip the first field
            if name == "DeletionFlag":
                continue

            # Convert Python type to SQL type
            if name in self._dtypes:
                dtype = self._dtypes[name]
                outtype = sqldict[dtype]
            else: 
                # If the column does not have a type, probably all missing
                # Try out best to make it the correct type for self._na
                if typ == 'C':
                    outtype = sqldict['str']
                elif typ in 'NF':
                    outtype = sqldict['float']
                elif typ == 'L':
                    outtype = sqldict['bool']
                elif typ == 'D':
                    outtype = sqldict['date']
            outs.append(out_str.format(name, outtype))

        # Insert an index line
        if self._idx:
            outs.insert(0, sqldict['index'])

        # Write the column information
        sql.write(',\n'.join(outs))
        sql.write(');\n')
        # Write the dialect-specific table generation command
        sql.write(sqldict['end'].format(table=table, csvname=csvname))
        sql.close()

    def to_dataframe(self, chunksize=None, na='nan'):
        '''Return the DBF contents as a DataFrame.

        Parameters
        ----------
        chunksize : int, optional
            Maximum number of records to process at any given time. If 'None'
            (defalut), process all records.

        na : various types accepted, optional
            The value that will be used to replace missing or malformed
            entries. Right now this accepts pretty much anything, and that
            value will be used as a replacement. (May not do what you expect.)
            However, the strings 'na' or 'nan' (case insensitive) will insert
            float('nan'), the string 'none' (case insensitive) or will insert
            the Python object `None`. Default for DataFrame is NaN ('nan');
            however, float/int columns are always float('nan')

        Returns
        -------
        DataFrame (chunksize == None)
            The DBF file contents as a Pandas DataFrame

        Generator (chunksize != None)
            This generator returns DataFrames with the maximum number of
            records equal to chunksize. (May be less)

        Notes
        -----
        This method requires Pandas >= 0.15.2.
        '''
        self._na_set(na)
        if not chunksize:
            # _get_recs is a generator, convert to list for DataFrame
            results = list(self._get_recs())
            df = pd.DataFrame(results, columns=self.columns)
            del(results) # Free up the memory? If GC works properly
            return df
        else:
            # Return a generator function instead
            return self._df_chunks(chunksize)

    def _df_chunks(self, chunksize):
        '''A DataFrame chunk generator.

        See `to_dataframe`.
        '''
        chunks = self._chunker(chunksize)
        # Keep track of the index, otherwise every DataFrame will be indexed
        # starting at 0
        idx = 0
        for chunk in chunks:
            results = list(self._get_recs(chunk=chunk))
            num = len(results) # Avoids skipped records problem
            df = pd.DataFrame(results, columns=self.columns, 
                              index=range(idx, idx+num))
            idx += num
            del(results) 
            yield df
    
    def to_pandassql(self, engine_str, table=None, chunksize=None, na='nan'):
        '''Write DBF contents to an SQL database using Pandas.

        Parameters
        ----------
        engine_str : string
            A SQLalchemy engine initialization string. See the SQL engine
            dialect documentation for more information.

        table : string, optional
            The name of the table to create for the DBF records. If 'None'
            (default), the DBF contents will be saved into a table with the
            same name as the input file without the file extension.The default
            behavior appends new data to an existing table. Delete the table
            by hand before running this method if you don't want the old data.

        chunksize : int, optional
            Maximum number of records to process at any given time. If 'None'
            (default), process all records.

        na : various types accepted, optional
            The value that will be used to replace missing or malformed
            entries. Right now this accepts pretty much anything, and that
            value will be used as a replacement. (May not do what you expect.)
            However, the strings 'na' or 'nan' (case insensitive) will insert
            float('nan'), the string 'none' (case insensitive) or will insert
            the Python object `None`. Default for SQL table is NaN ('nan');
            however, float/int columns are always float('nan').

        Notes
        -----
        This method requires Pandas >= 0.15.2 and SQLalchemy >= 0.9.7.
        '''
        self._na_set(na)
        if not table:
            table = self.dbf[:-4] # strip trailing ".dbf"
        engine = sql.create_engine(engine_str)

        # Setup string types for proper length, otherwise Pandas assumes
        # "Text" types, which may not be as efficient
        dtype = {}
        for field in self.fields:
            if field[1] == 'C':
                # Right now, Pandas doesn't support string length
                # Should work fine for sqlite and postgresql
                dtype[field[0]] = sql.types.String#(field[2])
        
        # The default behavior is to append new data to existing tables.
        if not chunksize:
            df = self.to_dataframe()
            df.to_sql(table, engine, dtype=dtype, if_exists='append')
        else:
            for df in self.to_dataframe(chunksize=chunksize):
                df.to_sql(table, engine, dtype=dtype, if_exists='append')
        del(df)

        
    def to_pandashdf(self, h5name, table=None, chunksize=None, na='nan', 
            complevel=9, complib='blosc', data_columns=None):
        '''Write DBF contents to an HDF5 file using Pandas.

        Parameters
        ----------
        h5name : string
            The name of HDF file to use. By default, this file is opened in
            'append' mode so that any existing files will not be overwritten,
            but it may cause problems.

        table : string, optional
            The name of the table to create for the DBF records. If 'None'
            (default), the DBF contents will be saved into a table with the
            same name as the input file without the file extension.The default
            behavior appends new data to an existing table. Delete the table
            by hand before running this method if you don't want the old data.

        chunksize : int, optional
            Maximum number of records to process at any given time. If 'None'
            (default), process all records.

        na : various types accepted, optional
            The value that will be used to replace missing or malformed
            entries. Right now this accepts pretty much anything, and that
            value will be used as a replacement. (May not do what you expect.)
            However, the strings 'na' or 'nan' (case insensitive) will insert
            float('nan'), the string 'none' (case insensitive) or will insert
            the Python object `None`. Default for HDF table is NaN ('nan');
            however, float/int columns are always float('nan').

        complib/complevel : int/string
            These keyword arguments set the compression library and level for
            the HDF file. These arguments are identical to the one defined for
            Pandas HDFStore, so see the Pandas documentation on `HDFStore` for
            more information.

        data_columns : list of column names or True
            This is a list of column names that will be created as data
            columns in the HDF file. This allows for advanced searching on
            these columns. If `True` is passed all columns will be data
            columns. There is some performace/file size degredation using this
            method, so for large numbers of columns, it is not recomended. See
            the Pandas IO documentation for more information.

        Notes
        -----
        This method requires Pandas >= 0.15.2 and PyTables >= 3.1.1.

        The default here is to create a compressed HDF5 file using the 'blosc'
        compression library (compression level = 9). This shouldn't affect
        performance much, but it does save an enormous amount of disk space.
        '''
        self._na_set(na)
        if not table:
            table = self.dbf[:-4] # strip trailing ".dbf"

        h5 = pd.HDFStore(h5name, 'a', complevel=complevel, complib=complib)

        if not chunksize:
            df = self.to_dataframe()
            h5.append(table, df, data_columns=data_columns)
        else:
            # Find the maximum string column length This is necessary because
            # the appendable table can not change width if a new DF is added
            # with a longer string
            max_string_len = {}
            mx = 0
            for field in self.fields:
                if field[1] == "C" and field[2] > mx:
                    mx = field[2]
            if mx != 0:
                max_string_len = {'values':mx}

            for df in self.to_dataframe(chunksize=chunksize):
                h5.append(table, df, min_itemsize=max_string_len,
                        data_columns=data_columns)
                h5.flush(fsync=True)
        
        del(df)
        h5.close()

class Dbf5(DbfBase):
    '''
    DBF version 5 file processing object.

    This class defines the methods necessary for reading the header and
    records from a version 5 DBF file.  Much of this code is based on an
    `ActiveState DBF example`_, which only worked for Python2.

    .. ActiveState DBF example: http://code.activestate.com/recipes/
            362715-dbf-reader-and-writer/

    Parameters
    ----------

    dbf : string
        The name (with optional path) of the DBF file.

    codec : string, optional
        The codec to use when decoding text-based records. The default is
        'utf-8'. See Python's `codec` standard lib module for other options.

    Attributes
    ----------

    dbf : string
        The input file name.

    f : file object
        The opened DBF file object

    numrec : int
        The number of records contained in this file.
    
    lenheader : int
        The length of the file header in bytes.

    numfields : int
        The number of data columns.

    fields : list of tuples
        Column descriptions as a tuple: (Name, Type, # of bytes).

    columns : list
        The names of the data columns.

    fmt : string
        The format string that is used to unpack each record from the file.

    fmtsiz : int
        The size of each record in bytes.
    '''
    def __init__(self, dbf, codec='utf-8'):
        self._enc = codec
        path, name = os.path.split(dbf)
        self.dbf = name
        # Escape quotes, set by indiviual runners
        self._esc = None
        # Reading as binary so bytes will always be returned
        self.f = open(dbf, 'rb')

        self.numrec, self.lenheader = struct.unpack('<xxxxLH22x', 
                self.f.read(32))    
        self.numfields = (self.lenheader - 33) // 32

        # The first field is always a one byte deletion flag
        fields = [('DeletionFlag', 'C', 1),]
        for fieldno in range(self.numfields):
            name, typ, size = struct.unpack('<11sc4xB15x', self.f.read(32))
            # eliminate NUL bytes from name string  
            name = name.strip(b'\x00')        
            fields.append((name.decode(self._enc), typ.decode(self._enc), size))
        self.fields = fields
        # Get the names only for DataFrame generation, skip delete flag
        self.columns = [f[0] for f in self.fields[1:]]
        
        terminator = self.f.read(1)
        assert terminator == b'\r'
     
        # Make a format string for extracting the data. In version 5 DBF, all
        # fields are some sort of structured string
        self.fmt = ''.join(['{:d}s'.format(fieldinfo[2]) for 
                            fieldinfo in self.fields])
        self.fmtsiz = struct.calcsize(self.fmt)

    def _get_recs(self, chunk=None):
        '''Generator that returns individual records.

        Parameters
        ----------
        chunk : int, optional
            Number of records to return as a single chunk. Default 'None',
            which uses all records.
        '''
        if chunk == None:
            chunk = self.numrec

        for i in range(chunk):
            # Extract a single record
            record = struct.unpack(self.fmt, self.f.read(self.fmtsiz))
            # If delete byte is not a space, record was deleted so skip
            if record[0] != b' ': 
                continue  
            
            # Save the column types for later
            self._dtypes = {}
            result = []
            for idx, value in enumerate(record):
                name, typ, size = self.fields[idx]
                if name == 'DeletionFlag':
                    continue

                # String (character) types, remove excess white space
                if typ == "C":
                    if name not in self._dtypes:
                        self._dtypes[name] = "str"
                    value = value.strip()
                    # Convert empty strings to NaN
                    if value == b'':
                        value = self._na
                    else:
                        value = value.decode(self._enc)
                        # Escape quoted characters
                        if self._esc:
                            value = value.replace('"', self._esc + '"')

                # Numeric type. Stored as string
                elif typ == "N":
                    # A decimal should indicate a float
                    if b'.' in value:
                        if name not in self._dtypes:
                            self._dtypes[name] = "float"
                        value = float(value)
                    # No decimal, probably an integer, but if that fails,
                    # probably NaN
                    else:
                        try:
                            value = int(value)
                            if name not in self._dtypes:
                                self._dtypes[name] = "int"
                        except:
                            # I changed this for SQL->Pandas conversion
                            # Otherwise floats were not showing up correctly
                            value = float('nan')

                # Date stores as string "YYYYMMDD", convert to datetime
                elif typ == 'D':
                    try:
                        y, m, d = int(value[:4]), int(value[4:6]), \
                                  int(value[6:8])
                        if name not in self._dtypes:
                            self._dtypes[name] = "date"
                    except:
                        value = self._na
                    else:
                        value = datetime.date(y, m, d)

                # Booleans can have multiple entry values
                elif typ == 'L':
                    if name not in self._dtypes:
                        self._dtypes[name] = "bool"
                    if value in b'TyTt':
                        value = True
                    elif value in b'NnFf':
                        value = False
                    # '?' indicates an empty value, convert this to NaN
                    else:
                        value = self._na

                # Floating points are also stored as strings.
                elif typ == 'F':
                    if name not in self._dtypes:
                        self._dtypes[name] = "float"
                    try:
                        value = float(value)
                    except:
                        value = float('nan')

                else:
                    err = 'Column type "{}" not yet supported.'
                    raise ValueError(err.format(value))

                result.append(value)
            yield result
    
