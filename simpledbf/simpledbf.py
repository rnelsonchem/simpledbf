import struct
import datetime
import os

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
            the Python object `None`.
        '''
        if na.lower == 'none':
            self._na = None
        elif na.lower in ('na', 'nan'):
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

    def to_csv(self, csvname, chunksize=None, na=''):
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
            the Python object `None`. Default for CSV is an empty string ('').
        '''
        self._na_set(na)
        csv = open(csvname, 'a')
        column_line = ','.join(self.columns)
        csv.write(column_line + '\n')

        # Build up a formatting string for output. 
        outs = []
        for field in self.fields:
            if field[0] == "DeletionFlag":
                continue
            # Wrap strings in quotes
            elif field[1] in 'CDL':
                outs.append('"{}"')
            elif field[1] in 'NF':
                outs.append('{}')
        out_line = ','.join(outs) + '\n'
        
        count = 0
        for result in self._get_recs():
            csv.write(out_line.format(*result))
            count += 1
            if count == chunksize:
                csv.flush()
                count = 0
        csv.close()

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
            the Python object `None`. Default for DataFrame is NaN ('nan').

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
            the Python object `None`. Default for SQL table is NaN ('nan').

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

    def to_pandashdf(self, h5name, table=None, chunksize=None, na='nan'):
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
            the Python object `None`. Default for HDF table is NaN ('nan').

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
        h5 = pd.HDFStore(h5name, 'a', complevel=9, complib='blosc')

        if not chunksize:
            df = self.to_dataframe()
            h5.append(table, df)
        else:
            for df in self.to_dataframe(chunksize=chunksize):
                h5.append(table, df)
                h5.flush(fsync=True)
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
    def __init__(self, dbf):
        path, name = os.path.split(dbf)
        self.dbf = name
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
            fields.append((name.decode(), typ.decode(), size))
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

            result = []
            for (name, typ, size), value in zip(self.fields, record):
                if name == 'DeletionFlag':
                    continue

                # String (character) types, remove excess white space
                if typ == "C":
                    value = value.decode().strip()
                    # Convert empty strings to NaN
                    if value == '':
                        value = self._na

                # Numeric type. Stored as string
                elif typ == "N":
                    # A decimal should indicate a float
                    if b'.' in value:
                        value = float(value)
                    # No decimal, probably an integer, but if that fails,
                    # probably NaN
                    else:
                        try:
                            value = int(value)
                        except:
                            value = self._na

                # Date stores as string "YYYYMMDD", convert to datetime
                elif typ == 'D':
                    try:
                        y, m, d = int(value[:4]), int(value[4:6]), \
                                  int(value[6:8])
                    except:
                        value = self._na
                    else:
                        value = datetime.date(y, m, d)

                # Booleans can have multiple entry values
                elif typ == 'L':
                    if value in b'TyTt':
                        value = True
                    elif value in b'NnFf':
                        value = False
                    # '?' indicates an empty value, convert this to NaN
                    else:
                        value = self._na

                # Floating points are also stored as strings.
                elif typ == 'F':
                    try:
                        value = float(value)
                    except:
                        value = self._na

                result.append(value)
            yield result
    
