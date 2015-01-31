simpledbf 0.2.0 Release Notes
#############################

Functionality stays the same, but a few implementation details have changed.
Tested with Python2, and everything except HDF export works fine.

Highlights
----------

* Empty strings are converted to Nan (ie `float('nan')`).
  
* Added try/except clauses to all other types, so poorly formatted values
  will be returned as NaN as well. This may not be the behavior that is
  expected, so be careful.

simpledbf 0.1.0 Release Notes
#############################

First release.

Highlights
----------

* Pure-Python3 read of DBF files

* Pure-Python3 write as CSV

* Convert to DataFrame (Pandas required)

* Convert to HDF5 table (Pandas and PyTables required)

* Convert to SQL table (Pandas and SQLalchemy required)
