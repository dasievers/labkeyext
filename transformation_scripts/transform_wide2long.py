#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
transformation script for converting to long-format
"""

# add to $PYTHONPATH here if necessary to access other modules
#import sys
#sys.path.append('...')

from labkeyext import ScriptReader
import pandas as pd
import numpy as np
from pandas.api.types import is_numeric_dtype


# =============================================================================
# 
# =============================================================================
sr = ScriptReader("${runInfo}", raw=True)
sr.read()
data = sr.data

#with open(sr.filePathRunPropertiesOut, 'a') as f:
#    f.write(os.getcwd())


# import analyte table
try:
    atablePath = '\\\\path\\analytes.tsv'
    atable = pd.read_csv(atablePath, sep='\t')
    analytes = set(atable['analyte'].to_list())
    units_preset = atable.set_index('analyte')['unit'].to_dict()
except FileNotFoundError:
    analytes = False
    units_preset = False

# detect whether a second header is present for units and process
units = {}
u = data.iloc[0].values
b = []
for v in u:
    if is_numeric_dtype(v) and not np.isnan(v):
        b.append(True)
    else:
        b.append(False)
b = np.array(b)
units_read = {}
if any(b):
    # second row at least one numeric type and treated as data
    units = units_preset
else:
    # second row ONLY has non-numeric data and treated as a unit specifier
    c = data.columns
    for i, v in enumerate(u):
        if type(v) is not str:
            u[i] = ''  # strike out non-strings
    units = dict(zip(c,u))
    # remove units row
    data = data.drop(index=0).reset_index(drop=True)


# =============================================================================
# 
# =============================================================================
# Identify columns used as id_vars.
# These are columns that are preserved during pivot
# and are also defined by the Assay design.
idcols = ['sample_id',
          'parent_id',
          'Date']

# columns required for entry
reqdcols = {'sample_id'}

# analyte columns
pivotcols = set(data.columns) - set(idcols)

# enforce required columns
for c in reqdcols:
    if c not in data.columns:
        raise Exception("{} is a required column, but missing".format(c))
    
# enforce analyte names from master table
if analytes:
    illegals = pivotcols - analytes
    if len(illegals) > 0:
        # illegal names present, report error
        raise Exception("{} are not valid analyte names".format(illegals))

# fill in missing optional columns with blanks (necessary to be present for melt based on idcols)
optcols = set(idcols) - reqdcols
for c in optcols:
    if c not in data.columns:
        data[c] = ""

# convert table to long format
datalong = pd.melt(data, id_vars=idcols).sort_values('sample_id').reset_index(drop=True)
datalong.rename(columns={'variable':'analyte'}, inplace=True)
# add units
if units:
    datalong['unit'] = [units[a] for a in datalong['analyte']]


# save results
datalong.to_csv(sr.filePathOut, sep='\t', index=False)

