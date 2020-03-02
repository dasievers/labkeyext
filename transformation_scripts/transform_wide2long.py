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


# =============================================================================
# 
# =============================================================================
sr = ScriptReader("${runInfo}", raw=True)
sr.read()
data = sr.data

#with open(sr.filePathRunPropertiesOut, 'a') as f:
#    f.write(os.getcwd())


#TODO enforce naming conventions for analytes


# convert to long format
idcols = ['sample_id',
          'parent_id',
          'Date',
          'unit']

# enforce required columns
reqdcols = {'sample_id'}
for c in reqdcols:
    assert c in data.columns

# fill in missing optional columns with blanks (necessary for melt)
optcols = set(idcols) - reqdcols
for c in optcols:
    if c not in data.columns:
        data[c] = ""

datalong = pd.melt(data, id_vars=idcols).sort_values('sample_id').reset_index(drop=True)
datalong.rename(columns={'variable':'analyte'}, inplace=True)

# save results
datalong.to_csv(sr.filePathOut, sep='\t', index=False)

