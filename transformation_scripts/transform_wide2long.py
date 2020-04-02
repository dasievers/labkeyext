#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
transformation script for converting to long-format
"""

# add to $PYTHONPATH here if necessary to access other modules
#import sys
#sys.path.append('...')

import re
from labkeyext import ScriptReader
import pandas as pd
import numpy as np
from labkey.utils import create_server_context
from labkey.query import select_rows
from labkey.exceptions import RequestError, QueryNotFoundError, ServerContextError, ServerNotFoundError


# =============================================================================
# 
# =============================================================================
sr = ScriptReader("${runInfo}", raw=True)
sr.read()
data = sr.data

# import analyte table
try:
    # query server list
    server_url = "${baseServerURL}"
    # extract just the foo.bar.bat
    server = re.search("[A-Za-z]+\.[A-Za-z]+\.[A-Za-z]+", server_url)[0]
    #TODO: automate credentials
#    sessionID = "${httpSessionId}"
#    apikey = 'session|'+sessionID
    apikey = 'apikey|............'
    """
    On your server, go to the specific table you're interested in
    and then click <export>, select the <Script> tab, then select
    the <Python> radio button and <Create Script>. Copy the
    schema_name to 'schema' and query_name to 'table' below.
    The 'folder' path is automatically updated here, though
    if needed, replace below with the second arg in the 
    labkey.utils.create_server_context() call from the created script text.
    """    
    folder = "${containerPath}"
    schema = 'lists'
    table = 'Analytes'
    server_context = create_server_context(server, 
                                           folder, 
                                           context_path='labkey',
                                            api_key=apikey)
    result = select_rows(server_context=server_context, 
                         schema_name=schema, 
                         query_name=table, 
                         timeout=10)
    atable = pd.DataFrame(result['rows'])
    analytes = set(atable['analyte'])
    units_preset = atable.set_index('analyte')['unit'].to_dict()

    """
    Optionally include this portion if reading a file on the
    server instead of performing an API query.
    """
#    #query for local file
#    atablePath = '...analytes.tsv'
#    atable = pd.read_csv(atablePath, sep='\t')
#    analytes = set(atable['analyte'].to_list())
#    units_preset = atable.set_index('analyte')['unit'].to_dict()
#except FileNotFoundError:
except QueryNotFoundError:
    analytes = False
    units_preset = False

# detect whether a second header is present for units and process
units = {}
u = data.iloc[0].values
b = []
for v in u:
#    if is_numeric_dtype(v) and not np.isnan(v):
    if 'int' in str(type(v)) or 'float' in str(type(v)):
        b.append(True)
    else:
        b.append(False)
b = np.array(b)

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

# convert table to long format...can't sort on sample id if nonnumeric
datalong = pd.melt(data, id_vars=idcols)#.sort_values('sample_id')
# drop any rows that don't have a value
datalong = datalong.dropna(subset=['value']).reset_index(drop=True)
# rename as necessary
datalong.rename(columns={'variable':'analyte'}, inplace=True)
# add units
if units:
    datalong['unit'] = [units[a] for a in datalong['analyte']]


# save results
datalong.to_csv(sr.filePathOut, sep='\t', index=False)

