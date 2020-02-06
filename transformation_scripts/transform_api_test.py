#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test internal API calls
"""
from __future__ import unicode_literals

# add to $PYTHONPATH here if necessary to access other modules
#import sys
#sys.path.append('...')

from lkutils import ScriptReader


# =============================================================================
# Metadata for script function
# =============================================================================
sr = ScriptReader('${runInfo}', raw=True)
sr.read()
data = sr.data

# =============================================================================
# TEST PYTHON API CALL
# =============================================================================
# only test functionality while reimporting a run of data (don't write to the script output file)

from labkey.utils import create_server_context
from labkey.exceptions import RequestError, QueryNotFoundError, ServerContextError, ServerNotFoundError
from labkey.query import select_rows, update_rows, Pagination, QueryFilter, \
    insert_rows, delete_rows, execute_sql
from requests.exceptions import Timeout


"""
Get user security context
${rLabkeySessionId} 
"""
server = '${baseServerURL}'

#sessionID = '${httpSessionId}'
#apikey = 'session|'+sessionID
apikey = 'apikey|YOURKEYHERE'

#folder = "${containerPath}"
folder = 'FCIC/FY18_LT_baselines'  # Project folder path

schema = 'assay.General.Sample ID Coordination'

#table = 'Batches'
#table = 'Runs'
table = 'Data'

# TODO: Is ssl really necessary for internal script?
server_context = create_server_context(server, 
                                       folder, 
                                       api_key=apikey)



result = select_rows(server_context, schema, table)
rows = result['rows']

# write out results to verify function
with open(sr.filePathRunPropertiesOut, 'a') as f:
    for r in rows:
        f.write(r+'\n')
