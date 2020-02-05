#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 24 10:13:59 2020

@author: dsievers

Script helper functions and objects
"""

import pandas as pd
import os
import csv
from io import StringIO

# =============================================================================
# 
# =============================================================================
class ScriptReader:
    """
    Read in the script metadata and also read the data file for transformation.
    Additional properties can be defined by calling addProp, which fetches
    them from the RunProperties file: useful for passing run parameters
    defined by the user at time of import.

    Parameters
    ----------
    filePathRunProperties : str
        Complete file path to run properties file. Defined by something like
        "${runInfo}".
    raw : bool
        Whether to use the raw/uploaded data file (true, default) or use the
        pre-processed TSV file created after LabKey has already tried to fit
        data to the Assay design.
    
    Methods
    -------
    addProp
        Add a property from the run properties file.
        
    read
        Read in the data file to pd.DataFrame.
    
    Attributes
    ----------
    data : pd.DataFrame
        A table containing the data.
    filePathIn : str
        Path to the script input data file; found using fileInVersion.
    filePathOut : str
        Path to the script output data file
    filePathRunPropertiesOut : str
        Path for writing modified run properties file from script; optional.    
    """
    def __init__(self, filePathRunProperties, raw=True):
        self.filePathRunProperties = filePathRunProperties
        self.filePathIn = ""
        self.filePathOut = ""
        self.filePathRunPropertiesOut = ""
        """
        Adapted from https://www.labkey.org/Documentation/_PremiumResources/wiki-download.view?entityId=f5ffaba0-e0aa-1037-a277-93f094e8571c&name=pyScript.py
        Read the runProperties.tsv file for the file paths of the data file we read from, and the one we will write to.
        Tab delimited in the format:
            [property name]    [property value]    [data type]    [transformed data location]
        Script output is called 'AssayRunTSVData', which also resides on the 'runDataFile' line.
        
        Note the original LK example script cannot be used as-is. Instead csv.reader is
        necessary to deal with real tabs ('\t') and ...\t... characters that are part 
        of the actual file paths together.
        """
        if raw:
            # use raw import file as source (avoid any LK pre-formatting)
            self.fileInVersion = 'runDataUploadedFile'
        else:
            # this version has already been converted to tsv and vetted against LK assay design
            # (some columns may be missing at this point)
            self.fileInVersion = 'runDataFile'
        
        with open(self.filePathRunProperties, "r") as f:
            for row in csv.reader(f, dialect='excel-tab'):
                assert len(row) <= 4  # protect against runaway splits
                if row[0] == self.fileInVersion:
                    self.filePathIn = row[1]
                if row[0] == "runDataFile":  # output path is also located on this line
                    self.filePathOut = row[3]
                if row[0] == "transformedRunPropertiesFile":
                    self.filePathRunPropertiesOut = row[1]
        
    def addProp(self, newprop, lookupName=None, col=1):
        if lookupName is None:
            lookupName = newprop
        with open(self.filePathRunProperties, "r") as f:
            for row in csv.reader(f, dialect='excel-tab'):
                assert len(row) <= 4  # protect against runaway splits
                if row[0] == lookupName:
                    setattr(self, newprop, row[col])

    def read(self, **kwargs):
        # import raw data
        extension = os.path.splitext(self.filePathIn)[1]
        if 'xls' in extension.lower():
        #TODO add openpyxl module
            self.data = pd.read_excel(self.filePathIn, **kwargs)
        else:  # then assume text file
            with open(self.filePathIn, 'r', errors='ignore') as f:
                # note errors must be ignored to deal with "can't decode byte 0xff in position 0"
                # when ingesting Opto22 datalogs
                s = f.read()
            if '\x00' in s:
                # delete null characters (from some Opto22 datalogs)
                s = s.replace('\x00', '')
            if '\t' in s:
                delimiter = '\t'
            elif ',' in s:
                delimiter = ','
            else:
                delimiter = None  # default
            self.data = pd.read_csv(StringIO(s), sep=delimiter, **kwargs)










