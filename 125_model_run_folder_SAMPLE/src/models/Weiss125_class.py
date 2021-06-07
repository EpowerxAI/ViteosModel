# -*- coding: utf-8 -*-
"""
Created on Sat Jan 16 17:37:44 2021

@author: consultant138
"""

import timeit
import numpy as np
import pandas as pd
import os
os.chdir('D:\\ViteosModel')
from tqdm import tqdm
import pickle
import datetime as dt
import sys
from datetime import datetime,date,timedelta
from pandas.io.json import json_normalize
from difflib import SequenceMatcher
import json
import re
import dask.dataframe as dd
import glob
from dateutil.parser import parse
from fuzzywuzzy import fuzz
import random
import decimal
from ViteosDecorator import logging_decorator
from ViteosMongoDB import  ViteosMongoDB_Class as mngdb

class Weiss125_Class:

    
    @logging_decorator
    def __init__(self):
        self.cols = ['Currency',
                     'Account Type',
                     'Accounting Net Amount',
                     #'Accounting Net Amount Difference',
                     #'Accounting Net Amount Difference Absolute ',
                     #'Activity Code',
                     'Age',
                     'Age WK',
                     'Asset Type Category',
                     'Base Currency',
                     'Base Net Amount',
                     #'Bloomberg_Yellow_Key',
                     'B-P Net Amount',
                     #'B-P Net Amount Difference',
                     #'B-P Net Amount Difference Absolute',
                     'BreakID',
                     'Business Date',
                     'Cancel Amount',
                     'Cancel Flag',
                     'CUSIP',
                     'Custodian',
                     'Custodian Account',
                     #'Derived Source',
                     'Description',
                     'Department',
                     #'ExpiryDate',
                     #'ExternalComment1',
                     #'ExternalComment2',
                     'ExternalComment3',
                     'Fund',
                     #'FX Rate',
                     #'Interest Amount',
                     'InternalComment1',
                     'InternalComment2',
                     'InternalComment3',
                     'Investment Type',
                     'Is Combined Data',
                     'ISIN',
                     'Keys',
                     'Mapped Custodian Account',
                     'Net Amount Difference',
                     'Net Amount Difference Absolute',
                     'Non Trade Description',
                     #'OTE Custodian Account',
                     #'Predicted Action',
                     #'Predicted Status',
                     #'Prediction Details',
                     'Price',
                     'Prime Broker',
                     'Quantity',
                     'SEDOL',
                     'Settle Date',
                     'SPM ID',
                     'Status',
                     #'Strike Price',
                     'System Comments',
                     'Ticker',
                     'Trade Date',
                     'Trade Expenses',
                     'Transaction Category',
                     'Transaction ID',
                     'Transaction Type',
                     'Underlying Cusip',
                     'Underlying Investment ID',
                     'Underlying ISIN',
                     'Underlying Sedol',
                     'Underlying Ticker',
                     'Source Combination',
                     '_ID']
                    #'UnMapped']
        
        self.add = ['ViewData.Side0_UniqueIds', 
                    'ViewData.Side1_UniqueIds',
                    #'MetaData.0._RecordID',
                    #'MetaData.1._RecordID',
                    'ViewData.Task Business Date']
        

    @logging_decorator
    def connect_without_ssh(self):
        
#        LOGGER.info('Invoking function connect_without_ssh')
        LOGGER.info('Connecting to ' + self.MONGO_HOST + '\n')
        
#        print('Invoking function connect_without_ssh')
        print('Connecting to ' + self.MONGO_HOST + '\n')
        
        self.client_without_ssh = pymongo.MongoClient(host = self.MONGO_HOST, port = self.MONGO_PORT, username = self.MONGO_USERNAME, password = self.MONGO_PASSWORD) 

        LOGGER.info('\nMongo Client without ssh created\n')
        print('\nMongo Client without ssh created\n')

        LOGGER.info('\n\tDatabases present in server ' + self.MONGO_HOST + '\n')        
        LOGGER.info(self.client_without_ssh.list_database_names())
        
        print('\n\tDatabases present in server ' + self.MONGO_HOST + '\n\t\t')
        print(*self.client_without_ssh.list_database_names(), sep = '\n\t\t')
        
