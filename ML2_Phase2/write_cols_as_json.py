# -*- coding: utf-8 -*-
"""
Created on Wed Nov 18 19:21:53 2020

@author: consultant138
"""
import os
os.chdir('D:\\ViteosModel\\ML2_Phase2')

import json
cols = ['Currency',
        'Account Type',
        'Accounting Net Amount',
        'Age',
        'Age WK',
        'Asset Type Category',
        'Base Currency',
        'Base Net Amount',
        'B-P Net Amount',
        'BreakID',
        'Business Date',
        'Cancel Amount',
        'Cancel Flag',
        'CUSIP',
        'Custodian',
        'Custodian Account',
        'Description',
        'Department',
        'ExternalComment3',
        'Fund',
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
        'Price',
        'Prime Broker',
        'Quantity',
        'SEDOL',
        'Settle Date',
        'SPM ID',
        'Status',
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
cols_json_string = json.dumps(cols)
print(cols_json_string)    
with open('columns_json.json', 'w', encoding='utf-8') as f:
    json.dump(cols_json_string, f, ensure_ascii=False, indent=4)

add_cols = ['ViewData.Side0_UniqueIds', 
            'ViewData.Side1_UniqueIds',
            'ViewData.Task Business Date']
add_cols_json_string = json.dumps(add_cols)
print(add_cols_json_string)    
with open('add_columns_json.json', 'w', encoding='utf-8') as f:
    json.dump(add_cols_json_string, f, ensure_ascii=False, indent=4)
    