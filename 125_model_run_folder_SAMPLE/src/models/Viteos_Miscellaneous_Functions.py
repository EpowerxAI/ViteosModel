# -*- coding: utf-8 -*-
"""
Created on Sat Feb 20 14:05:03 2021

@author: consultant138
"""
import pandas as pd
import dateutil
import dateutil.parser
from dateutil.parser import parse
from difflib import SequenceMatcher
import json

def normalize_final_no_pair_table_col_names(param_final_no_pair_table):
    final_no_pair_table_col_names_mapping_dict = {
                                      'SideA.ViewData.Side1_UniqueIds' : 'ViewData.Side1_UniqueIds',
                                      'SideB.ViewData.Side0_UniqueIds' : 'ViewData.Side0_UniqueIds',
                                      'SideA.ViewData.BreakID_A_side' : 'ViewData.BreakID_Side1', 
                                      'SideB.ViewData.BreakID_B_side' : 'ViewData.BreakID_Side0'
                                      }
    param_final_no_pair_table.rename(columns = final_no_pair_table_col_names_mapping_dict, inplace = True)
    return(param_final_no_pair_table)

def normalize_bp_acct_col_names(param_df):
    bp_acct_col_names_mapping_dict = {
                                      'ViewData.Cust Net Amount' : 'ViewData.B-P Net Amount',
                                      'ViewData.Cust Net Amount Difference' : 'ViewData.B-P Net Amount Difference',
                                      'ViewData.Cust Net Amount Difference Absolute' : 'ViewData.B-P Net Amount Difference Absolute',
                                      'ViewData.CP Net Amount' : 'ViewData.B-P Net Amount',
                                      'ViewData.CP Net Amount Difference' : 'ViewData.B-P Net Amount Difference',
                                      'ViewData.CP Net Amount Difference Absolute' : 'ViewData.B-P Net Amount Difference Absolute',
                                      'ViewData.PMSVendor Net Amount' : 'ViewData.Accounting Net Amount'
                                        }
    param_df.rename(columns = bp_acct_col_names_mapping_dict, inplace = True)
    return(param_df)

def contains_multiple_values_in_either_Side_0_or_1_UniqueIds_for_expected_single_sided_status(param_row):
    
    if(',' in str(param_row['ViewData.Side0_UniqueIds'])):
        Side_0_contains_comma = 1
    else:
        Side_0_contains_comma = 0

    if(',' in str(param_row['ViewData.Side1_UniqueIds'])):
        Side_1_contains_comma = 1
    else:
        Side_1_contains_comma = 0
    
    if((str(param_row['ViewData.Status']) in ['OB','SDB','UOB','CNF','CMF']) and ((Side_0_contains_comma == 1) or (Side_1_contains_comma == 1))):
        return('remove')
    else:
        return('keep')

def reset_index_for_df(param_df):
	if(param_df.shape[0] != 0):
		return_df = param_df.reset_index()
		return_df = return_df.drop('index',1)
	else:
		return_df = param_df.copy()
	return(return_df)

def getDateTimeFromISO8601String(s):
    d = dateutil.parser.parse(s)
    return d

def date_various_format(param_str_date_in_ddmmyyyy_format):
	if(isinstance(param_str_date_in_ddmmyyyy_format, str) == True):
		return_date_in_ymd_format = str(parse(param_str_date_in_ddmmyyyy_format, dayfirst = True))[0:10]
		return_date_ymd_iso_18_30_format = return_date_in_ymd_format + 'T18:30:00.000+0000'
		return_date_ymd_iso_00_00_format = return_date_in_ymd_format + 'T00:00:00.000+0000'
	else:
		return_date_in_ymd_format = None
		return_date_ymd_iso_18_30_format = None
		return_date_ymd_iso_00_00_format = None

	return(return_date_in_ymd_format,return_date_ymd_iso_18_30_format,return_date_ymd_iso_00_00_format)
	
def flatten_list(param_2d_list):
    flat_list = []
    # Iterate through the outer list
    for element in param_2d_list:
        if type(element) is list:
            # If the element is of type list, iterate through the sublist
            for item in element:
                flat_list.append(item)
        else:
            flat_list.append(element)
    return flat_list

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def dictionary_exclude_keys(param_dict, param_keys_to_exclude):
    return {x: param_dict[x] for x in param_dict if x not in param_keys_to_exclude}
		
def write_dict_at_top(param_filename, param_dict_to_add):
    with open(param_filename, 'r+') as f:
        param_existing_content = f.read()
        f.seek(0, 0)
        f.write(json.dumps(param_dict_to_add, indent = 4))
        f.write('\n')
        f.write(param_existing_content)

def assign_PB_Acct_side_row_apply(param_row):
    if((param_row['flag_side1'] >= 1) & (param_row['flag_side0'] == 0)):
        PB_or_Acct_Side_Value = 'PB_Side'
    elif((param_row['flag_side1'] == 0) & (param_row['flag_side0'] >= 1)):
        PB_or_Acct_Side_Value = 'Acct_Side'
    else:
        PB_or_Acct_Side_Value = 'Non OB'

    return(PB_or_Acct_Side_Value)
