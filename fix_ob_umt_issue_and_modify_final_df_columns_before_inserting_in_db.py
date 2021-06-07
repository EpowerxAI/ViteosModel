# -*- coding: utf-8 -*-
"""
Created on Wed Dec 16 17:19:28 2020

@author: consultant138
"""
client = 'Weiss'
setup_code = '125'
date_i = '04'

import os
os.chdir('D:\\ViteosModel')

import numpy as np
import pandas as pd
#from imblearn.over_sampling import SMOTE
from sklearn.metrics import accuracy_score 
from sklearn.metrics import classification_report
from tqdm import tqdm
import pickle
import datetime as dt
import sys
from ViteosMongoDB import  ViteosMongoDB_Class as mngdb
from datetime import datetime,date,timedelta
from pandas.io.json import json_normalize
import dateutil.parser
from difflib import SequenceMatcher
import pprint
import json
from pandas import merge
import re

import dask.dataframe as dd
import glob
import math
from sklearn.feature_extraction.text import TfidfVectorizer
from dateutil.parser import parse
import operator
import itertools
from sklearn.feature_extraction.text import CountVectorizer

import xgboost as xgb
from sklearn.preprocessing import LabelEncoder

from fuzzywuzzy import fuzz
import random
import decimal



#Change made on 16-12-2020 by Rohit to operate on columns of final_df_2 before inserting them into db.Copy paste from here

from pandas.api.types import is_datetime64_any_dtype as is_datetime
def fix_string_columns_before_inserting_into_db_for_list_of_string_to_search_report_replace(fun_df,\
                                                                                          fun_list_of_col_names = ['BreakID',\
                                                                                                               'Final_predicted_break',\
                                                                                                               'ML_flag',\
                                                                                                               'Predicted_Status',\
                                                                                                               'Predicted_action',\
                                                                                                               'Side0_UniqueIds',\
                                                                                                               'Side1_UniqueIds',\
                                                                                                               'SourceCombinationCode',\
                                                                                                               'probability_No_pair',\
                                                                                                               'probability_UMB',\
                                                                                                               'probability_UMR',\
                                                                                                               'probability_UMT'],\
                                                                                          fun_list_of_string_to_identify = ['\[','\]','\.0','\\n','AA','BB','None','none','nan','Nan','NaN',', ']):
    
    for col_name in fun_list_of_col_names:
        fun_df[col_name] = fun_df[col_name].astype(str)
        for string_to_identify in fun_list_of_string_to_identify:
            if(string_to_identify == '\.0' and (col_name == 'probability_No_pair' or col_name == 'probability_UMB' or col_name == 'probability_UMR' or col_name == 'probability_UMT')):
                continue
            else:
                if(fun_df[col_name].str.contains(string_to_identify, regex = True).any()):
                    print(col_name + ' column contains ' + string_to_identify + ' character')
                else:
                    print(col_name + ' column does not contain ' + string_to_identify + ' character')
    
                if(string_to_identify == '\[' or string_to_identify == '\]'):
                    fun_df[col_name] = fun_df[col_name].map(lambda x:x.lstrip('[').rstrip(']'))
                    fun_df[col_name] = fun_df[col_name].replace(string_to_identify,'',regex = True)    
                elif(string_to_identify == ', '):
                    fun_df[col_name] = fun_df[col_name].replace(string_to_identify,',',regex = True)
                else:
                    fun_df[col_name] = fun_df[col_name].replace(string_to_identify,'',regex = True)    
                
    return(fun_df)

def fix_BusinessDate_column_before_inserting_into_db(fun_df):
    if(fun_df.dtypes['BusinessDate'] == np.object):
        fun_df['BusinessDate'] = pd.to_datetime(fun_df['BusinessDate'])
        fun_df['BusinessDate'] = fun_df['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
        fun_df['BusinessDate'] = pd.to_datetime(fun_df['BusinessDate'])
    elif(is_datetime(fun_df['BusinessDate']) == True):
        fun_df['BusinessDate'] = fun_df['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
        fun_df['BusinessDate'] = pd.to_datetime(fun_df['BusinessDate'])
    else:
        print('BusinessDate columns is neither datetime nor string')
    return(fun_df)

def fix_SetupID_column_before_inserting_into_db(fun_df):
    fun_df[['SetupID']] = fun_df[['SetupID']].astype(int)
    return(fun_df)

def fix_Predicted_comment_column_before_inserting_into_db(fun_df,
                                                          fun_list_of_string_to_identify = ['AA','BB','None','none','nan','Nan','NaN']):
    fun_df[['PredictedComment']] = fun_df[['PredictedComment']].astype(str)
    for string_to_identify in fun_list_of_string_to_identify:
        if(fun_df['PredictedComment'].str.contains(string_to_identify, regex = True).any()):
            print('PredictedComment' + ' column contains ' + string_to_identify + ' character')
        else:
            print('PredictedComment' + ' column does not contain ' + string_to_identify + ' character')

        fun_df['PredictedComment'] = fun_df['PredictedComment'].replace(string_to_identify,'',regex = True)    
                
    return(fun_df)
    
def fix_TaskID_column_before_inserting_into_db(fun_df):
    fun_df[['TaskID']] = fun_df[['TaskID']].astype(float)
    fun_df[['TaskID']] = fun_df[['TaskID']].astype(np.int64)
    return(fun_df)

def remove_Unnamed_0_column_if_present(fun_df):
    if('Unnamed: 0' in list(fun_df.columns)):
        fun_df.drop(columns = ['Unnamed: 0'], axis = 1, inplace = True)
    return(fun_df)




base_dir_weiss = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Weiss\\'

meo_df = pd.read_csv(base_dir_weiss + 'meo_df_setup_125_date_2020-12-' + date_i + '.csv')
#meo_df_to_use = meo_df[['ViewData.BreakID','ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','ViewData.Accounting Net Amount','ViewData.B-P Net Amount']]
print(meo_df['ViewData.Task Business Date'].value_counts())
#meo_df_to_use = meo_df_to_use.reset_index()
#meo_df_to_use = meo_df_to_use.drop('index', 1)

#os.chdir(base_dir_weiss)
final_df_2 = pd.read_csv(base_dir_weiss + 'final_df_2_before_making_umt_from_umr_setup_125_date_2020-12-' + str(date_i) + '.csv') 
print(final_df_2['BusinessDate'].value_counts())

final_df_2.dtypes
final_df_2 = fix_string_columns_before_inserting_into_db_for_list_of_string_to_search_report_replace(final_df_2)
final_df_2 = fix_BusinessDate_column_before_inserting_into_db(final_df_2)
final_df_2 = fix_SetupID_column_before_inserting_into_db(final_df_2)
final_df_2 = fix_TaskID_column_before_inserting_into_db(final_df_2)
final_df_2 = remove_Unnamed_0_column_if_present(final_df_2)
final_df_2 = fix_Predicted_comment_column_before_inserting_into_db(final_df_2)
final_df_2.dtypes
final_df_2 = final_df_2.reset_index()
final_df_2 = final_df_2.drop('index', 1)

def get_NetAmountDifference_for_Side01UniqueIds_from_Side0UniqueId_and_Side0UniqueId_column_apply_row(fun_row, fun_meo_df):
    side0_UniqueIds_list_str = [x.lstrip().rstrip() for x in fun_row['Side0_UniqueIds'].replace('\' \'','\',\'').replace('\'','').split(',')]
    side1_UniqueIds_list_str = [x.lstrip().rstrip() for x in fun_row['Side1_UniqueIds'].replace('\' \'','\',\'').replace('\'','').split(',')]
    lst_Net_Amount_Difference_for_side0_UniqueIds_list_str = list(fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'].isin(side0_UniqueIds_list_str)]['ViewData.Accounting Net Amount'].unique())
    lst_Net_Amount_Difference_for_side1_UniqueIds_list_str = list(fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin(side1_UniqueIds_list_str)]['ViewData.B-P Net Amount'].unique())
    amount_diff_of_sum_of_side_0_1_lists = sum(lst_Net_Amount_Difference_for_side0_UniqueIds_list_str) - sum(lst_Net_Amount_Difference_for_side1_UniqueIds_list_str)
    rounded_amount_diff_of_sum_of_side_0_1_lists = round(amount_diff_of_sum_of_side_0_1_lists,3)
    if(fun_row['Predicted_Status'] == 'UMR'):
        if((abs(rounded_amount_diff_of_sum_of_side_0_1_lists) >= 0.01) and (abs(rounded_amount_diff_of_sum_of_side_0_1_lists) <= 1.00)):
            Predicted_Status_new = 'UMT'
            Predicted_action_new = fun_row['Predicted_action'].replace('UMR','UMT')
        else:
            Predicted_Status_new = fun_row['Predicted_Status'] 
            Predicted_action_new = fun_row['Predicted_action']
    else:
        Predicted_Status_new = fun_row['Predicted_Status'] 
        Predicted_action_new = fun_row['Predicted_action']

    return(Predicted_Status_new, Predicted_action_new, rounded_amount_diff_of_sum_of_side_0_1_lists,lst_Net_Amount_Difference_for_side0_UniqueIds_list_str,lst_Net_Amount_Difference_for_side1_UniqueIds_list_str)

final_df_2[['Predicted_Status_new','Predicted_action_new','Sum_of_Net_Amount_Difference','lst_Net_Amount_Difference_for_side0_UniqueIds','lst_Net_Amount_Difference_for_side1_UniqueIds']] = final_df_2.apply(lambda row : get_NetAmountDifference_for_Side01UniqueIds_from_Side0UniqueId_and_Side0UniqueId_column_apply_row(fun_row = row, fun_meo_df = meo_df), axis = 1,result_type="expand")

filepaths_final_df_2_after_making_umt_from_umr = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\fix_umt_ob_issue_for_these_final_df_2\\new_final_df_2_after_making_umt_from_umr_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
final_df_2.to_csv(filepaths_final_df_2_after_making_umt_from_umr)

final_df_2.drop(columns = ['Predicted_Status','Predicted_action'], axis = 1, inplace = True)

final_df_2.rename(columns = {'Predicted_Status_new' : 'Predicted_Status','Predicted_action_new' : 'Predicted_action'}, inplace = True)

final_df_2.drop(columns = ['Sum_of_Net_Amount_Difference','lst_Net_Amount_Difference_for_side0_UniqueIds','lst_Net_Amount_Difference_for_side1_UniqueIds'], axis = 1, inplace = True)

filepaths_final_df_2 = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\fix_umt_ob_issue_for_these_final_df_2\\final_df_2_setup_' + setup_code + '_date_' + str(date_i) + '_3.csv'
final_df_2.to_csv(filepaths_final_df_2)
print(final_df_2['BusinessDate'].value_counts())


#1111
del(final_df_2)
base_dir = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Weiss\\fix_umt_ob_issue_for_these_final_df_2\\'
os.chdir(base_dir)
#4_dec_2020
final_df_2 = pd.read_csv(base_dir + 'final_df_2_setup_125_date_' + date_i + '_3.csv')
print(final_df_2['BusinessDate'].value_counts())
final_df_2.dtypes
final_df_2 = fix_string_columns_before_inserting_into_db_for_list_of_string_to_search_report_replace(final_df_2)
final_df_2 = fix_BusinessDate_column_before_inserting_into_db(final_df_2)
final_df_2 = fix_SetupID_column_before_inserting_into_db(final_df_2)
final_df_2 = fix_TaskID_column_before_inserting_into_db(final_df_2)
final_df_2 = remove_Unnamed_0_column_if_present(final_df_2)
final_df_2 = fix_Predicted_comment_column_before_inserting_into_db(final_df_2)
final_df_2.dtypes
final_df_2.to_csv(date_i + '_dec.csv')

import os
os.chdir('D:\\ViteosModel')

mngdb_obj_1_for_reading_and_writing_in_uat_server = mngdb(param_without_ssh  = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = '10.1.15.137', param_MONGO_PORT = 27017,
#                 param_MONGO_HOST = '192.168.170.50', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = 'mongouseradmin', param_MONGO_PASSWORD = '@L0ck&Key')
#                 param_MONGO_USERNAME = '', param_MONGO_PASSWORD = '')
mngdb_obj_1_for_reading_and_writing_in_uat_server.connect_with_or_without_ssh()
db_1_for_MEO_data = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['ReconDB_ML']

coll_1_for_writing_prediction_data = db_1_for_MEO_data['MLPrediction_Cash']

#Break up final_df_2 into two parts : 
#    Part 1. final_df_2_without_UMR_Many_One_to_One_Many since it will contain BreakId as int64
#    Part 2. final_df_2_with_UMR_Many_One_to_One_Many since it will contain BreakId as list for Many to One and Many to Many breaks

final_df_2_without_UMR_Many_One_to_One_Many = final_df_2[final_df_2['Predicted_action'] != 'UMR_One-Many_to_Many-One']
final_df_2_without_UMR_Many_One_to_One_Many['BreakID'] = final_df_2_without_UMR_Many_One_to_One_Many['BreakID'].astype(str)
final_df_2_without_UMR_Many_One_to_One_Many['Final_predicted_break'] = final_df_2_without_UMR_Many_One_to_One_Many['Final_predicted_break'].astype(str)

data_dict_without_UMR_Many_One_to_One_Many = final_df_2_without_UMR_Many_One_to_One_Many.to_dict("records_final_1")
coll_1_for_writing_prediction_data.insert_many(data_dict_without_UMR_Many_One_to_One_Many) 


final_df_2_with_UMR_Many_One_to_One_Many = final_df_2[final_df_2['Predicted_action'].isin(['UMR_One-Many_to_Many-One','UMR_Many_to_Many'])]

final_df_2_with_UMR_Many_One_to_One_Many['BreakID'] = final_df_2_with_UMR_Many_One_to_One_Many['BreakID'].astype(str)
final_df_2_with_UMR_Many_One_to_One_Many['Final_predicted_break'] = final_df_2_with_UMR_Many_One_to_One_Many['Final_predicted_break'].astype(str)
final_df_2_with_UMR_Many_One_to_One_Many['BreakID'] = final_df_2_with_UMR_Many_One_to_One_Many['BreakID'].map(lambda x:x.lstrip('[').rstrip(']'))
final_df_2_with_UMR_Many_One_to_One_Many['Final_predicted_break'] = final_df_2_with_UMR_Many_One_to_One_Many['Final_predicted_break'].map(lambda x:x.lstrip('[').rstrip(']'))

data_dict_with_UMR_Many_One_to_One_Many = final_df_2_with_UMR_Many_One_to_One_Many.to_dict("records_final_2")
coll_1_for_writing_prediction_data.insert_many(data_dict_with_UMR_Many_One_to_One_Many) 




