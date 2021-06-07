# -*- coding: utf-8 -*-
"""
Created on Fri Feb  5 17:06:06 2021

@author: consultant138
"""

import numpy as np
import pandas as pd
import datetime as dt
import Viteos_Miscellaneous_Functions
from Viteos_Miscellaneous_Functions import reset_index_for_df
from Read import Read_Class as rd_cl
from Write import Write_Class as wrt_cl

class Viteos_Updown_125_Class (rd_cl, wrt_cl):
    """
    A class used to output Updown transactions in Weiss Cash Rec; Setup Code - 125 

    ...

    Attributes
    ----------
    param_pb_df : pd.DataFrame
        a Dataframe from PB side which has observations pertaining only to PB side account
    param_acct_df : pd.DataFrame
        a Dataframe from PB side which has observations pertaining only to PB side account

    Methods
    -------
	create_match_cols_01(param_df : pd.DataFrame, param_match_cols_01_names_dict : dict) -> pd.DataFrame
        Creates columns after matching values for columns stored in param_match_cols_01_names_dict. Created columns have value 1 for matching column value and 0 for non matching value
    
	m_cross_n_loop_updown(param_list_to_loop_on : list, param_left_df : pd.DataFrame, param_right_df : pd.DataFrame, param_column_to_slice_on_key = 'ViewData.Task Business Date1')
		Creates M X N architecture
	
	date_column_str_to_pddate(param_col_name : str, param_df : pd.DataFrame) -> pd.DataFrame
		Creates date value inside column to pd.Date value
	
	updmark(y : np.float64, x : list) -> int	
		Creates marker column for up down transacitons, 1 for matching, 0 for null or non matching
		
	equals_fun(a : np.ndarray, b : np.ndarray) -> int
		Matches two values a and b. Returns 1 for equal and 0 for unequal
		
	updownat(param_map_match_values : np.ndarray, param_curr_match_values : np.ndarray, param_sd_match_values : np.ndarray, param_fund_match_values : np.ndarray) -> np.ndarray	
		Return updown comment with a fixed template. Updown suffix is calculated based on values (by decreasing order of values matching to zero) given in paramter argument columns
		
	"""

    sel_col = ['ViewData.Currency',
        'ViewData.Accounting Net Amount',
        'ViewData.Cust Net Amount', 'ViewData.BreakID',
        'ViewData.CUSIP', 'ViewData.Description', 'ViewData.Fund',
        'ViewData.Investment Type', 
        'ViewData.ISIN', 'ViewData.Keys', 
        'ViewData.Mapped Custodian Account',  'ViewData.Prime Broker',
        'ViewData.Settle Date1',
        'ViewData.Quantity',
        'ViewData.Status',
        'ViewData.Ticker', 
        'ViewData.Transaction Type', 
        'ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds', 
        'ViewData.Task Business Date1','ViewData.InternalComment2'
        ]
    
    cols_to_not_use = ['ViewData.CUSIP','ViewData.ISIN','ViewData.Keys','ViewData.Description','ViewData.Prime Broker','Internal Comment2','Status','Ticker']    

    SideA_cols_mapping_dict = {'SideA.ViewData.Side0_UniqueIds':'Side0_UniqueIds',
                               'SideA.ViewData.Side1_UniqueIds':'Side1_UniqueIds',
                               'SideA.PredictedCategory':'PredictedCategory',
                               'SideA.PredictedComment':'PredictedComment',
                               'SideA.Predicted_action' : 'Predicted_action' 
							   }

    SideB_cols_mapping_dict = {'SideB.ViewData.Side0_UniqueIds':'Side0_UniqueIds',
                               'SideB.ViewData.Side1_UniqueIds':'Side1_UniqueIds',
                               'SideB.PredictedCategory':'PredictedCategory',
                               'SideB.PredictedComment':'PredictedComment',
                               'SideB.Predicted_action' : 'Predicted_action' 
							   }

    updown_df_to_return_col = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','PredictedCategory','PredictedComment','Predicted_action']
	
    match_cols_01_names_dict = {'map_match' : ['SideA.ViewData.Mapped Custodian Account', 'SideB.ViewData.Mapped Custodian Account'],
                                'amt_match' : ['SideA.ViewData.Accounting Net Amount', 'SideB.ViewData.Cust Net Amount'],
                                'fund_match' : ['SideA.ViewData.Fund', 'SideB.ViewData.Fund'],
                                'curr_match' : ['SideA.ViewData.Currency', 'SideB.ViewData.Currency'],
                                'sd_match' : ['SideA.ViewData.Settle Date1', 'SideB.ViewData.Settle Date1'] }


    PredictedCategory_val = 'Updown'    
    Predicted_action_val = 'No-Pair'	

	
    def __init__(self, param_pb_df, param_acct_df):

        """
        Parameters
        ----------
		param_pb_df : pd.DataFrame
        a Dataframe from PB side which has observations pertaining only to PB side account
		
		param_acct_df : pd.DataFrame
        a Dataframe from PB side which has observations pertaining only to PB side account
        """
        
        vec_updown_mark = np.vectorize(self.updmark)
        vec_updownat = np.vectorize(self.updownat)
        final_sel_col = [x for x in self.sel_col if x not in self.cols_to_not_use]
        
        self.updown_pb_df = param_pb_df.copy()
        self.updown_acct_df = param_acct_df.copy()
        self.updown_pb_df = self.updown_aa_new[final_sel_col]
        self.updown_bb_new = self.updown_bb_new[final_sel_col]

        date_cols_list = ['ViewData.Settle Date','ViewData.Trade Date', 'ViewData.Task Business Date']
        self.updown_pb_df = self.date_column_str_to_pddate_list(param_col_name_list = date_cols_list, param_df_in_list_function = self.updown_pb_df)
        self.updown_acct_df = self.date_column_str_to_pddate_list(param_col_name_list = date_cols_list, param_df_in_list_function = self.updown_acct_df)
                
        updown_col_rename_dict_at_start = {'ViewData.B-P Net Amount' : 'ViewData.Cust Net Amount'}
        
        self.updown_pb_df.rename(columns = updown_col_rename_dict_at_start, inplace = True)
        self.updown_acct_df.rename(columns = updown_col_rename_dict_at_start, inplace = True)
        

        bplist = self.updown_pb_df.groupby('ViewData.Task Business Date1')['ViewData.Cust Net Amount'].apply(list).reset_index()
        acclist = self.updown_acct_df.groupby('ViewData.Task Business Date1')['ViewData.Accounting Net Amount'].apply(list).reset_index()

        updown_df = pd.merge(bplist, acclist, on = 'ViewData.Task Business Date1', how = 'inner')

        if(updown_df.shape[0] != 0):
            updown_df['upd_amt'] = updown_df.apply(lambda x : [value for value in x['ViewData.Cust Net Amount'] if value in x['ViewData.Accounting Net Amount']], axis = 1)
            updown_df = updown_df[['ViewData.Task Business Date1','upd_amt']]
            self.updown_pb_df = pd.merge(self.updown_pb_df, updown_df, on = 'ViewData.Task Business Date1', how = 'left')
            self.updown_acct_df = pd.merge(self.updown_acct_df, updown_df, on = 'ViewData.Task Business Date1', how = 'left')
        else:
            updown_df = pd.DataFrame()

        self.updown_pb_df['upd_amt']= self.updown_pb_df['upd_amt'].fillna('Null_Amount_Value')
        self.updown_acct_df['upd_amt']= self.updown_acct_df['upd_amt'].fillna('Null_Amount_Value')

        self.updown_pb_df['upd_mark'] = self.updown_pb_df.apply(lambda x : vec_updown_mark(x['ViewData.Cust Net Amount'], x['upd_amt']) , axis= 1)
        self.updown_acct_df['upd_mark'] = self.updown_acct_df.apply(lambda x : vec_updown_mark(x['ViewData.Accounting Net Amount'], x['upd_amt']) , axis= 1)
        
        self.updown_pb_df_upd_mark_1 = self.updown_pb_df[self.updown_pb_df['upd_mark']==1]
        self.updown_acct_df_upd_mark_1 = self.updown_acct_df[self.updown_acct_df['upd_mark']==1]
        
        self.vec_equals_fun = np.vectorize(self.equals_fun)
    
        
        if((self.updown_pb_df_upd_mark_1.shape[0]!=0) & (self.updown_acct_df_upd_mark_1.shape[0]!=0)):
            pool =[]
            key_index =[]
            training_df =[]
            call1 = []
            Task_Business_Date_unique_values_list = list(set(list(set(updown_acct_df_upd_mark_1['ViewData.Task Business Date1'])) + list(set(updown_pb_df_upd_mark_1['ViewData.Task Business Date1']))))
            self.updown_m_cross_n_df = pd.concat(self.m_cross_n_loop_updown(param_list_to_loop_on = Task_Business_Date_unique_values_list, param_left_df = self.updown_pb_df_upd_mark_1, param_right_df = self.updown_acct_df_upd_mark_1, param_column_to_slice_on_key = 'ViewData.Task Business Date1'))
            self.updown_m_cross_n_df = self.create_match_cols_01(param_df = self.updown_m_cross_n_df, param_match_cols_01_names_dict = match_cols_01_names_dict)
            self.updown_m_cross_n_df['key_match_sum'] = self.updown_m_cross_n_df['map_match'] + self.updown_m_cross_n_df['curr_match']
            if(self.updown_m_cross_n_df.shape[0] != 0):
                self.updown_df_to_return = updown_m_cross_n_df[(updown_m_cross_n_df['amt_match']==1) & (updown_df_to_return['key_match_sum']>=1)]
            else:
                aa_new = aa_new.copy()
                bb_new = bb_new.copy()
                flag_side1_ids_to_remove_from_aa_new_exist = 0
                flag_side0_ids_to_remove_from_bb_new_exist = 0
                self.updown_df_to_return = pd.DataFrame()
			
            if(self.pre_updown_df_to_return.shape[0] != 0):
                self.pre_updown_df_to_return['SideA.PredictedCategory'] = self.PredictedCategory_val
                self.pre_updown_df_to_return['SideB.PredictedCategory'] = self.PredictedCategory_val
                self.pre_updown_df_to_return['SideA.Predicted_action'] = self.Predicted_action_val
                self.pre_updown_df_to_return['SideB.Predicted_action'] = self.Predicted_action_val
                updown_df_to_return_map_match_values = self.pre_updown_df_to_return['map_match'].values
                updown_df_to_return_curr_match_values = self.pre_updown_df_to_return['curr_match'].values
                updown_df_to_return_sd_match_values = self.pre_updown_df_to_return['sd_match'].values
                updown_df_to_return_fund_match_values = self.pre_updown_df_to_return['fund_match'].values
				
                self.updown_df_to_return['SideA.PredictedComment'] = vec_updownat(param_map_match_values = updown_df_to_return_map_match_values, param_curr_match_values = updown_df_to_return_curr_match_values, param_sd_match_values = updown_df_to_return_sd_match_values, param_fund_match_values = updown_df_to_return_fund_match_values)
                self.updown_df_to_return['SideB.PredictedComment'] = vec_updownat(param_map_match_values = updown_df_to_return_map_match_values, param_curr_match_values = updown_df_to_return_curr_match_values, param_sd_match_values = updown_df_to_return_sd_match_values, param_fund_match_values = updown_df_to_return_fund_match_values)
				
							
                sideA_col = ['SideA.' +  col for col in self.updown_df_to_return_col]
                sideB_col = ['SideB.' +  col for col in self.updown_df_to_return_col]
        
                self.updown_df_to_return_A_side = self.updown_df_to_return[sideA_col]
                self.updown_df_to_return_B_side = self.updown_df_to_return[sideB_col]
    
                self.updown_df_to_return_A_side = self.updown_df_to_return_A_side.rename(columns= self.SideA_cols_mapping_dict) 
                self.updown_df_to_return_B_side = self.updown_df_to_return_B_side.rename(columns= self.SideB_cols_mapping_dict)

                self.updown_df_to_return = pd.concat([self.updown_df_to_return_B_side,self.updown_df_to_return_B_side])
                self.updown_df_to_return = reset_index_for_df(param_df = self.updown_df_to_return)

#                DONE : Rohit to write elimination code to remove ids containing 'up/down at mapped custodian account' and ids containing 'up/down at currency'
                updown_mapped_custodian_acct_side0_ids = self.updown_df_to_return[(self.updown_df_to_return['predicted comment'] == 'up/down at mapped custodian account') & (~self.updown_df_to_return['Side0_UniqueIds'].isin(['None','nan','']))]['Side0_UniqueIds']                
                updown_mapped_custodian_acct_side1_ids = elim[(elim['predicted comment'] == 'up/down at mapped custodian account') & (~elim['Side1_UniqueIds'].isin(['None','nan','']))]['Side1_UniqueIds']                
                updown_currency_side0_ids = elim[(elim['predicted comment'] == 'up/down at currency') & (~elim['Side0_UniqueIds'].isin(['None','nan','']))]['Side0_UniqueIds']                
                updown_currency_side1_ids = elim[(elim['predicted comment'] == 'up/down at currency') & (~elim['Side1_UniqueIds'].isin(['None','nan','']))]['Side1_UniqueIds']                
#        
#        if((len(updown_currency_side1_ids) != 0) & (len(updown_mapped_custodian_acct_side1_ids) != 0)):
#            list_of_side1_ids_to_remove_from_aa_new = updown_currency_side1_ids.to_list() + updown_mapped_custodian_acct_side1_ids.to_list()
#        elif((len(updown_currency_side1_ids) == 0) & (len(updown_mapped_custodian_acct_side1_ids) != 0)):
#            list_of_side1_ids_to_remove_from_aa_new = updown_mapped_custodian_acct_side1_ids.to_list()
#        elif((len(updown_currency_side1_ids) != 0) & (len(updown_mapped_custodian_acct_side1_ids) == 0)):
#            list_of_side1_ids_to_remove_from_aa_new = updown_currency_side1_ids.to_list()
#        else:
#            list_of_side1_ids_to_remove_from_aa_new = []
#            
#        if((len(updown_currency_side0_ids) != 0) & (len(updown_mapped_custodian_acct_side0_ids) != 0)):
#            list_of_side0_ids_to_remove_from_bb_new = updown_currency_side0_ids.to_list() + updown_mapped_custodian_acct_side0_ids.to_list()
#        elif((len(updown_currency_side0_ids) == 0) & (len(updown_mapped_custodian_acct_side0_ids) != 0)):
#            list_of_side0_ids_to_remove_from_bb_new = updown_mapped_custodian_acct_side0_ids.to_list()
#        elif((len(updown_currency_side0_ids) != 0) & (len(updown_mapped_custodian_acct_side0_ids) == 0)):
#            list_of_side0_ids_to_remove_from_bb_new = updown_currency_side0_ids.to_list()
#        else:
#            list_of_side0_ids_to_remove_from_bb_new = []
#
#        if(len(list_of_side1_ids_to_remove_from_aa_new) != 0):
#            list_of_side1_ids_to_remove_from_aa_new_without_duplicates = list(set(list_of_side1_ids_to_remove_from_aa_new))
#            flag_side1_ids_to_remove_from_aa_new_exist = 1
#        else:
#            flag_side1_ids_to_remove_from_aa_new_exist = 0
#            list_of_side1_ids_to_remove_from_aa_new_without_duplicates = []
#        if(len(list_of_side0_ids_to_remove_from_bb_new) != 0):
#            list_of_side0_ids_to_remove_from_bb_new_without_duplicates = list(set(list_of_side0_ids_to_remove_from_bb_new))
#            flag_side0_ids_to_remove_from_bb_new_exist = 1
#        else:
#            flag_side0_ids_to_remove_from_bb_new_exist = 0
#            list_of_side0_ids_to_remove_from_bb_new_without_duplicates = []
#        
#        
#        #       Remove Side0_UniqueIds from bb_new and Side1_UniqueIds from aa_new
#        aa_new = aa_new[~aa_new['ViewData.Side1_UniqueIds'].isin(list_of_side1_ids_to_remove_from_aa_new_without_duplicates)]
#        bb_new = bb_new[~bb_new['ViewData.Side0_UniqueIds'].isin(list_of_side0_ids_to_remove_from_bb_new_without_duplicates)]
#        
#        #        Remove ids containing 'up/down at mapped custodian account' and ids containing 'up/down at currency' from elim. 
#        elim_except_mapped_custodian_acct_and_currency = elim[~elim['Side1_UniqueIds'].isin(list_of_side1_ids_to_remove_from_aa_new_without_duplicates)]
#        elim_except_mapped_custodian_acct_and_currency = elim_except_mapped_custodian_acct_and_currency[~elim_except_mapped_custodian_acct_and_currency['Side0_UniqueIds'].isin(list_of_side0_ids_to_remove_from_bb_new_without_duplicates)]
#        #        This elim containing remaining up-down comments will be interesected with final_umr_table. All intersected ids will be removed from elim_except_mapped_custodian_acct_and_currency. This new elim df will be elim_except_mapped_custodian_acct_and_currency_and_umr. Ids from elim_except_mapped_custodian_acct_and_currency_and_umr will be removed before making final_no_pair_table
#        elim.drop_duplicates(keep=False, inplace = True)
#        elim_except_mapped_custodian_acct_and_currency.drop_duplicates(keep=False, inplace = True)
#    else:
#        aa_new = aa_new.copy()
#        bb_new = bb_new.copy()
#        flag_side1_ids_to_remove_from_aa_new_exist = 0
#        flag_side0_ids_to_remove_from_bb_new_exist = 0
#        elim = pd.DataFrame()
#else:
#    aa_new = aa_new.copy()
#    bb_new = bb_new.copy()
#    flag_side1_ids_to_remove_from_aa_new_exist = 0
#    flag_side0_ids_to_remove_from_bb_new_exist = 0
#    elim = pd.DataFrame()
#stop_time_updown = timeit.default_timer()
#print('Time for first apply = ', (stop_time_updown - start_time_updown)/60)
#1111




        
    def create_match_cols_01(self, param_df : pd.DataFrame, param_match_cols_01_names_dict : dict) -> pd.DataFrame:
        for key_column_name in param_match_cols_01_names_dict:
            list_first_value_col_name_values = param_df[param_match_cols_01_names_dict.get(key_column_name)[0]].values
            list_second_value_col_name_values = param_df[param_match_cols_01_names_dict.get(key_column_name)[1]].values
            param_df[key_column_name] = self.vec_equals_fun(list_first_value_col_name_values, list_second_value_col_name_values)
        return(param_df)

    def m_cross_n_loop_updown(param_list_to_loop_on : list, param_left_df : pd.DataFrame, param_right_df : pd.DataFrame, param_column_to_slice_on_key = 'ViewData.Task Business Date1'):
        """Creates M X N architecture

        If the argument `sound` isn't passed in, the default Animal
        sound is used.

        Parameters
        ----------
        param_list_to_loop_on : list
            list of values for dates in string format which will be looped on
		
		param_left_df : pd.DataFrame, compulsory
	        Left side dataframe which will be used during merging  

		param_right_df : pd.DataFrame, compulsory
	        Right side dataframe which will be used during merging  
		
		param_column_to_slice_on_key : str (default= 'ViewData.Task Business Date1'), optional
			Date column which will be used for slicing and matching with each value of param_list_to_loop_on. If not given, will default to 'ViewData.Task Business Date1'
		"""
        appended_data = []

        if(len(param_list_to_loop_on) != 0):

            for list_val in param_list_to_loop_on:
                left_df_slice = param_left_df[param_left_df[param_column_to_slice_on_key] == list_val]
                right_df_slice = param_right_df[param_right_df[param_column_to_slice_on_key] == list_val]
                
                left_df_slice['marker'] = 1
                right_df_slice['marker'] = 1
                
                left_df_slice = left_df_slice.reset_index()
                left_df_slice = left_df_slice.drop('index',1)
                right_df_slice = right_df_slice.reset_index()
                right_df_slice = right_df_slice.drop('index',1)
                
                left_df_slice.columns = ['SideB.' + x  for x in left_df_slice.columns] 
                right_df_slice.columns = ['SideA.' + x  for x in right_df_slice.columns]
                
                outer_joined_slice = pd.merge(left_df_slice, right_df_slice, left_on = 'SideB.marker', right_on = 'SideA.marker', how = 'outer')
                appended_data.append(outer_joined_slice)
                                
        else:
            appended_data = []

        return(appended_data)
        
    def date_column_str_to_pddate(self, param_col_name : str, param_df : pd.DataFrame) -> pd.DataFrame:
        param_df[param_col_name] = pd.to_datetime(param_df[param_col_name])
        param_df[param_col_name + '1'] = param_df[param_col_name].dt.date
        return(param_df)
    
    def date_column_str_to_pddate_list(self, param_col_name_list : list, param_df_in_list_function : pd.DataFrame) -> pd.DataFrame:
        for col_name in param_col_name_list:
            param_df_in_list_function = self.date_column_str_to_pddate(param_col_name = col_name, param_df = param_df_in_list_function)
        return(param_df_in_list_function)
        
    def updmark(self, y : np.float64, x : list) -> int:
        if x =='Null_Amount_Value':
            return 0
        else:
            if y in x:
                return 1
            else:
                return 0

    def equals_fun(self, a : np.ndarray, b : np.ndarray) -> int:
        if a == b:
            return 1
        else:
            return 0
        
    def updownat(self, param_map_match_values : np.ndarray, param_curr_match_values : np.ndarray, param_sd_match_values : np.ndarray, param_fund_match_values : np.ndarray) -> np.ndarray:
        if param_map_match_values == 0:
            fun_comment_suffix = 'mapped custodian account'
        elif param_curr_match_values == 0:
            fun_comment_suffix = 'currency'
        elif param_sd_match_values == 0 :
            fun_comment_suffix = 'Settle Date'
        elif param_fund_match_values == 0:
            fun_comment_suffix = 'fund'    
    #    elif param_ttype_match_values == 0:
    #        k = 'transaction type'
        else :
            fun_comment_suffix = 'Investment type'
            
        fun_comment = 'up/down at'+ ' ' + fun_comment_suffix
        return fun_comment
    
