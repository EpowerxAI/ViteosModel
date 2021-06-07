# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 12:57:52 2021

@author: consultant138
"""
import pandas as pd
import Viteos_Miscellaneous_Functions

mapping_dict_trans_type_125 = {
                        'BUY_SELL' : ['Buy','Sell','buy','sell','BUY','SELL'],
                        'XSSHORT_Sell' : ['XSSHORT','Sell'],
                        'XSELL_Sell' : ['XSSHORT','Sell'],
                        'XBCOVER_Buy' : ['XBCOVER','Buy'],
                        'XBUY_Buy' : ['XBUY','Buy'],
                        'WITHDRAWAL_DEPOSIT' : ['WITHDRAWAL','DEPOSIT'],
                        'Withdraw_Deposit' : ['Withdraw','Deposit'],
                        'SPEC_Stk_Loan_Jrl_DEP' : ['SPEC Stk Loan Jrl','DEP'],
                        'SPEC_Stk_Loan_Jrl_WTH' : ['SPEC Stk Loan Jrl','WTH'],
                        'CASH_DEPOSIT_PAYMENT' : ['CASH DEPOSIT','PAYMENT'],
                        'DELIVER_RECEIVE_PAYMENT' : ['DELIVER VS PAYMENT','RECEIVE VS PAYMENT'],
                        'CANCEL_INTEREST' : ['CANCEL INTEREST','INTEREST'],
                        'TRF_FM_SHORT_MARK_TO_MARKET' : ['TRF FM MARGIN MARK TO MARKET','TRF TO SHORT MARK TO MARKET'],
                        'SHORT_POSITION_INTRST_DIVIDEND_CANCEL' : ['SHORT POSITION INTRST/DIVIDEND','SHORT POSITION CANCEL']
# Added on 27-12-2020 to catch non interacting transaction type.
                        ,'ARRANGING CASH COLLATERAL_non_interacting' : ['ARRANGING CASH COLLATERAL','ARRANGING CASH COLLATERAL']
                        ,'MARK TO THE MARKET_non_interacting' : ['MARK TO THE MARKET','MARK TO THE MARKET']
                        ,'CASH BALANCE TYPE ADJUSTMENT_non_interacting' : ['CASH BALANCE TYPE ADJUSTMENT','CASH BALANCE TYPE ADJUSTMENT']
                        ,'MARGIN TYPE JOURNAL_non_interacting' : ['MARGIN TYPE JOURNAL','MARGIN TYPE JOURNAL']
                        ,'JOURNAL_non_interacting' : ['JOURNAL','JOURNAL']
# Added on 27-12-202 to catch Tran Type = ForwardFX for Mapped Custodian Account values of UBS_UBFX_ON and UBS_UBFX_OP 
                        ,'ForwardFX_UBS_UBFX_ON_OP' : ['ForwardFX','ForwardFX']
}
mapping_dict_trans_type_379 = {
                        'STIF Interest_non_interacting' : ['STIF Interest','STIF Interest'],
                        'Same' : ['everthing_else','everthing_else']
                        }

mapping_dict_trans_type_setup_code = {'125' : mapping_dict_trans_type_125,
									  '379' : mapping_dict_trans_type_379}

class ViteosClosed:
	
	def __init__(self, param_setup_code = None):
		self.setup_code = param_setup_code
		return None

	def assign_Transaction_Type_for_closing_apply_row(self, param_row, param_setup_code_crucial = None, param_transaction_type_col_name = 'ViewData.Transaction Type'):
		if(param_setup_code_crucial is None):
			param_setup_code_crucial = self.setup_code
		if(param_setup_code_crucial == '125'):
			if(param_row[param_transaction_type_col_name] in ['Buy','Sell','buy','sell','BUY','SELL']):
		        Transaction_Type_for_closing = 'BUY_SELL'
		    elif(param_row[param_transaction_type_col_name] in ['XSSHORT','Sell']):
		        Transaction_Type_for_closing = 'XSSHORT_Sell'
		    elif(param_row[param_transaction_type_col_name] in ['XSELL','Sell']):
		        Transaction_Type_for_closing = 'XSELL_Sell'
		    elif(param_row[param_transaction_type_col_name] in ['XBCOVER','Buy']):
		        Transaction_Type_for_closing = 'XBCOVER_Buy'
		    elif(param_row[param_transaction_type_col_name] in ['XBUY','Buy']):
		        Transaction_Type_for_closing = 'XBUY_Buy'
		    elif(param_row[param_transaction_type_col_name] in ['WITHDRAWAL','DEPOSIT']):
		        Transaction_Type_for_closing = 'WITHDRAWAL_DEPOSIT'
		    elif(param_row[param_transaction_type_col_name] in ['SPEC Stk Loan Jrl','DEP']):
		        Transaction_Type_for_closing = 'SPEC_Stk_Loan_Jrl_DEP'
		#Note that Transaction_Type_for_closing = 'SPEC_Stk_Loan_Jrl_WTH' will be covered in another column
		#    elif(param_row[param_transaction_type_col_name] in ['SPEC Stk Loan Jrl','WTH']):
		#        Transaction_Type_for_closing = 'SPEC_Stk_Loan_Jrl_WTH'        
		    elif(param_row[param_transaction_type_col_name] in ['CASH DEPOSIT','PAYMENT']):
		        Transaction_Type_for_closing = 'CASH_DEPOSIT_PAYMENT'
		    elif(param_row[param_transaction_type_col_name] in ['DELIVER VS PAYMENT','RECEIVE VS PAYMENT']):
		        Transaction_Type_for_closing = 'DELIVER_RECEIVE_PAYMENT'
		    elif(param_row[param_transaction_type_col_name] in ['CANCEL INTEREST','INTEREST']):
		        Transaction_Type_for_closing = 'CANCEL_INTEREST'
		#     elif(param_row[param_transaction_type_col_name] in ['TRF FM MARGIN MARK TO MARKET','TRF TO SHORT MARK TO MARKET']):
		#         Transaction_Type_for_closing = 'TRF_FM_SHORT_MARK_TO_MARKET'
		    elif(param_row[param_transaction_type_col_name] in ['TRF FM MARGIN MARK TO MARKET','TRF TO SHORT MARK TO MARKET']):
		        Transaction_Type_for_closing = 'TRF_FM_SHORT_MARK_TO_MARKET'
		    elif(param_row[param_transaction_type_col_name] in ['SHORT POSITION INTRST/DIVIDEND','SHORT POSITION CANCEL']):
		        Transaction_Type_for_closing = 'SHORT_POSITION_INTRST_DIVIDEND_CANCEL'
		    elif(param_row[param_transaction_type_col_name] in ['Transfer','nan','None']):
		        Transaction_Type_for_closing = 'TRANSFER_OR_NULL'
		# Added on 27-12-2020 to catch non interacting transaction type.
		    elif(param_row[param_transaction_type_col_name] in ['ARRANGING CASH COLLATERAL']):
		        Transaction_Type_for_closing = 'ARRANGING CASH COLLATERAL_non_interacting'
		    elif(param_row[param_transaction_type_col_name] in ['MARK TO THE MARKET']):
		        Transaction_Type_for_closing = 'MARK TO THE MARKET_non_interacting'
		    elif(param_row[param_transaction_type_col_name] in ['CASH BALANCE TYPE ADJUSTMENT']):
		        Transaction_Type_for_closing = 'CASH BALANCE TYPE ADJUSTMENT_non_interacting'
		    elif(param_row[param_transaction_type_col_name] in ['MARGIN TYPE JOURNAL']):
		        Transaction_Type_for_closing = 'MARGIN TYPE JOURNAL_non_interacting'
		    elif(param_row[param_transaction_type_col_name] in ['JOURNAL']):
		        Transaction_Type_for_closing = 'JOURNAL_non_interacting'
		# Added on 27-12-202 to catch Tran Type = ForwardFX for Mapped Custodian Account values of UBS_UBFX_ON and UBS_UBFX_OP 
		    elif((param_row[param_transaction_type_col_name] in ['ForwardFX']) & (param_row['ViewData.Mapped Custodian Account'] in ['UBS_UBFX_ON','UBS_UBFX_OP'])):
		        Transaction_Type_for_closing = 'ForwardFX_UBS_UBFX_ON_OP'
		
		    else:
		         Transaction_Type_for_closing = param_row[param_transaction_type_col_name]

		elif(param_setup_code_crucial == '379'):
		    if(param_row[param_transaction_type_col_name] in ['STIF Interest']):
		        Transaction_Type_for_closing = 'STIF Interest_non_interacting'
		    else:
		         Transaction_Type_for_closing = 'Same'

	    return(Transaction_Type_for_closing)

	def assign_Transaction_Type_for_closing_apply_row_2(self, param_row, param_setup_code_crucial = None, param_transaction_type_col_name = 'ViewData.Transaction Type2'):
		if(param_setup_code_crucial is None):
			param_setup_code_crucial = self.setup_code
	
		if(param_setup_code_crucial == '125'):
		    if(param_row[param_transaction_type_col_name] in ['SPEC Stk Loan Jrl','WTH']):
		        Transaction_Type_for_closing2 = 'SPEC_Stk_Loan_Jrl_WTH'
		    else:
		        Transaction_Type_for_closing2 = 'Not_SPEC_Stk_Loan_Jrl_WTH'

		return(Transaction_Type_for_closing2)

	def cleaned_meo(self,param_meo_df):
	
	    param_meo_df = Viteos_Miscellaneous_Functions.normalize_bp_acct_col_names(param_df = param_meo_df)
	    
	#    Commened out below line on 26-11-2020 to exclude SPM from closed coverage, and added the line below the commened line
	#    param_meo_df = param_meo_df[~param_meo_df['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]
	    param_meo_df = param_meo_df[~param_meo_df['ViewData.Status'].isin(['SPM','SMT','HST', 'OC', 'CT', 'Archive','SMR','UMB','SMB'])] 
	    param_meo_df = param_meo_df[~param_meo_df['ViewData.Status'].isnull()]\
	                                     .reset_index()\
	                                     .drop('index',1)
	    
	    param_meo_df['Date'] = pd.to_datetime(param_meo_df['ViewData.Task Business Date'])
	    param_meo_df = param_meo_df[~param_meo_df['Date'].isnull()]\
	                          .reset_index()\
	                          .drop('index',1)
	    
	    param_meo_df['Date'] = pd.to_datetime(param_meo_df['Date']).dt.date
	    param_meo_df['Date'] = param_meo_df['Date'].astype(str)
	
	    param_meo_df['ViewData.Side0_UniqueIds'] = param_meo_df['ViewData.Side0_UniqueIds'].astype(str)
	    param_meo_df['ViewData.Side1_UniqueIds'] = param_meo_df['ViewData.Side1_UniqueIds'].astype(str)
	
	    param_meo_df['flag_side0'] = param_meo_df.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
	    param_meo_df['flag_side1'] = param_meo_df.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)
	
	
	    param_meo_df.loc[param_meo_df['ViewData.Side0_UniqueIds']=='nan','flag_side0'] = 0
	    param_meo_df.loc[param_meo_df['ViewData.Side1_UniqueIds']=='nan','flag_side1'] = 0
	
	    param_meo_df.loc[param_meo_df['ViewData.Side0_UniqueIds']=='None','flag_side0'] = 0
	    param_meo_df.loc[param_meo_df['ViewData.Side1_UniqueIds']=='None','flag_side1'] = 0
	
	    param_meo_df.loc[param_meo_df['ViewData.Side0_UniqueIds']=='','flag_side0'] = 0
	    param_meo_df.loc[param_meo_df['ViewData.Side1_UniqueIds']=='','flag_side1'] = 0
	
	    param_meo_df['ViewData.BreakID'] = param_meo_df['ViewData.BreakID'].astype(int)
	    param_meo_df = param_meo_df[param_meo_df['ViewData.BreakID']!=-1] \
	          .reset_index() \
	          .drop('index',1)
	          
	    param_meo_df['Side_0_1_UniqueIds'] = param_meo_df['ViewData.Side0_UniqueIds'].astype(str) + \
	                                param_meo_df['ViewData.Side1_UniqueIds'].astype(str)

	    param_meo_df['PB_or_Acct_Side'] = param_meo_df.apply(lambda row : Viteos_Miscellaneous_Functions.assign_PB_Acct_side_row_apply(param_row = row), axis = 1, result_type="expand")
	    param_meo_df['ViewData.Transaction Type'] = param_meo_df['ViewData.Transaction Type'].astype(str)
	    param_meo_df['Transaction_Type_for_closing'] = param_meo_df.apply(lambda row : self.assign_Transaction_Type_for_closing_apply_row(param_row = row, param_transaction_type_col_name = 'ViewData.Transaction Type'), axis = 1, result_type="expand")
	    param_meo_df['ViewData.Transaction Type2'] = param_meo_df['ViewData.Transaction Type']
	    param_meo_df['Transaction_Type_for_closing_2'] = param_meo_df.apply(lambda row : self.assign_Transaction_Type_for_closing_apply_row_2(param_row = row, param_transaction_type_col_name = 'ViewData.Transaction Type2'), axis = 1, result_type="expand")
	    param_meo_df['abs_net_amount_difference'] = param_meo_df['ViewData.Net Amount Difference'].apply(lambda x : abs(x))
	    param_meo_df = param_meo_df.sort_values(by=['ViewData.Transaction ID','ViewData.Transaction Type'],ascending = False)
	    return(param_meo_df)  

	def interacting_closing(self,param_df): #The name of the function contains 125 but it is also being used in 379
	    if(param_df.shape[0] != 0):
			param_df['ViewData.Mapped Custodian Account'] = param_df['ViewData.Mapped Custodian Account'].astype(str)
		    param_df['ViewData.Currency'] = param_df['ViewData.Currency'].astype(str)    
		    param_df['ViewData.Source Combination Code'] = param_df['ViewData.Source Combination Code'].astype(str)
		    param_df['abs_net_amount_difference'] = param_df['abs_net_amount_difference'].astype(str)
		    param_df['filter'] = param_df['ViewData.Source Combination Code'] + param_df['ViewData.Mapped Custodian Account'] + param_df['ViewData.Currency'] + param_df['abs_net_amount_difference']
		    grouped_by_filter_df = param_df.groupby('filter').size().reset_index(name='counts_for_filter')
		    merged_df_with_filter_counts = pd.merge(param_df, grouped_by_filter_df, on = 'filter', how = 'left')
		    merged_df_with_filter_counts_ge_1 = merged_df_with_filter_counts[merged_df_with_filter_counts['counts_for_filter'] > 1] 
		else:
			merged_df_with_filter_counts_ge_1 = pd.DataFrame()
	    
		return(merged_df_with_filter_counts_ge_1)
	
	def all_combination_file(self,param_df):
	    param_df['filter_key'] = param_df['ViewData.Source Combination Code'].astype(str) + \
	                                       param_df['ViewData.Mapped Custodian Account'].astype(str) + \
	                                       param_df['ViewData.Currency'].astype(str)                             
	
	    all_training_df_for_transaction_type =[]
	    for key in (list(np.unique(np.array(list(param_df['filter_key'].values))))):
	        all_training_df_for_transaction_type_filter_slice = param_df[param_df['filter_key']==key]
	        if all_training_df_for_transaction_type_filter_slice.empty == False:
	
	            all_training_df_for_transaction_type_filter_slice = all_training_df_for_transaction_type_filter_slice.reset_index()
	            all_training_df_for_transaction_type_filter_slice = all_training_df_for_transaction_type_filter_slice.drop('index', 1)
	
	            all_training_df_for_transaction_type_filter_joined = pd.merge(all_training_df_for_transaction_type_filter_slice, all_training_df_for_transaction_type_filter_slice, on='filter_key')
	            all_training_df_for_transaction_type.append(all_training_df_for_transaction_type_filter_joined)
	    if(len(all_training_df_for_transaction_type) == 0):
	        return(pd.DataFrame())
	    else:
	        return(pd.concat(all_training_df_for_transaction_type))

	def identifying_closed_breaks(self, param_all_meo_df_combination_df = pd.DataFrame(), param_setup_code_crucial = None, param_trans_type_1 = '', param_trans_type_2 = '', param_Transaction_Type_for_closing = ''):

		if(param_setup_code_crucial is None):
			param_setup_code_crucial = self.setup_code
			
	    if(param_all_meo_combination_df.shape[0] != 0):
	        if(param_setup_code_crucial == '125'):
	    
	            Matching_closed_break_df_1 = \
	                param_all_meo_combination_df[ \
	                                            (param_all_meo_combination_df['ViewData.Transaction Type_x'].astype(str).isin([param_trans_type_1])) & \
	                                            (param_all_meo_combination_df['ViewData.Transaction Type_y'].astype(str).isin([param_trans_type_2])) & \
	    #                                         (param_all_meo_combination_df['ViewData.PB_or_Acct_Side_x'].astype(str) == param_all_meo_combination_df['PB_or_Acct_Side_y'].astype(str) == 'Acct_Side') & \
	                                            (abs(param_all_meo_combination_df['ViewData.Net Amount Difference_x']).astype(str) == abs(param_all_meo_combination_df['ViewData.Net Amount Difference_y']).astype(str)) & \
	                                            (param_all_meo_combination_df['ViewData.BreakID_x'].astype(str) != param_all_meo_combination_df['ViewData.BreakID_y'].astype(str)) \
	                                             ]
	            Matching_closed_break_df_2 = \
	                param_all_meo_combination_df[ \
	                                            (param_all_meo_combination_df['ViewData.Transaction Type_x'].astype(str).isin([param_trans_type_2])) & \
	                                            (param_all_meo_combination_df['ViewData.Transaction Type_y'].astype(str).isin([param_trans_type_1])) & \
	    #                                         (param_all_meo_combination_df['ViewData.PB_or_Acct_Side_x'].astype(str) == param_all_meo_combination_df['PB_or_Acct_Side_y'].astype(str) == 'Acct_Side') & \
	                                            (abs(param_all_meo_combination_df['ViewData.Net Amount Difference_x']).astype(str) == abs(param_all_meo_combination_df['ViewData.Net Amount Difference_y']).astype(str)) & \
	                                            (param_all_meo_combination_df['ViewData.BreakID_x'].astype(str) != param_all_meo_combination_df['ViewData.BreakID_y'].astype(str)) \
	                                             ]
	    # Added on 27-12-202 to catch Tran Type = ForwardFX for Mapped Custodian Account values of UBS_UBFX_ON and UBS_UBFX_OP 
	            if((param_trans_type_1 == 'ForwardFX') & (param_all_meo_combination_df['ViewData.Mapped Custodian Account_x'].iloc[0] in ['UBS_UBFX_ON','UBS_UBFX_OP'])):
	                Matching_closed_break_df_forwardfx_UBS_UBFX_ON_OP = \
	                    param_all_meo_combination_df[ \
	                                                (param_all_meo_combination_df['ViewData.Transaction Type_x'].astype(str).isin([param_trans_type_2])) & \
	                                                (param_all_meo_combination_df['ViewData.Transaction Type_y'].astype(str).isin([param_trans_type_1])) & \
	    #                                             ((param_all_meo_combination_df['ViewData.Mapped Custodian Account_x'].astype(str) == param_all_meo_combination_df['ViewData.Mapped Custodian Account_y'].astype(str) == 'UBS_UBFX_ON') | (param_all_meo_combination_df['ViewData.Mapped Custodian Account_x'].astype(str) == param_all_meo_combination_df['ViewData.Mapped Custodian Account_y'].astype(str) == 'UBS_UBFX_OP')) & \
	    #                                             (param_all_meo_combination_df['ViewData.PB_or_Acct_Side_x'].astype(str) == param_all_meo_combination_df['PB_or_Acct_Side_y'].astype(str) == 'Acct_Side') & \
	                                                (abs(param_all_meo_combination_df['ViewData.Net Amount Difference_x']).astype(str) == abs(param_all_meo_combination_df['ViewData.Net Amount Difference_y']).astype(str)) & \
	                                                (param_all_meo_combination_df['ViewData.BreakID_x'].astype(str) != param_all_meo_combination_df['ViewData.BreakID_y'].astype(str)) \
	                                                 ]
	            else:
	                Matching_closed_break_df_forwardfx_UBS_UBFX_ON_OP = pd.DataFrame()
	    
	            closed_df_list = [ \
	                              Matching_closed_break_df_1 \
	                              , \
	                              Matching_closed_break_df_2
	    # Added on 27-12-202 to catch Tran Type = ForwardFX for Mapped Custodian Account values of UBS_UBFX_ON and UBS_UBFX_OP 
	                              , \
	                              Matching_closed_break_df_forwardfx_UBS_UBFX_ON_OP
	    
	                                 ]
	            
		    elif(param_setup_code_crucial == '379'):
		
		        if(param_Transaction_Type_for_closing == 'STIF Interest_non_interacting'):
		            Matching_closed_break_df_1 = \
		                param_all_meo_combination_df[ \
		                                            (param_all_meo_combination_df['Transaction_Type_for_closing_x'].astype(str).isin([param_Transaction_Type_for_closing])) & \
		                                            (param_all_meo_combination_df['Transaction_Type_for_closing_y'].astype(str).isin([param_Transaction_Type_for_closing])) & \
		    #                                         (param_all_meo_combination_df['ViewData.PB_or_Acct_Side_x'].astype(str) == param_all_meo_combination_df['PB_or_Acct_Side_y'].astype(str) == 'Acct_Side') & \
		                                            (abs(param_all_meo_combination_df['ViewData.Net Amount Difference_x']).astype(str) == abs(param_all_meo_combination_df['ViewData.Net Amount Difference_y']).astype(str)) & \
		                                            (param_all_meo_combination_df['ViewData.BreakID_x'].astype(str) != param_all_meo_combination_df['ViewData.BreakID_y'].astype(str)) & \
		                                            (param_all_meo_combination_df['PB_or_Acct_Side_x'].astype(str) == param_all_meo_combination_df['PB_or_Acct_Side_y'].astype(str))]
		        else:
		            Matching_closed_break_df_1 = pd.DataFrame()
		        if(param_Transaction_Type_for_closing == 'Same'):
		            Matching_closed_break_df_2 = \
		                param_all_meo_combination_df[ \
		                                            (param_all_meo_combination_df['Transaction_Type_for_closing_x'].astype(str).isin([param_Transaction_Type_for_closing])) & \
		                                            (param_all_meo_combination_df['Transaction_Type_for_closing_y'].astype(str).isin([param_Transaction_Type_for_closing])) & \
		    #                                         (param_all_meo_combination_df['ViewData.PB_or_Acct_Side_x'].astype(str) == param_all_meo_combination_df['PB_or_Acct_Side_y'].astype(str) == 'Acct_Side') & \
		                                            (abs(param_all_meo_combination_df['ViewData.Net Amount Difference_x']).astype(str) == abs(param_all_meo_combination_df['ViewData.Net Amount Difference_y']).astype(str)) & \
		                                            (param_all_meo_combination_df['ViewData.BreakID_x'].astype(str) != param_all_meo_combination_df['ViewData.BreakID_y'].astype(str)) & \
		                                            (param_all_meo_combination_df['PB_or_Acct_Side_x'].astype(str) == param_all_meo_combination_df['PB_or_Acct_Side_y'].astype(str))]
		        else:
		            Matching_closed_break_df_2 = pd.DataFrame()
		        
		        closed_df_list = [ \
		                          Matching_closed_break_df_1 \
		                          , \
		                          Matching_closed_break_df_2
		                            ]
	            
            Transaction_type_closed_break_df = pd.concat(closed_df_list)

	        if(Transaction_type_closed_break_df.shape[0] != 0):
	            return(Transaction_type_closed_break_df)
	        else:
	            return(pd.DataFrame())
	    else:
	        return(pd.DataFrame())

	def primary_closing_df_list(self, param_setup_code_crucial = None, param_meo_df = pd.DataFrame()):
		if(param_setup_code_crucial is None):
			param_setup_code_crucial = self.setup_code

		fun_setup_code = param_setup_code_crucial
		
		if(param_meo_df.shape[0] != 0):		
			normalized_meo_df = normalize_bp_acct_col_names(param_meo_df)
			meo_for_closed = cleaned_meo(normalized_meo_df)
			
			
			closed_df_list = []
			mapping_dict_trans_type = mapping_dict_trans_type_setup_code.get(param_setup_code_crucial)
			for transaction_type_for_closing_value in mapping_dict_trans_type:
			    if(meo_for_closed[meo_for_closed['Transaction_Type_for_closing'] == transaction_type_for_closing_value].shape[0] != 0):
			        meo_for_transaction_type_for_closing_value_input = meo_for_closed[meo_for_closed['Transaction_Type_for_closing'] == transaction_type_for_closing_value]
			        meo_for_transaction_type_for_closing_value = self.interacting_closing(param_df = meo_for_transaction_type_for_closing_value_input)
			        all_combination_df = self.all_combination_file(param_df = meo_for_transaction_type_for_closing_value)
			        if(all_combination_df.shape[0] != 0):
			            closed_df_for_transaction_type_for_closing_value = self.identifying_closed_breaks(param_all_meo_combination_df = all_combination_df, \
			                                                                     param_setup_code_crucial = fun_setup_code, \
			                                                                     param_trans_type_1 = mapping_dict_trans_type.get(transaction_type_for_closing_value)[0], \
			                                                                     param_trans_type_2 = mapping_dict_trans_type.get(transaction_type_for_closing_value)[1])
			            closed_df_list.append(closed_df_for_transaction_type_for_closing_value)
			        else:
			            closed_df_list.append(pd.DataFrame())
			        del(meo_for_transaction_type_for_closing_value_input)
			        del(meo_for_transaction_type_for_closing_value)
			        del(all_combination_df)
		return(closed_df_list)

	def aggregate_closing_logic(self, param_meo_for_closed, param_Transaction_Type_for_closing_col_name, param_transaction_type_for_closing_value, param_setup_code_crucial = None):
		if(param_setup_code_crucial is None):
			param_setup_code_crucial = self.setup_code

		fun_setup_code = param_setup_code_crucial

	    if(param_meo_for_closed[param_meo_for_closed[param_Transaction_Type_for_closing_col_name] == param_transaction_type_for_closing_value].shape[0] != 0):
	        meo_for_transaction_type_for_closing_value_input = param_meo_for_closed[param_meo_for_closed[param_Transaction_Type_for_closing_col_name] == param_transaction_type_for_closing_value]
	        meo_for_transaction_type_for_closing_value = self.interacting_closing(param_df = meo_for_transaction_type_for_closing_value_input)
	        all_combination_df = self.all_combination_file(param_df = meo_for_transaction_type_for_closing_value)
	        if(all_combination_df.shape[0] != 0):
	            closed_df_for_transaction_type_for_closing_value = self.identifying_closed_breaks(param_all_meo_combination_df = all_combination_df, \
	                                                                     param_setup_code_crucial = fun_setup_code, \
	                                                                     param_trans_type_1 = mapping_dict_trans_type.get(transaction_type_for_closing_value)[0], \
	                                                                     param_trans_type_2 = mapping_dict_trans_type.get(transaction_type_for_closing_value)[1])
	            closed_df_list.append(closed_df_for_transaction_type_for_closing_value)
	        else:
	            closed_df_list.append(pd.DataFrame())
	
def SPEC_Stk_Loan_Jrl_WTH_closing_df_list(self, param_setup_code_crucial = '125', param_meo_df, param_)
if(meo_for_closed[meo_for_closed['Transaction_Type_for_closing_2'] == 'SPEC_Stk_Loan_Jrl_WTH'].shape[0] != 0):
    meo_for_SPEC_Stk_Loan_Jrl_WTH_input = meo_for_closed[meo_for_closed['Transaction_Type_for_closing_2'] == 'SPEC_Stk_Loan_Jrl_WTH'] 
    meo_for_SPEC_Stk_Loan_Jrl_WTH = interacting_closing(meo_for_SPEC_Stk_Loan_Jrl_WTH_input)
    All_combination_df_SPEC_Stk_Loan_Jrl_WTH = All_combination_file(fun_df = meo_for_SPEC_Stk_Loan_Jrl_WTH)
    closed_df_SPEC_Stk_Loan_Jrl_WTH = identifying_closed_breaks(fun_all_meo_combination_df = All_combination_df_SPEC_Stk_Loan_Jrl_WTH, \
                                                             fun_setup_code_crucial = '125', \
                                                             fun_trans_type_1 = 'SPEC Stk Loan Jrl', \
                                                             fun_trans_type_2 = 'WTH')
else:
    closed_df_SPEC_Stk_Loan_Jrl_WTH = pd.DataFrame()
    
closed_df_list.append(closed_df_SPEC_Stk_Loan_Jrl_WTH)
					
		