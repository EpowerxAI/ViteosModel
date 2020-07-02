# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 20:03:23 2020

@author: consultant138
"""

import pandas as pd
#import numpy as np
#import os
#import datetime
#import matplotlib.pyplot as plt
from tqdm import tqdm
#from sklearn import preprocessing
from sklearn.model_selection import train_test_split
#from sklearn.ensemble import RandomForestClassifier
#from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix
#from sklearn.metrics import accuracy_score 
from sklearn.metrics import classification_report 
from ViteosMongoDB import ViteosMongoDB_Class as mngdb_cl
from ViteosDecorator import logging_decorator
from catboost import CatBoostClassifier

class ViteosModel_Class:

        @logging_decorator  
        def __init__(self, param_without_ssh = True):
            
                mngdb_obj = mngdb_cl()
                print("ViteosMongoDB instance created")
                
                mngdb_obj.connect_with_or_without_ssh()
                print("ViteosMongoDB instance connection being established")

                mngdb_obj.df_to_evaluate()
                print("ViteosMongoDB instance dataframe to evaluate function invoked")

                mngdb_obj.make_df()
                print("ViteosMongoDB columns being renamed to begin with ViewData")

                print("ViteosMongoDB instance dataframe ready to be consumed by the ViteosModel class")
	
                print("The shape of the dataframe to be evalued for modelling is \n")
                mngdb_obj.df_main.shape
                
                self.df = mngdb_obj.df_main

                self.common_cols = ['ViewData.Accounting Net Amount', 'ViewData.Age',
                               'ViewData.Age WK', 'ViewData.Asset Type Category',
                               'ViewData.B-P Net Amount', 'ViewData.Base Net Amount','ViewData.CUSIP', 
                               'ViewData.Cancel Amount',
                               'ViewData.Cancel Flag',
                               'ViewData.Commission', 'ViewData.Currency', 'ViewData.Custodian',
                               'ViewData.Custodian Account',
                               'ViewData.Description', 'ViewData.ExpiryDate', 'ViewData.Fund',
                               'ViewData.ISIN',
                               'ViewData.Investment Type',
                               #'ViewData.Keys',
                               'ViewData.Mapped Custodian Account',
                               'ViewData.Net Amount Difference',
                               'ViewData.Net Amount Difference Absolute', 'ViewData.OTE Ticker',
                               'ViewData.Price',
                               'ViewData.Prime Broker', 'ViewData.Quantity',
                               'ViewData.SEDOL', 'ViewData.SPM ID', 'ViewData.Settle Date',
                               'ViewData.Strike Price','Date',
                               'ViewData.Ticker', 'ViewData.Trade Date',
                               'ViewData.Transaction Category',
                               'ViewData.Transaction Type', 'ViewData.Underlying Cusip',
                               'ViewData.Underlying ISIN',
                               'ViewData.Underlying Sedol','filter_key','ViewData.Status','ViewData.BreakID',
                               'ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']

                self.model_cols = ['SideA.ViewData.Accounting Net Amount', 
                               'SideA.ViewData.B-P Net Amount', 
                               'SideA.ViewData.CUSIP', 
                               'SideA.ViewData.Currency', 
                               'SideA.ViewData.Description',
                               'SideA.ViewData.ISIN', 
                               'SideB.ViewData.Accounting Net Amount',
                               'SideB.ViewData.B-P Net Amount',
                               'SideB.ViewData.CUSIP',
                               'SideB.ViewData.Currency',
                               'SideB.ViewData.Description', 
                               'SideB.ViewData.ISIN',
                               'SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 
                               'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','label']


                self.y_col = ['label'] 
                
                self.cat_features = ['SideA.ViewData.CUSIP',
                               'SideA.ViewData.Currency',
                               'SideA.ViewData.Description',
                               'SideA.ViewData.ISIN',
                               'SideB.ViewData.CUSIP',
                               'SideB.ViewData.Currency',
                               'SideB.ViewData.Description',
                               'SideB.ViewData.ISIN']
        
        @logging_decorator        
        def fun_reset_index(self,df):
                df = df.reset_index()
                df = df.drop('index',1)
                return df
        
        @logging_decorator
        def add_Date_col(self,df):
                df['Date'] = pd.to_datetime(df['ViewData.Task Business Date'])
#                df_125['Date'] = pd.to_datetime(df_125['ViewData.Task Business Date'])
                df = df[~df['Date'].isnull()]
                df = self.fun_reset_index(df)
                df['Date'] = pd.to_datetime(df['Date']).dt.date 
                df['Date'] = df['Date'].astype(str)
                return df
        
        @logging_decorator        
        def change_side_col_type(self,df):
                df['ViewData.Side0_UniqueIds'] = df['ViewData.Side0_UniqueIds'].astype(str)
                df['ViewData.Side1_UniqueIds'] = df['ViewData.Side1_UniqueIds'].astype(str)
                return df

        @logging_decorator        
        def nan_to_zero_flag_side_column(self,df):
                df.loc[df['ViewData.Side0_UniqueIds']=='nan','flag_side0'] = 0 
                df.loc[df['ViewData.Side1_UniqueIds']=='nan','flag_side1'] = 0
                return df

        @logging_decorator        
        def add_flag_side_0_1_columns(self,df):
                df['flag_side0'] = df.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
                df['flag_side1'] = df.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)
                return df
        
        @logging_decorator                                    
        def add_A_B_Side_values_to_Trans_Side_col(self,df):
                df.loc[df['ViewData.Side1_UniqueIds']=='nan','Trans_side'] = 'B_side'
                df.loc[df['ViewData.Side0_UniqueIds']=='nan','Trans_side'] = 'A_side'
                return df

        @logging_decorator            
        def change_A_B_Side_currency_columns(self,df):
                df.loc[df['Trans_side']=='A_side','ViewData.B-P Currency'] = df.loc[df['Trans_side']=='A_side','ViewData.Currency']
                df.loc[df['Trans_side']=='B_side','ViewData.Accounting Currency'] = df.loc[df['Trans_side']=='B_side','ViewData.Currency'] 
                return df

        @logging_decorator            
        def add_new_cols(self,df):
                self.df['record_null_count'] = self.df[['MetaData.0._RecordID','MetaData.1._RecordID']].isnull().sum(axis=1)

                self.df = self.add_Date_col(self.df)                
                
                self.df = self.change_side_col_type(self.df)
                
                self.df = self.add_flag_side_0_1_columns(self.df)
                
                self.df = self.nan_to_zero_flag_side_column(self.df) 

                self.df = self.add_A_B_Side_values_to_Trans_Side_col(self.df)
                
                self.df = self.change_A_B_Side_currency_columns(self.df)

        @logging_decorator                
        def choose_unique_OBs(self,df):
                df = df.sort_values(['ViewData.BreakID','Date'], ascending = [True,False])  
                df = self.fun_reset_index(df)
                df['OB_unique_flag'] = df.groupby('ViewData.BreakID').cumcount()
                df = df[df['OB_unique_flag'] == 0]
                df = self.fun_reset_index(df)                
                return df

        @logging_decorator          
        def no_pair_df(self, param_no_pair_status_to_choose = ['OB'], param_sample_fraction = 0.30):
                self.no_pair_df = self.df[(self.df['flag_side0']<=1) & (self.df['flag_side1']<=1) & (self.df['ViewData.Status'].isin(param_no_pair_status_to_choose))]
                
                self.no_pair_df = self.fun_reset_index(self.no_pair_df)
                
                self.no_pair_df['ViewData.BreakID'] = self.no_pair_df['ViewData.BreakID'].astype(int)                
              
                self.no_pair_df = self.no_pair_df[self.no_pair_df['ViewData.BreakID'] != -1] 
                
                self.no_pair_df = self.fun_reset_index(self.no_pair_df)
                
                self.no_pair_df = self.choose_unique_OBs(self.no_pair_df)
                
                self.no_pair_df = self.no_pair_df.sample(param_sample_fraction)

        @logging_decorator                
        def add_filter_key_column(self, df, param_currency_column_name):
                df['filter_key'] = df['ViewData.Mapped Custodian Account'].astype(str) + df[param_currency_column_name].astype(str)
                df = self.fun_reset_index(df)
                return df

        @logging_decorator                
        def aa_bb(self):
                self.aa = self.no_pair_df[self.no_pair_df['Trans_side'] == 'A_Side'] 
                self.bb = self.no_pair_df[self.no_pair_df['Trans_side'] == 'B_Side'] 
                
                self.aa = self.add_filter_key_column(df = self.aa, param_currency_column_name = 'ViewData.B-P Currency')
                self.bb = self.add_filter_key_column(df = self.bb, param_currency_column_name = 'ViewData.Accounting Currency')

        @logging_decorator           
        def train_test_loop(self):
                pool = []
                key_index =[]
                training_df =[]
                
                for d in tqdm(self.aa['Date'].unique()):
                    aa1 = self.aa[self.aa['Date']==d,:][self.common_cols]
                    aa1 = self.fun_reset_index(aa1)
                    bb1 = self.bb[self.bb['Date']==d,:][self.common_cols]
                    bb1 = self.fun_reset_index(bb1)
                    
                    bb1 = bb1.sort_values(by = 'filter_key', ascending = True)
                    
                    for i, key in enumerate(aa1['filter_key'].values):
                        if bb1.loc[bb1['filter_key']==key,:].empty == False:
            
                            pool.append(bb1[bb1['filter_key']==key].head(1).index)
                            
                            repeat_num = bb1.loc[pool[len(pool)-1],:].shape[0]
            
                            aa_df = pd.concat([aa1[aa1.index==i]]*repeat_num, ignore_index=True)
                            bb_df = bb1.loc[pool[len(pool)-1],:][self.common_cols].reset_index()
                            bb_df = bb_df.drop('index', 1)
            
                            aa_df = aa_df.rename(columns={'ViewData.BreakID':'ViewData.BreakID_A_side'})
                            bb_df = bb_df.rename(columns={'ViewData.BreakID':'ViewData.BreakID_B_side'})

                            #dff = pd.concat([aa[aa.index==i],bb.loc[pool[i],:][accounting_vars]],axis=1)
            
                            aa_df = self.fun_reset_index(aa_df)
            
                            aa_df.columns = ['SideA.' + x for x in aa_df.columns] 
                            bb_df.columns = ['SideB.' + x for x in bb_df.columns]
            
                            dff = pd.concat([aa_df,bb_df],axis=1)
                            training_df.append(dff)
                            key_index.append(i)
        
                        #else:
                            #print(i)            
                
                self.training_df1 = pd.concat(training_df)
                
                self.training_df1['SideB.ViewData.BreakID_B_side'] = self.training_df1['SideB.ViewData.BreakID_B_side'].astype('int64')
                self.training_df1['SideA.ViewData.BreakID_A_side'] = self.training_df1['SideA.ViewData.BreakID_A_side'].astype('int64')                

        @logging_decorator        
        def add_umr_umb_df(self, base_df, param_status):
                created_df = base_df[base_df['ViewData.Status'].isin([param_status])]

                created_df = created_df[(created_df['flag_side0']==1) & (created_df['flag_side1']==1)]
                created_df = self.fun_reset_index(created_df)
                return created_df

        @logging_decorator
        def add_filter_key_to_df(self):
                self.df['filter_key'] = self.df['ViewData.Mapped Custodian Account'].astype(str) + self.df['ViewData.B-P Currency'].astype(str)

        @logging_decorator
        def umr_umb_loop(self, base_df, umr_umb_df, param_label_value_to_add):
                loop_df = []

                for i in range(len(umr_umb_df)):
                    side1 = umr_umb_df.loc[i, 'ViewData.Side0_UniqueIds']
                    side2 = umr_umb_df.loc[i, 'ViewData.Side1_UniqueIds']
                    first_record_side1 = base_df[base_df['ViewData.Side0_UniqueIds']==side1][self.common_cols].head(1)
                    first_record_side2 = base_df[base_df['ViewData.Side1_UniqueIds']==side2][self.common_cols].head(1)
    
                    first_record_side1 = first_record_side1.rename(columns={'ViewData.BreakID':'ViewData.BreakID_A_side'})
                    first_record_side2 = first_record_side2.rename(columns={'ViewData.BreakID':'ViewData.BreakID_B_side'})
    
                    first_record_side1 = self.fun_reset_index(df = first_record_side1)
                    first_record_side2 = self.fun_reset_index(df = first_record_side2)
    
                    first_record_side1.columns = ['SideA.' + x for x in first_record_side1.columns] 
                    first_record_side2.columns = ['SideB.' + x for x in first_record_side2.columns]
            
                    umr_umb_df_new = pd.concat([first_record_side1,first_record_side2],axis=1)
                    loop_df.append(umr_umb_df_new)
                    
                
                    #key_index.append(i)
                loop_df_final = pd.concat(loop_df)
                loop_df_final = self.fun_reset_index(loop_df_final)
                
                return loop_df_final

        @logging_decorator            
        def process_umr(self):

                umr_all_day = self.add_umr_umb_df(base_df = self.df, param_status = 'UMR')
                self.umr_df_final = self.umr_umb_loop(base_df = self.df, umr_umb_df = umr_all_day, param_label_value_to_add = 'UMR')      

        @logging_decorator
        def process_umb(self):

                umb_all_day = self.add_umr_umb_df(base_df = self.df, param_status = 'UMB')
                self.umb_df_final = self.umr_umb_loop(base_df = self.df, umr_umb_df = umb_all_day, param_label_value_to_add = 'UMB')      

        @logging_decorator        
        def add_label_before_training_final_df(self):
                self.umr_df_final['label'] = 'UMR'
                self.umb_df_final['label'] = 'UMB'
                self.training_df1['label'] = 'No-Pair'

        @logging_decorator                
        def process_train_full_df(self):
                self.train_full = pd.concat([self.training_df1, self.umr_df_final, self.umb_df_final], axis = 0)
                self.train_full = self.fun_reset_index(self.train_full)
                
                self.umr_unique = self.train_full[self.train_full['label'].isin(['UMR'])][['SideA.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.Side0_UniqueIds','SideB.ViewData.Side1_UniqueIds']]
                self.umr_unique = self.fun_reset_index(self.umr_unique)
                
                self.train_full['duplicate_flag'] = 0
                
                for i in range(len(self.umr_unique)):
                    side1 = self.umr_unique.loc[i, 'SideA.ViewData.Side0_UniqueIds']
                    side2 = self.umr_unique.loc[i, 'SideB.ViewData.Side1_UniqueIds']
                    self.train_full.loc[((self.train_full['SideA.ViewData.Side0_UniqueIds']==side1) & (self.train_full['SideB.ViewData.Side1_UniqueIds']==side2) & (~self.train_full['label'].isin(['UMR']))),'duplicate_flag']=1
                
                self.train_full = self.train_full[self.train_full['duplicate_flag'] == 0]
                self.train_full = self.fun_reset_index(self.train_full)
                
         ## ONE TO MANY ##

        @logging_decorator                
        def process_one_to_many_with_loop(self):
                one_to_many = self.df[(self.df['flag_side0']==1) & (self.df['flag_side1']>1) & (self.df['ViewData.Status']!='SMR')]
                #self.one_to_many[self.one_to_many['ViewData.Side0_UniqueIds'] !='nan']['ViewData.Status'].value_counts()
                #comb = one_to_many[one_to_many['ViewData.Side0_UniqueIds'] =='nan']
                comb_and_match = one_to_many[one_to_many['ViewData.Side0_UniqueIds'] !='nan']

                otm_pool =[]
                for i,val in enumerate(comb_and_match['ViewData.Side0_UniqueIds'].unique()):
    
                    if self.df[self.df['ViewData.Side0_UniqueIds'] ==val].empty ==False:

                        acc_side = self.df[self.df['ViewData.Side0_UniqueIds'] ==val].tail(1)[self.common_cols]
                        #acc_side = acc_side.drop('ViewData.Side1_UniqueIds',1)
                        acc_side = self.fun_reset_index(acc_side)
                        acc_side = acc_side.rename(columns={'ViewData.BreakID':'ViewData.BreakID_A_side'})
                        acc_side.columns = ['SideA.' + x for x in acc_side.columns] 


                        #print(val)
                        #print(acc_side)
                        for j in comb_and_match['ViewData.Side1_UniqueIds'].values[i].split(','):

                            pb_side = self.df[self.df['ViewData.Side1_UniqueIds'] ==j].tail(1)[self.common_cols]
                            pb_side = self.fun_reset_index(pb_side)
                            pb_side = pb_side.rename(columns={'ViewData.BreakID':'ViewData.BreakID_B_side'})
                            pb_side.columns = ['SideB.' + x for x in pb_side.columns]

                            final_data = pd.concat([acc_side,pb_side], axis=1)
                            final_data['SideB.ViewData.Side1_UniqueIds']= j
                            otm_pool.append(final_data)

                self.full_otm_data = pd.concat(otm_pool,axis=0)
                self.full_otm_data['label'] ='Partial_match'

        @logging_decorator
        def process_train_full_new(self, param_no_pair_frac = 0.50):
                self.train_full_new = pd.concat([self.train_full,self.full_otm_data], axis = 0)
                self.train_full_new = self.fun_reset_index(self.train_full_new)
                no_pair = self.train_full_new[self.train_full_new['label'] == 'No-Pair']
                rest = self.train_full_new[self.train_full_new['label'] != 'No-Pair']
                self.train_full_new = pd.concat([no_pair,rest])
                self.train_full_new = self.fun_reset_index(self.train_full_new)

        @logging_decorator
        def make_X_y_train_test(self, param_test_size_fraction = 0.30):
                self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(self.train_full_new[self.model_cols].drop((['label']),axis=1), 
                                                                    self.train_full_new[self.model_cols]['label'], test_size = param_test_size_fraction, 
                                                                    random_state=88)
                self.X_train = self.X_train.fillna(0)
                self.X_test = self.X_test.fillna(0)

        @logging_decorator                
        def modelling(self, param_n_estimators = 1000, param_bootstrap_value = True, param_max_features = 'sqrt'):        
                #model = RandomForestClassifier(n_estimators = param_n_estimators, 
                #                               bootstrap = param_bootstrap_value,
                #                               max_features = param_max_features, max_depth=None)
                #                               #min_samples_leaf= 5, min_samples_split = 12)

                SEED =88

                params ={'loss_function' : 'MultiClass',
                        'eval_metric' : 'AUC',
                        'learning_rate':0.055,
                        'iterations':1000,
                        'depth':5,
                        'random_seed':SEED,
                        'od_type':'Iter',
                        'od_wait':200,
                        'cat_features':self.cat_features,
                        'task_type':'CPU'}

                clf = CatBoostClassifier(**params)

                clf.fit(self.X_train.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side'],1), 
                        self.y_train,eval_set=(self.X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side'],1), self.y_test),
                        use_best_model=True,plot=True)
                # Actual class predictions
                rf_predictions = clf.predict(self.X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side'],1))
                # Probabilities for each class
                rf_probs = clf.predict_proba(self.X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side'],1))[:, 1]
                print('Confusion Matrix')
                print(confusion_matrix(self.y_test,rf_predictions))
                
                probability_class_0 = clf.predict_proba(self.X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side'],1))[:, 0]
                probability_class_1 = clf.predict_proba(self.X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side'],1))[:, 1]                

                print('Classification Report')
                print(classification_report(self.y_test, rf_predictions))
                
                self.X_test['Predicted_action'] = rf_predictions
                #self.X_test['Predicted_action_probabilty'] = rf_probs
                self.X_test['probability_No_pair'] = probability_class_0
                self.X_test['probability_UMR'] = probability_class_1
                #X_test['probability_UMR'] = probability_class_2
                
                
#if __name__ == '__main__':    
#        obj = ViteosModel_Class()
#        
#
#        obj.add_filter_key_to_df()
#        obj.process_umr()
#        obj.process_umb()          
#        obj.add_label_before_training_final_df()        
#        obj.process_train_full_df()
#        obj.process_one_to_many_with_loop()
        
                