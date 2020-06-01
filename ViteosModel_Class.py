# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 20:03:23 2020

@author: consultant138
"""

import pandas as pd
import numpy as np
#import os
#import datetime
import matplotlib.pyplot as plt
from tqdm import tqdm
from sklearn import preprocessing
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
#from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix
#from sklearn.metrics import accuracy_score 
from sklearn.metrics import classification_report 

class ViteosModel:
    
    def __init__(self,filepath,new_model_vars_acc = [],new_model_vars_pb = []):
         self.all_cols = list(pd.read_csv(filepath_or_buffer = filepath, nrows =1))
         self.not_required_cols = list(filter(lambda x:x.startswith(('CombiningData','DataSides')), self.all_cols))
         self.df = pd.read_csv(filepath, usecols =lambda column:column not in self.not_required_cols)
         self.df['record_null_count'] = self.df[['MetaData.0._RecordID','MetaData.1._RecordID']].isnull().sum(axis=1)
         self.df['Date'] = pd.to_datetime(self.df['ViewData.Task Business Date']).dt.date

         self.imp_cols = ['MetaData.0._ParentID',
                          'MetaData.1._ParentID',
                          'MetaData.0._RecordID',
                          'MetaData.1._RecordID',
                          'TaskInstanceID', 
                          'ViewData.BreakID',
                          'ViewData.Status',
                          'ViewData._ID','_id',
                          'ViewData.Side0_UniqueIds',
                          'ViewData.Side1_UniqueIds', 
                          'ViewData.Task Business Date',
                          'ViewData.Keys',
                          'Date', 
                          'ViewData.Mapped Custodian Account']
         
         self.accounting_vars = ['ViewData.Accounting Base Net Amount',
                                 'ViewData.Accounting Cancel Amount',
                                 'ViewData.Accounting Transaction Type',
                                 'ViewData.Accounting Cancel Flag',
                                 'ViewData.Accounting Commission',
                                 'ViewData.Accounting Currency',
                                 'ViewData.Accounting ExpiryDate',
                                 'ViewData.Accounting Interest Amount',
                                 'ViewData.Accounting Net Amount',
                                 'ViewData.Accounting Non Trade Description',
                                 'ViewData.Accounting OTE Custodian Account',
                                 'ViewData.Accounting OTE Ticker',
                                 'ViewData.Accounting OTEIncludeFlag',
                                 'ViewData.Accounting Price',
                                 'ViewData.Accounting Principal Amount',
                                 'ViewData.Accounting Quantity',
                                 'ViewData.Accounting Sec Fees',
                                 'ViewData.Accounting Settle Date',
                                 'ViewData.Accounting Strike Price',
                                 'ViewData.Accounting Trade Date',
                                 'ViewData.Accounting Trade Expenses',
                                 'ViewData.BalanceAcc',
                                 'ViewData.BaseBalanceAcc',
                                 'ViewData.Accounting Description',
                                 'ViewData.Accounting CUSIP']
         
        
         self.pb_vars = ['ViewData.B-P Base Net Amount',
                         'ViewData.B-P Cancel Amount',
                         'ViewData.B-P Cancel Flag',
                         'ViewData.B-P Transaction Type',
                         'ViewData.B-P Commission',
                         'ViewData.B-P Currency',
                         'ViewData.B-P ExpiryDate',
                         'ViewData.B-P Interest Amount',
                         'ViewData.B-P Net Amount',
                         'ViewData.B-P Non Trade Description',
                         'ViewData.B-P OTE Custodian Account',
                         'ViewData.B-P OTE Ticker',
                         'ViewData.B-P OTEIncludeFlag',
                         'ViewData.B-P PB Account Numeric',
                         'ViewData.B-P Price',
                         'ViewData.B-P Principal Amount',
                         'ViewData.B-P Quantity',
                         'ViewData.B-P Sec Fees',
                         'ViewData.B-P Settle Date',
                         'ViewData.B-P Strike Price',
                         'ViewData.B-P Trade Date',
                         'ViewData.B-P Trade Expenses',
                         'ViewData.BalancePB',
                         'ViewData.B-P Description',
                         'ViewData.B-P CUSIP']
         
         self.model_cols = ['ViewData.Accounting Base Net Amount',
                            'ViewData.Accounting Cancel Amount',
                            'ViewData.Accounting Cancel Flag',
                            'ViewData.Accounting Commission', 
                            'ViewData.Accounting Transaction Type',
                            'ViewData.Accounting Net Amount',
                            'ViewData.Accounting Price',
                            'ViewData.Accounting Quantity',
                            'ViewData.Accounting Currency',
                            'ViewData.Accounting Transaction Type',
                            'ViewData.B-P Cancel Amount',
                            'ViewData.B-P Net Amount',
                            'ViewData.B-P Price',
                            'ViewData.B-P Quantity',
                            'ViewData.B-P Transaction Type',
                            'ViewData.BalanceAcc',
                            'ViewData.BalancePB',
                            'ViewData.BaseBalanceAcc',
                            'ViewData.B-P Currency',
                            'ViewData.B-P Transaction Type',
                            'label',
                            'ViewData.Accounting CUSIP',
                            'ViewData.B-P CUSIP'
                            ]
#         self.df['ViewData.Accounting CUSIP'] = self.df['ViewData.Accounting CUSIP'].fillna('Missing')
#         self.df['ViewData.B-P CUSIP'] = self.df['ViewData.B-P CUSIP'].fillna('Missing')
         
         self.accounting_vars = list(set(self.accounting_vars).union(set(new_model_vars_acc)))
         self.pb_vars = list(set(self.pb_vars).union(set(new_model_vars_pb)))
         self.model_cols = list(set(self.model_cols).union(set(new_model_vars_acc),set(new_model_vars_pb)))
         self.required_cols = self.imp_cols + self.accounting_vars + self.pb_vars
         
    def fun_reset_index(self,df):
        df = df.reset_index()
        df = df.drop('index',1)
        return df
    
    def fun_remove_rows(self, ViewData_Status_remove_list, MatchStatus_remove_list):
#        Remove all columns with NaN values in ViewData
        self.df = self.df.dropna(subset=list(filter(lambda x:x.startswith(('ViewData')), self.df.columns)),how='all')
#        Remove rows with values in list ViewData_Status_remove_list
        self.df = self.df[~self.df['ViewData.Status'].isin(ViewData_Status_remove_list)]
#        Remove rows with values in list fun_ViewData_MatchStatus_remove_list
        self.df = self.df[~self.df['MatchStatus'].isin(MatchStatus_remove_list)]
        self.df = self.df[~self.df['ViewData._ID'].isnull()]
        
#    def fun_find_either_or_all_not_null_Record_ID_df(self):
#        self.MetaData_0_or_1_RecordID_not_null = self.df.loc[(self.df['MetaData.0._RecordID'].notnull()) | (self.df['MetaData.1._RecordID'].notnull()),['MetaData.0._ParentID','MetaData.1._ParentID','MetaData.0._RecordID','MetaData.1._RecordID','ViewData.BreakID','ViewData.Status','ViewData._ID','Task Business Date']]
#        self.MetaData_0_and_1_RecordID_not_null = self.df.loc[(self.df['MetaData.0._RecordID'].notnull()) & (self.df['MetaData.1._RecordID'].notnull()),['MetaData.0._ParentID','MetaData.1._ParentID','MetaData.0._RecordID','MetaData.1._RecordID','ViewData.BreakID','ViewData.Status','ViewData._ID','Task Business Date']]
#        self.Either_or_all_not_null_Record_ID_df = self.MetaData_0_or_1_RecordID_not_null.append(self.MetaData_0_and_1_RecordID_not_null,ignore_index = True)
    
    def fun_make_aua_meo_df(self):
        self.meo = self.df[(self.df['record_null_count']<2)][self.required_cols]
        self.aua = self.df[(self.df['record_null_count'] == 2)][self.required_cols]
        
    def training(self, test_size_val = 0.30, rf_n_estimators_val = 1000,rf_bootstrap_bool_val = True, rf_max_features_val = 'sqrt', rf_max_depth_val = None):
        self.all_day_df = self.df[self.required_cols]
        self.all_day_break_df = self.all_day_df[self.all_day_df['ViewData.Status'].isin(['OB','SPM'])]
        self.all_day_break_df = self.fun_reset_index(df = self.all_day_break_df)
        self.all_day_break_df.loc[self.all_day_break_df['ViewData.Side1_UniqueIds'].isnull(),'Trans_side'] = 'B_side'
        self.all_day_break_df.loc[self.all_day_break_df['ViewData.Side0_UniqueIds'].isnull(),'Trans_side'] = 'A_side'
        
        self.a_side_df = self.all_day_break_df[self.all_day_break_df['Trans_side']=='A_side'][self.imp_cols + self.pb_vars]
        self.b_side_df = self.all_day_break_df[self.all_day_break_df['Trans_side']=='B_side'][self.imp_cols + self.accounting_vars]
    
        self.a_side_df['filter_key'] = self.a_side_df['ViewData.Mapped Custodian Account'].astype(str) + self.a_side_df['ViewData.B-P Currency'].astype(str)
        self.b_side_df['filter_key'] = self.b_side_df['ViewData.Mapped Custodian Account'].astype(str) + self.b_side_df['ViewData.Accounting Currency'].astype(str)
        
        self.a_side_df = self.fun_reset_index(df = self.a_side_df)
        self.b_side_df = self.fun_reset_index(df = self.b_side_df)
        
        self.pool =[]
        self.key_index =[]
        self.training_df =[]
        for d in self.a_side_df['Date'].unique():
            self.a_side_df_d = self.a_side_df[self.a_side_df['Date']==d]
            self.b_side_df_d = self.b_side_df[self.b_side_df['Date']==d]
    
            self.a_side_df_d = self.fun_reset_index(df = self.a_side_df_d)
            self.b_side_df_d = self.fun_reset_index(df = self.b_side_df_d)
                
            for i, key in tqdm(enumerate(self.a_side_df_d['filter_key'])):
                if self.b_side_df_d[self.b_side_df_d['filter_key']==key].empty == False:
                    self.pool.append(self.b_side_df_d[self.b_side_df_d['filter_key']==key].index)

                    repeat_num = self.b_side_df_d.loc[self.pool[len(self.pool)-1],:][self.accounting_vars].shape[0]
            
                    self.a_side_df_new = pd.concat([self.a_side_df_d[self.a_side_df_d.index==i]]*repeat_num, ignore_index=True)
                    
                    self.b_side_df_new = self.b_side_df_d.loc[self.pool[len(self.pool)-1],:][self.accounting_vars]
                    self.b_side_df_new = self.fun_reset_index(df = self.b_side_df_new)

#                    dff = pd.concat([aa[aa.index==i],bb.loc[pool[i],:][accounting_vars]],axis=1)
                    self.concat_a_b_df = pd.concat([self.a_side_df_new,self.b_side_df_new],axis=1)
                    self.training_df.append(self.concat_a_b_df)
                    self.key_index.append(i)
        
#                    else:
#                    print(i) 
        self.training_df = pd.concat(self.training_df)
        self.training_df = self.fun_reset_index(df = self.training_df)
        self.all_day_recon_df = self.all_day_df[self.all_day_df['ViewData.Status'].isin(['UMR','SMR'])]
        self.all_day_recon_df = self.fun_reset_index(df = self.all_day_recon_df)
        self.train_df = pd.concat([self.training_df, self.all_day_recon_df], axis=0)
        self.train_df = self.fun_reset_index(df = self.train_df)
        self.train_df['label'] = self.train_df.apply(lambda x: 'Pair' if x['ViewData.Status'] in ['SMR'] else('UMR' if x['ViewData.Status'] in ['UMR'] else 'No-Pair'), axis=1)
        
        print('train_df ViewData.Accounting CUSIP value_counts() before encoding')
        print(self.train_df['ViewData.Accounting CUSIP'].value_counts())
        
        self.train_df['ViewData.Accounting CUSIP'] = self.train_df['ViewData.Accounting CUSIP'].astype(str)
        self.train_df['ViewData.B-P CUSIP'] = self.train_df['ViewData.B-P CUSIP'].astype(str)
        
        le = preprocessing.LabelEncoder()
        le.fit(self.train_df['ViewData.Accounting Transaction Type'])
        self.train_df['ViewData.Accounting Transaction Type'] = le.transform(self.train_df['ViewData.Accounting Transaction Type'])

        le = preprocessing.LabelEncoder()
        le.fit(self.train_df['ViewData.Accounting CUSIP'])
        self.train_df['ViewData.Accounting CUSIP'] = le.transform(self.train_df['ViewData.Accounting CUSIP'])

        print('train_df ViewData.Accounting CUSIP value_counts() after encoding')
        print(self.train_df['ViewData.Accounting CUSIP'].value_counts())
#        le_acc = preprocessing.LabelEncoder()
#        le_acc.fit(self.train_df['ViewData.Accounting Description'])
#        self.train_df['ViewData.Accounting Description'] = le_acc.transform(self.train_df['ViewData.Accounting Description'])


        le = preprocessing.LabelEncoder()
        le.fit(self.train_df['ViewData.B-P Transaction Type'])
        self.train_df['ViewData.B-P Transaction Type'] = le.transform(self.train_df['ViewData.B-P Transaction Type'])

        le = preprocessing.LabelEncoder()
        le.fit(self.train_df['ViewData.B-P CUSIP'])
        self.train_df['ViewData.B-P CUSIP'] = le.transform(self.train_df['ViewData.B-P CUSIP'])

        
#        le_pb = preprocessing.LabelEncoder()
#        le_pb.fit(self.train_df['ViewData.B-P Description'])
#        self.train_df['ViewData.B-P Description'] = le_pb.transform(self.train_df['ViewData.B-P Description'])
        
        curr_dict = {'USD':0, 'EUR':1,'GBP':2,'CAD':3,'ZAR':4}
        self.train_df['ViewData.B-P Currency'] = self.train_df['ViewData.B-P Currency'].map(curr_dict)
        self.train_df['ViewData.Accounting Currency'] = self.train_df['ViewData.Accounting Currency'].map(curr_dict)
        
        X_train, X_test, y_train, y_test = train_test_split(self.train_df[self.model_cols].drop((['label']),axis=1), 
                                                            self.train_df[self.model_cols]['label'], test_size = test_size_val, 
                                                            random_state=101)
        
        X_train = X_train.fillna(0)
        X_test = X_test.fillna(0)
        
        model = RandomForestClassifier(n_estimators = rf_n_estimators_val, 
                                       bootstrap = rf_bootstrap_bool_val,
                                       max_features = rf_max_features_val, max_depth = rf_max_depth_val)
#         Fit on training data
        model.fit(X_train, y_train)
#         Actual class predictions
        rf_predictions = model.predict(X_test)
#       Probabilities for each class
        rf_probs = model.predict_proba(X_test)[:, 1]
        confusion_matrix(y_test, rf_predictions)
        print(classification_report(y_test, rf_predictions))
#        print(pd.DataFrame(rf_probs)[0].hist())
        features = self.model_cols
        importances = model.feature_importances_
        indices = np.argsort(importances)

        plt.title('Feature Importances')
        plt.barh(range(len(indices)), importances[indices], color='b', align='center')
        plt.yticks(range(len(indices)), [features[i] for i in indices])
        plt.xlabel('Relative Importance')
        plt.show()
        


if __name__ == '__main__':
    print("Control object without new_model_vars")
    weiss_123_control = ViteosModel(filepath= "//vitblrdevcons01/Raman  Strategy ML 2.0/ReconDB.HST_RecData_123.csv",new_model_vars_acc = [],new_model_vars_pb = [])
    weiss_123_control.fun_remove_rows(ViewData_Status_remove_list=['HST', 'OC', 'CT'],MatchStatus_remove_list=[21])
    weiss_123_control.fun_make_aua_meo_df()
    weiss_123_control.training()
    
#    print("First test object")
#    weiss_123_test1 = ViteosModel(filepath= "//vitblrdevcons01/Raman  Strategy ML 2.0/ReconDB.HST_RecData_123.csv",new_model_vars_acc = ['ViewData.Accounting Description'],new_model_vars_pb = ['ViewData.B-P Description'])
#    weiss_123_test1.fun_remove_rows(ViewData_Status_remove_list=['HST', 'OC', 'CT'],MatchStatus_remove_list=[21])
#    weiss_123_test1.fun_make_aua_meo_df()
#    weiss_123_test1.training()
    
#    print("Second test object")
#    weiss_123_test2 = ViteosModel(filepath= "//vitblrdevcons01/Raman  Strategy ML 2.0/ReconDB.HST_RecData_123.csv",new_model_vars_acc = ['ViewData.Accounting Description','ViewData.Description'],new_model_vars_pb = ['ViewData.B-P Description'])
#    weiss_123_test2.fun_remove_rows(ViewData_Status_remove_list=['HST', 'OC', 'CT'],MatchStatus_remove_list=[21])
#    weiss_123_test2.fun_make_aua_meo_df()
#    weiss_123_test2.training()
    
