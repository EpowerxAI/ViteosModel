# -*- coding: utf-8 -*-
"""
Created on Thu Apr 26 12:40:58 2018

@author: shweta
"""

# import libraries
import pandas as pd
import sys
pd.options.display.max_rows = 9999
import numpy as np
import h2o
from h2o.estimators.gbm import H2OGradientBoostingEstimator
import csv
import databaseconfig as cfg
import os
import datetime
import ipdb
#========================= Take inputs ========================================

FIELDS_TO_IGNORE_FILE = "./ReconML_FieldsToIgnore_withColType.xlsx"

def print_usage():
    print('Script to train a Cash recon model for a specific client')
    print('\nUsage: python3 {} {} {}\n'.format(sys.argv[0],  '<ClientShortCode>', '<model_training_inputfile>'))
    print('<ClientShortCode>\t: ClientShortCode of the client')
    print('<model_training_inputfile>\t: Full path of "Matching Engine Output" inputfile')


args = sys.argv[1:]
print('Arguments passed in: {}'.format(args))
if len(args) != 2:
    print_usage()
    sys.exit(1)

csc = args[0]   # ClientShortCode
model_training_inputfile = args[1]
model_save_path = "./"

df = pd.read_csv(model_training_inputfile, low_memory=False)
df.rename(columns = {'predict':'training_label'}, inplace=True)

# ipdb.set_trace()

# Remove columns which are not required
df = df.drop(df.columns[df.columns.str.startswith(('_','Unnamed', 'InternalComment'))], axis=1)
df = df.drop(df.columns[df.columns.str.endswith(('Date'))], axis=1)
df = df.drop(df.columns[df.columns.str.contains(('Color|color'))], axis=1)
df = df.drop(df.columns[df.columns.str.contains('ID|Id|id')], axis=1)
df = df.drop(df.columns[df.columns.str.endswith(('SEDOL'))], axis=1)
df = df.drop(df.columns[df.columns.str.endswith(('ISIN'))], axis=1)
df = df.drop(df.columns[df.columns.str.endswith(('Sedol'))], axis=1)
df = df.drop(df.columns[df.columns.str.endswith(('Cusip'))], axis=1)
#df = df.drop(df.columns[df.columns.str.endswith(('CUSIP'))], axis=1)
df = df.drop(df.columns[df.columns.str.endswith(('Ticker'))], axis=1)
df = df.drop(df.columns[df.columns.str.contains('custom')], axis=1)
df = df.drop(df.columns[df.columns.str.contains('Custom')], axis=1)

# Remove columns as specified by Ushanas
colData = pd.read_excel(FIELDS_TO_IGNORE_FILE, sheet_name='Cash')
cols_to_ignore = colData.loc[colData['Ignore for Analysis'] == 'Y', 'Field Name']
cols_to_ignore = cols_to_ignore.tolist()
df.drop(cols_to_ignore, axis=1, inplace=True, errors='ignore')  # ignore errors, as some columns may have


# Remove rows with Status as [] and source combination as UNMAPPED
status_to_ignore = ['Archive', 'HST', 'OC',np.NaN]
df = df[~df.Status.isin(status_to_ignore)]
df = df[df['Source Combination'] != 'UNMAPPED']
df = df.dropna(subset=['training_label'])

# Remove rows having a 'training_label' with value_counts < 3 
#   (otherwise, train/valid/test split creation does not work)
vc = df.training_label.value_counts()
status_to_keep = vc[vc >= 3].index.tolist()
df = df[df.training_label.isin(status_to_keep)]

# Filter rows for Client
if csc != "All":
    df=df[df["ClientShortCode"] == csc]

# reset index as many rows have been dropped
df.reset_index(drop=True, inplace=True)



# Change datatype of columns ending with "Absolute" and having DataType as String to bool
colData=pd.read_excel(FIELDS_TO_IGNORE_FILE, sheet_name='Cash')
lst1 = colData[(colData["Field Name"].str.endswith("Absolute")) & (colData["DataType"] == "String")]["Field Name"].tolist()

for i in range(colData.shape[0]) :
    fname=colData["Field Name"].iat[i]
    if fname in (lst1) :
        colData.loc[(colData["Field Name"] == fname),"DataType"] = "Bool"


# Change datatype of columns ending with "Difference" and having DataType as String to bool
lst1 = colData[(colData["Field Name"].str.endswith("Difference")) & (colData["DataType"] == "String")]["Field Name"].tolist()

for i in range(colData.shape[0]):
    fname=colData["Field Name"].iat[i]
    if fname in (lst1) :
        colData.loc[(colData["Field Name"] == fname), "DataType"] = "Bool"
       
# Replace datatype specified in excelsheet to pandas compatible datatype
# Create a dict using 'Field Name' and 'DataType'
# Use this dictonary to apply datatype
colData["DataType"]=colData["DataType"].replace("String", 'np.str')
colData["DataType"]=colData["DataType"].replace("Numeric", 'np.float64')
colData["DataType"]=colData["DataType"].replace("Decimal", 'np.float64')
colData["DataType"]=colData["DataType"].replace("Bool", 'np.bool')
#colData["DataType"]=colData["DataType"].replace("Decimal", 'np.int64')
colData["DataType"]=colData["DataType"].replace("Date", 'np.str')

colData=colData[['Field Name','DataType']]
col_dict = colData.set_index('Field Name').to_dict()

for k,v in col_dict['DataType'].items():
#    print(k,v,"\n")
    if k in df.columns:
        df[k]=df[k].astype(eval(v)) 
        
        
# Create a list of column datatypes as will be used by h2o
temp = df.ftypes.tolist()
replace_dict = {'object': 'categorical', 'float64': 'double', 'int64': 'double' , 'bool': 'categorical'}
temp_lst = [x for x in map(lambda x: x.split(':')[0], temp)]
new_col_types = [y for y in map(lambda x: replace_dict.get(x), temp_lst)]
new_col_types = ['double'] + new_col_types

# save data to temp file
df.to_csv("./temp_train.csv", index=True, na_rep="", quoting=csv.QUOTE_ALL)

#==============================================================================
# init h2o and read data from temp file

h2o.init(max_mem_size='10g')
dfh2o = h2o.import_file(path="./temp_train.csv" ,col_types =new_col_types)
dfh2o=dfh2o.drop("C1")  # additional column added by h2o

y = 'training_label'
x = dfh2o.col_names    # dfh2o is now an h2o frame

x.remove(y)

train, valid, test = dfh2o.split_frame(ratios=[.8, .1], seed=1234)

gbm = H2OGradientBoostingEstimator(model_id = 'Cash' + csc[:3] + '_'+ datetime.datetime.now().strftime('%Y-%m-%d-%H%M') + '.model', learn_rate=0.3, balance_classes = True, seed = 1234,max_depth=20,ntrees=300)
gbm.train( x=x, y=y, training_frame = train, validation_frame = valid)            
            
pred = gbm.predict(test_data=test)
pred_y = pred['predict']
y_test_actual = test['training_label']
# ipdb.set_trace()
print("Test Accuracy: ", (pred_y == y_test_actual).as_data_frame(use_pandas=True).mean())


dfh2o_varimp = gbm.varimp(use_pandas=True)
dfh2o_varimp = dfh2o_varimp[['variable', 'scaled_importance']].head(20)
print("Top Variable importance", dfh2o_varimp)

cm = gbm.confusion_matrix(test)
confusion_m = cm.as_data_frame()
print("Confusion matrix: ")
print(confusion_m)


#========================== Save Model to disk ================================

# model_save_path = '/home/algolaptop3/Desktop/viteos/model/'

h2o.save_model(gbm, path=model_save_path, force=True)

print("Model has been Saved to {}".format(model_save_path))


#remove temp file
os.remove('./temp_train.csv')
