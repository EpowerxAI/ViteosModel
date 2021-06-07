import pandas as pd
import sys
import csv
from tqdm import tqdm   # for indicating progress
import numpy as np
from pymongo import MongoClient
import datetime
import databaseconfig as cfg
import ipdb

#========================= Take inputs ========================================

def print_usage():
    print('Script to prepare training data for Cash recon model for a specific client')
    print('\nUsage: python3 {} {} {} {} {}\n'.format(sys.argv[0], '<ClientShortCode>', '<datefrom>', '<dateto>', '<recontype>'))
    print('<ClientShortCode>\t: ClientShortCode of the client')
    print('<outputfilename>\t: Full path of outputfile\n')


args = sys.argv[1:]
print('Arguments passed in: {}'.format(args))
if len(args) != 4:
    print_usage()
    sys.exit(1)
#
#inputfilename_meo = args[0]
#inputfilename_aua = args[1]
#csc = args[2]
#outputfilename = args[3]
#
#meo = pd.read_csv(inputfilename_meo, low_memory=False)
#aua = pd.read_csv(inputfilename_aua, low_memory=False)
    
csc = args[0]
datefrom = args[1]
dateto = args[2]
recon_type = args[3]

# ipdb.set_trace()

#========================= Create MongoDB connection ========================================
# creating connectioons for communicating with Mongo DB
client = MongoClient(cfg.dbconnection['meo_connection_host'])
dbname = cfg.dbname['meo_db']
db_meo = client[dbname]   # database to be connected
collection_meo_name = cfg.collection_names['collection_meo']
collection_meo = db_meo[collection_meo_name]
print('MEO collection:', collection_meo)

# creating connectioons for communicating with Mongo DB
dbname = cfg.dbname['aua_db']
db_aua = client[dbname]   # database to be connected
collection_aua_name = cfg.collection_names['collection_aua_' + recon_type]
collection_aua = db_meo[collection_aua_name]
print('AUA collecction:', collection_aua)

startdate = datetime.datetime.strptime(datefrom,'%Y-%m-%d')
enddate = datetime.datetime.strptime(dateto,'%Y-%m-%d')
assert ( enddate >= startdate )

#Read meo frame with MongoDB
try:
    print('Loading MEO data...')
    df_meo = collection_meo.find({'ClientShortCode': csc,'Recon Purpose': recon_type, 'Task Business Date':{'$gte':startdate,'$lt':enddate}})
    meo =  pd.DataFrame(list(df_meo))
    # meo.drop("_id",axis=1,inplace=True)
    # print ('\n Cash Database \n')
    # print(meo)
    
    # meo.drop("_id",axis=1,inplace=True)
    #print(meo)
except Exception as e:
        print( str(e))
        
# ipdb.set_trace()

#Read aua frame with MongoDB
try:
    print('Loading AUA data...')
    df_aua = collection_aua.find({'ClientShortCode': csc,'Recon Purpose': recon_type, 'Task Business Date':{'$gte':startdate,'$lt':enddate}})
    # df_aua = db_aua.collection_aua.find({'csc':csc,'created':{'$gte':startdate,'$lt':enddate}})
    aua =  pd.DataFrame(list(df_aua))
    # aua.drop("_id",axis=1,inplace=True)
    # print ('\n Cash Database \n')
    
    # aua.drop("_id",axis=1,inplace=True)
    #print(meo)
except  e:
        print( str(e))
#========================= Preprocess inputs ==================================

# print('aua:', aua.head())
# print('meo:', meo.head())
# Drop rows with non-required statuses
# ipdb.set_trace()
aua = aua[~aua.Status.isin(['Archive', 'HST', 'OC'])]
meo = meo[~meo.Status.isin(['Archive', 'HST', 'OC'])]

#meo.dropna(subset=['Status'], inplace=True)
aua.dropna(subset=['Status'], inplace=True)     # drop rows having NAs in Status column of aua

if csc != 'All':
    aua = aua[aua.ClientShortCode == csc]
    meo = meo[meo.ClientShortCode == csc]

meo.reset_index(drop=True, inplace=True)
aua.reset_index(drop=True, inplace=True)


#==================== Back propagate Status from AUA to MEO ===================

print('Applying training labels directly..')
for i in tqdm(range(aua.shape[0])):
    s0 = aua['Side0_UniqueIds'].iat[i]
    s1 = aua['Side1_UniqueIds'].iat[i]
    st = aua['Status'].iat[i]
    if (pd.isnull(s0) == False) & (pd.isnull(s1) == False):
           meo.loc[(meo.Side0_UniqueIds == s0) & (meo.Side1_UniqueIds == s1), 'training_label'] = st

print('Applying training labels after splitting..')
for i in tqdm(range(aua.shape[0])):
    s0 = aua['Side0_UniqueIds'].iat[i]
    s1 = aua['Side1_UniqueIds'].iat[i]
    st = aua['Status'].iat[i]

    if pd.isnull(s0) == False:

        ss0=s0.split(',')
        for st1 in ss0:
            #print('**',st1)
            meo.loc[(meo.Side0_UniqueIds == st1) & (meo['training_label'].isnull()), 'training_label'] = st

        temp_s0 = meo[(meo.Side0_UniqueIds == s0)].Side0_UniqueIds
        if  len(ss0) > 1:
            #print(s0, temp_s0.sum())
            meo.loc[(meo.Side0_UniqueIds == s0) & (meo['training_label'].isnull()), 'training_label'] = st
            #_ = input()

    if pd.isnull(s1) == False:

        ss1=s1.split(',')
        for st2 in ss1:
            #print('**',st2)
            meo.loc[(meo.Side1_UniqueIds == st2) & (meo['training_label'].isnull()), 'training_label'] = st

        temp_s1 = meo[(meo.Side1_UniqueIds == s1)].Side1_UniqueIds
        if len(ss1) > 1:
            #print(s1, temp_s1.sum())
            meo.loc[(meo.Side1_UniqueIds == s1) & (meo['training_label'].isnull()), 'training_label'] = st


# ipdb.set_trace()
# print('\ntraining_label column value_counts:')
# print(meo.training_label.value_counts(dropna=False))
timestamp_of_prep = datetime.datetime.now()
file_for_training_model = './file_for_model_trng-' + str(timestamp_of_prep) + '.csv'

meo.to_csv(file_for_training_model, index=True, na_rep='', quoting=csv.QUOTE_ALL)
print('Data to be used for model training has been saved to file {}'.format(file_for_training_model))

