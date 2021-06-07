final_oto_umb_table['ViewData.Side0_UniqueIds'] = final_oto_umb_table['ViewData.Side0_UniqueIds'].astype(str)
            amount_array = ['NULL']
        sideA_col.append(item)
 'probability_UMR',
#filepaths_AUA = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/AUA/AUACollections.AUA_HST_RecData_' + setup_code + '_' + str(date_input) + '.csv'
            cancel_fin = cancel_fin[sel_col_1]
result_non_trade = result_non_trade.reset_index()
                Transaction_type_closed_break_df = \
X_test_left2 = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(filtered_otm_flat))]
            amount_array2 = subSum(values2,net_sum2)
many_ids_1 = []
    # remove numbers
              'abs_amount_flag', 'tt_map_flag', 'description_similarity_score',
final_df.drop(['final_ID','PredictedComment_x','PredictedComment_y','Side1_UniqueIds_y'], axis = 1, inplace = True)
# In[217]:
                                            'ViewData.BreakID_Side1' : 'BreakID',
filepaths_final_oto_umb_table_new = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\UAT_Run\\X_Test_' + setup_code +'\\final_oto_umb_table_new.csv'
umr_mto_table_new['SetupID'] = setup_code 
umb_carry_forward_df['Predicted_action'] = umb_carry_forward_df['Predicted_action'].astype(str)
for i in range(0,umr_otm_table_final.shape[0]):
 'probability_No_pair',
final_oto_umb_table
     'ViewData.ISIN',
#open_ids_0_last
# In[93]:
#                                       'Final_predicted_break', \
brk['final_ID'] = brk.apply(lambda row : fid1(row['Predicted_action'],row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1 )
                                 }
        values2 =  X_test[(X_test['SideA.ViewData.Side1_UniqueIds']==key)]['SideB.ViewData.Accounting Net Amount'].values
final_oto_umb_table = final_oto_umb_table.rename(columns = {'Predicted_action_2':'Predicted_action','probability_No_pair_2':'probability_No_pair','probability_UMB_2':'probability_UMB'})
umr_otm_table_final['ViewData.Side0_UniqueIds'] = umr_otm_table_final['ViewData.Side0_UniqueIds'].astype(str)
umb_carry_forward_df['probability_No_pair'] = umb_carry_forward_df['probability_No_pair'].astype(str)
umb_carry_forward_df = meo_df[meo_df['ViewData.Status'] == 'UMB']
                 'SideA.ViewData._ID', 'SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds', 'SideA.ViewData.Side1_UniqueIds',          
if(umr_otm_table.empty == False):
# In[182]:
        elim1 = cancel_broker[(cancel_broker['map_match']==1) & (cancel_broker['curr_match']==1)  & ((cancel_broker['isin_match']==1) |(cancel_broker['cusip_match']==1)| (cancel_broker['ticker_match']==1) | (cancel_broker['Invest_id']==1))]
#                                       'Side0_UniqueIds', \
        aa1 = dummy[dummy['ViewData.Task Business Date1']==d]
result_trade = result_trade.drop('predicted comment', axis = 1)
            dffk2 = dffk2[~dffk2['final_ID'].isin(comb)]
if(umr_mto_table.empty == False):
result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
##############
for i in many_ids_0:
umb_carry_forward_df['probability_UMB'] = umb_carry_forward_df['probability_UMB'].astype(str)
test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('eqt','equity'))
# In[127]:
X_test_left4 = X_test_left4.reset_index().drop('index',1)
rr2 = X_test_left4[X_test_left4['Predicted_action_2']=='UMB_One_to_One'].groupby(['SideB.ViewData.Side0_UniqueIds'])['SideA.ViewData.Side1_UniqueIds'].unique().reset_index()
    df[new_text_field_name] = df[new_text_field_name].str.replace(' eur','')
cols_for_database_new = ['Side0_UniqueIds',
    print(key)
for key in X_test_left['SideB.ViewData.Side0_UniqueIds'].unique():
dff7 = dffk5[sel_col]
final_df_2.drop(['final_ID','PredictedComment_x','PredictedComment_y','Side0_UniqueIds_y'], axis = 1, inplace = True)
change_col_names_umr_otm_final_new_dict = {
    if i in filtered_otm:
X_test_left.shape
for key in X_test[~((X_test['SideB.ViewData.Side0_UniqueIds'].isin(exceptions_0_umb_ids)) | (X_test['SideB.ViewData.Side0_UniqueIds'].isin(final_umr_table['SideB.ViewData.Side0_UniqueIds'])))]['SideB.ViewData.Side0_UniqueIds'].unique():
    elim1['SideA.predicted comment'] = elim1.apply(lambda x : updownat(x['map_match'],x['curr_match'],x['sd_match'],x['fund_match'],x['ttype_match']), axis = 1)
data22 = data2[data2['ViewData.Transaction Type1'].isin(trade_types)]
for j in many_ids_1:
#                                                    umr_mto_table_new_to_write, \
filename = 'finalized_model_soros_non_trade_v9.sav'
for i in many_ids_1:
# In[171]:
# In[218]:
cols = [
        cancel_fin = cancel_fin[sel_col_1]
cliff_for_loop = 16
                                 'Transfer' : {'side' : 'Acct',
# In[273]:
    print(temp_umr_otm_table_message)
change_names_of_umb_carry_forward_df_mapping_dict = {
# In[166]:
# In[173]:
 'Predicted_action',
                                 'REORG' : {'side' : 'PB',
#'Interest Amount',
    for length in range(1, 3):
#                                       'probability_No_pair', \
X_test_left4 = X_test_left3[~((X_test_left3['SideB.ViewData.Side0_UniqueIds'].isin(open_ids_0_last)) | (X_test_left3['SideA.ViewData.Side1_UniqueIds'].isin(open_ids_1_last)))]
filtered_mto_flat = [item for sublist in filtered_mto for item in sublist]
# In[138]:
             'ViewData.Combined Fund',
df2['desc_cat'] = df2['ViewData.Description'].apply(lambda x : descclean(x,cat_list))
# In[160]:
df2['new_pb'] = df2['new_pb'].apply(lambda x : new_pf_mapping(x))
        appended_data.append(cc1)
        dup_array_0.append(i)
final_closed_df['probability_No_pair'] = ''
X_test_umb = X_test_umb.reset_index().drop('index',1)
    frames = [elim2,elim3]
# In[168]:
             # 'SideB.ViewData.Accounting Net Amount', 
many_ids_1_list =[] 
#    elim_col = list(elim1.columns)
sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','flag_side0'] = 0
        k = 'zero'
        com = k + ' ' +y + ' ' + str(z)
 'ML_flag',
# In[126]:
    id_listB = list(set(df_213_1['SideB.final_ID'])) 
    unique_many_ids_1 = np.unique(np.concatenate(many_ids_1))
result_trade = result_trade[['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']]
final_closed_df['Predicted_Status'] = 'UCB'
        dffk2 = dffk2[~dffk2['final_ID'].isin(comb)]
#                 'Side1_UniqueIds', \
# ### Many to One
    umr_mto_table = umr_mto_table[umr_mto_table['SideA.ViewData.Side1_UniqueIds'].isin(one_id_1_final)]
# In[6]:
                one_id_0.append(id0_unique)
final_table_to_write = final_table_to_write.append(final_closed_df)
# In[148]:
                    (abs(fun_side_meo_combination_df[Net_amount_col_name_list[0]]).astype(str) == abs(fun_side_meo_combination_df[Net_amount_col_name_list[1]]).astype(str)) & \
# In[129]:
## Total IDs 
    cancel_trade = list(set(dffk3[dffk3['cancel_marker'] == 1]['ViewData.Transaction ID']))
umr_otm_table_final 
final_closed_df['probability_UMR'] = ''
Pre_final = [
dup_ids_1 = []
                        (Transaction_type_closed_break_df['ViewData.Custodian_x'].astype(str) == 'CS') & \
filepaths_final_table_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_table_to_write.csv'
# #### Prime Broker Creation
        #print(net_sum)
pb_amount_sum =  X_test[X_test['Predicted_action_2']=='UMB_One_to_One'].groupby(['SideB.ViewData.Side0_UniqueIds'])['SideA.ViewData.B-P Net Amount'].sum().reset_index()
setup_code = '153'
umr_otm_table_final_new.rename(columns = change_names_of_umr_otm_table_mapping_dict, inplace = True)
#def return_BreakID_list(list_x, fun_meo_df):
test_file =  clean_text(test_file,'SideA.ViewData.Description', 'SideA.ViewData.Description_new') 
umb_carry_forward_df[['BreakID', 'TaskID']] = umb_carry_forward_df[['BreakID', 'TaskID']].astype(np.int64)
coll_1_for_writing_prediction_data = db_1_for_MEO_data['MLPrediction_Cash']
            'Settle_date_diff', 'SideA.ISIN_NA', 'SideB.ISIN_NA', 
# In[113]:
umr_mto_table_new['probability_UMR'] = 0.95
final_oto_umb_table_new[['SetupID']] = final_oto_umb_table_new[['SetupID']].astype(int)
                    (fun_side_meo_combination_df['ViewData.Transaction Type_x'].astype(str).isin(fun_transaction_type_list)) & \
                                            'ViewData.BreakID' : 'BreakID',
#umr_mto_table
umb_carry_forward_df['SetupID'] = setup_code 
umr_otm_table_final_col_order = ['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','BreakID_Side1_str','BreakID_Side0']
                                            'ViewData.Source Combination Code' : 'SourceCombinationCode'
       'ViewData.Mapped Custodian Account', 'ViewData.Department',
# #### For Trade Model
acc_amount = X_test[X_test['Predicted_action_2']=='UMB_One_to_One'].groupby(['SideB.ViewData.Side0_UniqueIds'])['SideB.ViewData.Accounting Net Amount'].max().reset_index()
                              'SideA.ViewData.Side1_UniqueIds' : 'ViewData.Side1_UniqueIds',
              'Combined_Desc',
# In[165]:
dffk4 = dfk_nontrade[dfk_nontrade['ViewData.Side0_UniqueIds']=='AA']
    a_side_agg['No_Pair_flag'] = a_side_agg['Predicted_action'].apply(lambda x: 1 if 'No-Pair' in x else 0)
    unique_many_ids_0 = ['None']
            c1['predicted category'] = 'Cancelled trade'
final_oto_umb_table_new['Task Business Date'] = pd.to_datetime(final_oto_umb_table_new['Task Business Date'])
        id_listB = list(set(c1['final_ID']))
umr_otm_table_final_new_to_write = umr_otm_table_final_new[cols_for_database_new]
    id_listA = list(set(df_213_1['SideA.final_ID']))
    comment_df_final_list.append(elim)
dup_array_0 = []
umr_otm_table_final_new['Predicted_Status'] = 'UMR'
result_trade = result_trade.reset_index()
# In[137]:
X_test_left = X_test_left.reset_index().drop('index',1)
    for i in range(0,umr_mto_table.shape[0]):
    elif e == 0:
rr4 = pd.merge(rr3, pb_amount_sum, on='SideB.ViewData.Side0_UniqueIds', how='left')
#Comment
filepaths_final_oto_umb_table_new = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' +client + '\\final_oto_umb_table_new.csv'
            if count==2:
umr_mto_table['BreakID_Side1'] = umr_mto_table['BreakID_Side1'].astype(int)
rr8 = rr8.reset_index().drop('index',1)
                                 'JNL' : {'side' : 'PB',
#        umr_mto_table['BreakID_Side0'].iloc[i] = list(meo_df[meo_df['ViewData.Side0_UniqueIds'].isin(umr_mto_table['SideB.ViewData.Side0_UniqueIds'].values[i])]['ViewData.BreakID'])
        for index,number in enumerate(numbers):
    umr_otm_table_final['BreakID_Side0'].iloc[i] = meo_df[meo_df['ViewData.Side0_UniqueIds'] == umr_otm_table_final['SideB.ViewData.Side0_UniqueIds'][i]]['ViewData.BreakID'].values
days = [1,30,31,29]
                        (Transaction_type_closed_break_df['ViewData.Custodian_y'].astype(str) == 'CS') \
# In[112]:
# In[207]:
result_non_trade['predicted category'] = pd.Series(demo)
                                          'Transaction_Type' : ['Miscellaneous'],
if(len(many_ids_1) == 0):
final_closed_df[['BreakID', 'TaskID']] = final_closed_df[['BreakID', 'TaskID']].astype(np.int64)
    pb_ids_otm_left.append(pb_ids_otm)
#pd.set_option('max_columns',50)
    df[new_text_field_name] = df[new_text_field_name].str.replace(' euro','')
        bb1 = dummy1[dummy1['ViewData.Task Business Date1']==d]
umr_otm_table_final = umr_otm_table_final[umr_otm_table_final_col_order] 
#    for i in range(0,umr_mto_table.shape[0]):
# In[131]:
df2['ViewData.Prime Broker1'] = df2['ViewData.Prime Broker1'].fillna('kkk')
comment_df_final.rename(columns = change_col_names_comment_df_final_dict, inplace = True)
# ## UMR One to Many and Many to One 
filepaths_umr_mto_table_new = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' +client + '\\umr_mto_table_new.csv'
# ## Many to Many new
side1_id = list(set(brk[brk['ViewData.Side0_UniqueIds'] =='AA']['ViewData.Side1_UniqueIds']))
# In[211]:
    a_side_agg = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action'].unique().reset_index()
                dup_ids_1.append(i)
    for d in tqdm(k1):
df = df[df['ViewData.Status'].isin(['OB','SDB','UOB','UDB','CMF','CNF','SMB','SPM'])]
        cancel_fin = pd.concat([original,cancellation])
# In[199]:
                                                   'ViewData.Task Business Date', \
    umr_mto_table['BreakID_Side0'] = umr_mto_table.apply(lambda x: list(meo_df[meo_df['ViewData.Side0_UniqueIds'].isin(umr_otm_table_final['SideB.ViewData.Side0_UniqueIds'])]['ViewData.BreakID']), axis=1)
# In[174]:
# In[128]:
umb_carry_forward_df[umb_carry_forward_columns_to_select_from_meo_df] = umb_carry_forward_df[umb_carry_forward_columns_to_select_from_meo_df].astype(str)
final_oto_umb_table_new_to_write.to_csv(filepaths_final_oto_umb_table_new)
umb_carry_forward_df['Predicted_Status'] = umb_carry_forward_df['Predicted_Status'].astype(str)
        if elim1.shape[0]!=0:
final_oto_umb_table_new[['BreakID', 'Task ID']] = final_oto_umb_table_new[['BreakID', 'Task ID']].astype(float)
    elim = pd.concat(frames)
cb_predictions = clf.predict(data222)#.astype(str)
# In[198]:
Soros_umr = pd.read_csv('Soros_UMR_new.csv')
        cancel_broker[['map_match','amt_match','fund_match','curr_match','sd_match','ttype_match','Qnt_match','isin_match','cusip_match','ticker_match','Invest_id']] = cancel_broker.apply(lambda row : amountelim(row), axis = 1,result_type="expand")
                pass
## Remove UMR IDs
# In[145]:
# ## Removing IDs from OTO UMB
# In[102]:
data = data.drop('index', axis = 1)
        bb1['marker'] = 1
    elim_col = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']
                return [number] + subset
dup_array_1 = []
dff4 = pd.concat([dff4,dff6])
# In[208]:
# In[139]:
X_test.loc[(X_test['Amount_diff_2'] != 0) & (X_test['Predicted_action'] == 'UMR_One_to_One'), 'Predicted_action'] = 'UMB_One_to_One'
                                            'ViewData.Task Business Date' : 'BusinessDate',
#                 'probability_UMB', \
    umr_table = b_count2[(b_count2['Predicted_action']=='UMR_One_to_One') & (b_count2['count']==1) & (b_count2['len']<=3)]
X_test_left['SideB.ViewData.Side0_UniqueIds'].value_counts()
# In[260]:
    umr_mto_table['SideB.ViewData.Side0_UniqueIds'] =many_ids_0 
umr_otm_table_final_new['probability_UMB'] = 0.05
def cancelcomment3(y):
    k = list(set(list(set(dummy['ViewData.Task Business Date1']))))
    acc_amount = X_test[X_test['SideB.ViewData.Side0_UniqueIds']==key]['SideB.ViewData.Accounting Net Amount'].max()
            c1 = dummy
final_closed_df[['BreakID', 'TaskID']] = final_closed_df[['BreakID', 'TaskID']].astype(float)
    for j in many_ids_1:
    #print(i)
one_id_0_final = []
    df[new_text_field_name] = df[new_text_field_name].str.replace('eur0','')
    elif x == 0.0:
result_non_trade['predicted comment'] = 'NA'
                dup_ids_0.append(i)
                                        'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
        bb1 = bb1.drop('index', 1)
exceptions_0_umb = X_test[X_test['Predicted_action_2']=='UMB_One_to_One']['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count')
# In[215]:
                                          'Transaction_Type' : ['DEP','WDRL'],
final_oto_umb_table_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'Source Combination Code', 'Predicted_Status', 'ML_flag']] = final_oto_umb_table_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'Source Combination Code', 'Predicted_Status', 'ML_flag']].astype(str)
for i in range(0,umr_otm_table_final_new.shape[0]):
umr_mto_table_new['PredictedComment'] = ''
# ## Remove Open Ids
# In[161]:
        aa1.columns = ['SideB.' + x  for x in aa1.columns] 
    print(temp_umr_mto_table_message)
X_test_left2 = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(filtered_mto_flat))]
                                            'ViewData.BreakID_Side0' : 'Final_predicted_break',
# In[140]:
#final_no_pair_table_copy.to_csv(filepaths_final_no_pair_table_copy)
brk['ViewData.Side1_UniqueIds'] = brk['ViewData.Side1_UniqueIds'].replace('nan','BB')
                                            'ViewData.Task ID' : 'TaskID',
    return df
for i in dup_array_1:
test_file['SideA.ViewData.Prime Broker'] = test_file['SideA.ViewData.Prime Broker'].apply(lambda x : 'kkk' if x == '' else x)
umb_carry_forward_df['Final_predicted_break'] = umb_carry_forward_df['Final_predicted_break'].astype(str)
                            (Transaction_type_closed_break_df['ViewData.Transaction ID_x'].astype(str) == Transaction_type_closed_break_df['ViewData.Transaction ID_y'].astype(str)) \
umb_carry_forward_df = umb_carry_forward_df[umb_carry_forward_columns_to_select_from_meo_df]
            cancel_fin = cancel_fin.drop('index', axis = 1)
        umr_mto_table['BreakID_Side0'].iloc[i] = list(meo_df[meo_df['ViewData.Side0_UniqueIds'].isin(umr_mto_table['SideB.ViewData.Side0_UniqueIds'].values[i])]['ViewData.BreakID'])#        fun_otm_mto_df['BreakID_Side1'].iloc[i] = list(fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin(fun_otm_mto_df['SideA.ViewData.Side1_UniqueIds'].iloc[i])]['ViewData.BreakID'])
filepaths_final_closed_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_closed_df.csv'
# In[266]:
#Closed Ends
final_closed_df['PredictedCategory'] = ''
#filepaths_meo_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\meo_df.csv'
open_ids_0_last , open_ids_1_last = no_pair_seg2(X_test_left3)
#    return [fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin(list(i))]['ViewData.BreakID'] for i in list_x]
                                 'DEB_CRED' : {'side' : 'PB',
        comment_df_final_list.append(cancel_fin)
final_oto_umb_table_new['ML_flag'] = 'ML'
umr_mto_table_new['Predicted_action'] = 'UMR_One-Many_to_Many-One'
#open_ids_1_last
df2['new_desc_cat'] = df2['new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)
# In[151]:
        one_id_1_final.append(j) 
result_trade =data22.copy()
# In[169]:
# In[1849]:
        cancellation['predicted comment'] =  cancellation.apply(lambda x : cancelcomment1(x['ViewData.Transaction ID'],x['ViewData.Settle Date1']), axis = 1)
'Quantity','SEDOL','Settle Date','SPM ID','Status',
change_names_of_umr_otm_table_mapping_dict = {
X_test_left = X_test_left.drop(['SideB.ViewData._ID','SideA.ViewData._ID'],1).drop_duplicates()
brk['ViewData.Side0_UniqueIds'] = brk['ViewData.Side0_UniqueIds'].fillna('AA')
final_closed_df[closed_columns_for_updation] = final_closed_df[closed_columns_for_updation].astype(str)
            sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']
threshold_1 = X_test['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count')
Created on Thu Aug 13 19:12:48 2020
# In[2123]:
#Start of Commenting
test_file['SideA.ViewData.Prime Broker'] = test_file['SideA.ViewData.Prime Broker'].astype(str)
    elim3 = elim3.rename(columns= {'SideB.final_ID':'final_ID',
rr5 = rr4.groupby(['SideA.ViewData.Side1_UniqueIds'])['SideB.ViewData.Side0_UniqueIds'].unique().reset_index()
filepaths_umr_otm_table_final_new = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' +client + '\\umr_otm_table_final_new.csv'
    count =0
final_closed_df['ML_flag'] = final_closed_df['ML_flag'].astype(str)
# In[132]:
#MANY TO MANY NEW
                              'SideB.predicted category':'predicted category',
# In[164]:
brk = brk[brk['Predicted_action'] == 'No-Pair']
umr_mto_table = normalize_final_no_pair_table_col_names(fun_final_no_pair_table = umr_mto_table)
                many_ids_1.append(id1_aggregation)
    dff5 = dff5[dff5['final_ID'].isin(id_listA)]
                 param_MONGO_USERNAME = 'mongouseradmin', param_MONGO_PASSWORD = '@L0ck&Key')
rr7['acc_len'] = rr7['SideB.ViewData.Side0_UniqueIds'].apply(lambda x: len(x))
        if subSum(values,net_sum) == []: 
umr_otm_table_double_count = pd.DataFrame(acc_id_single)
X_test_left4['SideB.ViewData.Side0_UniqueIds'].nunique() + X_test_left4['SideA.ViewData.Side1_UniqueIds'].nunique()
umb_carry_forward_df['Side0_UniqueIds'] = umb_carry_forward_df['Side0_UniqueIds'].astype(str)
rr8[rr8['diff']==0]
filtered_mto = [i for i in many_ids_0_list if not i in dup_array_1_list]
umb_carry_forward_df['probability_UMR'] = umb_carry_forward_df['probability_UMR'].astype(str)
             # 'SideA.new_desc_cat',
    #'SideA.ViewData.B-P Net Amount', 
 'probability_UMB',
#final_closed_df[[\
# In[249]:
#For remaining obs, fill probability as 60 - 90
umb_carry_forward_columns_to_select_from_meo_df = ['ViewData.BreakID', \
                                        'predicted comment' : 'PredictedComment'
model_cols_2 =[
    com1 = 'This is cancelled trade'
            return []
X_test_left2 = X_test_left2.reset_index().drop('index',1)
# In[133]:
    unique_many_ids_0 = np.unique(np.concatenate(many_ids_0))
umb_carry_forward_df['BusinessDate'] = umb_carry_forward_df['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
    elim1['SideB.predicted comment'] = elim1.apply(lambda x : updownat(x['map_match'],x['curr_match'],x['sd_match'],x['fund_match'],x['ttype_match']), axis = 1)
# #### Testing of Model and final prediction file - Non Trade
# In[214]:
        values =  X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key) & (X_test['Predicted_action_2']=='UMB_One_to_One')]['SideA.ViewData.B-P Net Amount'].values
test_file['new_pb1'] = test_file['new_pb1'].apply(lambda x: x.replace('Citi','CITI'))
                              'SideA.predicted comment':'predicted comment'})
umb_carry_forward_df['Predicted_action'] = 'UMB_Carry_Forward'
final_closed_df['Final_predicted_break'] = ''
             # 'ViewData.Combined Investment Type',
for i in dup_array_0:
#                 'Predicted_Status', \
final_no_pair_table.to_csv(filepaths_final_no_pair_table)
            id1_unique = key       
# In[124]:
           # 'Amount_diff_2', 
client = 'Soros'
# In[175]:
            if(fun_transaction_type_list == ['JNL']):
    if len(cancel_trade)>0:
filepaths_final_no_pair_table_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_no_pair_table_copy.csv'
              'All_key_nan','new_key_match', 'new_pb1','Combined_TType',
# In[263]:
        cancel_fin = c1[sel_col_1]
comment_df_final_list.append(result_non_trade)
                              'SideB.ViewData.Side1_UniqueIds' : 'ViewData.Side1_UniqueIds',                                   
# In[176]:
              'ViewData.Combined Transaction Type', 'Combined_Investment_Type','Combined_Asset_Type_Category',
                                          'Transaction_Type' : ['Collateral'],
                    (fun_side_meo_combination_df['ViewData.Transaction Type_y'].astype(str).isin(fun_transaction_type_list)) & \
# In[272]:
final_oto_umb_table = normalize_final_no_pair_table_col_names(fun_final_no_pair_table = final_oto_umb_table)
def  clean_text(df, text_field, new_text_field_name):
    if x=='nan' or x == '' or x == 'None':
filepaths_meo_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\meo_df.csv'
umb_carry_forward_df['ML_flag'] = umb_carry_forward_df['ML_flag'].astype(str)
test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))
com_temp = pd.read_csv('Soros comment template for delivery.csv')
        return []
#meo_df = pd.read_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Soros\\meo_df.csv')
change_col_names_umr_mto_table_new_dict = {
        if subSum(values2,net_sum2) == []: 
        item = 'SideA.'+items
              #'SideB.ViewData.Description',
# In[268]:
final_df_2 = final_df.merge(comment_df_final_side1, on = 'Side1_UniqueIds', how = 'left')
            amount_array2 =[]
rr7 = pd.merge(rr6,rr4[['SideA.ViewData.Side1_UniqueIds','SideA.ViewData.B-P Net Amount']].drop_duplicates(), on='SideA.ViewData.Side1_UniqueIds',how='left')
#filepaths_final_table_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_table_to_write.csv'
for key in X_test[~((X_test['SideA.ViewData.Side1_UniqueIds'].isin(exceptions_1_umb_ids)) |(X_test['SideA.ViewData.Side1_UniqueIds'].isin(final_umr_table['SideA.ViewData.Side1_UniqueIds'])))]['SideA.ViewData.Side1_UniqueIds'].unique():
threshold_1_without_umb = threshold_1[threshold_1['count']<=cliff_for_loop]['index'].unique()
final_oto_umb_table_new.rename(columns = change_names_of_final_oto_umb_table_new_mapping_dict, inplace = True)
# In[153]:
final_closed_df['SetupID'] = setup_code 
change_col_names_comment_df_final_dict = {
meo['ViewData.Status'].value_counts()
        sideB_col.append(item)
import random
# In[196]:
# In[8]:
# In[116]:
# In[180]:
    acc_id_single.append(key)
    umb_ids_1 = X_test_left[(X_test_left['SideB.ViewData.Side0_UniqueIds']==key) & (X_test_left['Predicted_action_2']=='UMB_One_to_One')]['SideA.ViewData.Side1_UniqueIds'].unique()
umr_mto_table_new = pd.merge(umr_mto_table, meo_df[['ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), on = 'ViewData.Side1_UniqueIds', how='left')
df2.shape
# In[200]:
        com = "Geneva" + ' ' +y + ' ' + str(z)
# In[105]:
def cancelcomment2(y):
final_table_to_write.to_csv(filepaths_final_table_to_write)
umr_mto_table_new_to_write.to_csv(filepaths_umr_mto_table_new)
result_trade = pd.merge(result_trade,com_temp,on = 'predicted category',how = 'left')
    if key in threshold_0_umb:
if dffk3[dffk3['cancel_marker'] == 1].shape[0]!=0:
    umr_otm_table['SideA.ViewData.Side1_UniqueIds'] =many_ids_1 
umr_mto_table['ViewData.Side0_UniqueIds'] = umr_mto_table['ViewData.Side0_UniqueIds'].astype(str)
    id_listA = list(set(elim1['SideA.final_ID']))
result_non_trade = pd.merge(result_non_trade,com_temp,on = 'predicted category',how = 'left')
    b_side_agg = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].unique().reset_index()
pd.set_option('display.max_columns', 500)
change_col_names_final_oto_umb_table_new_dict = {
# In[252]:
umr_otm_table_final['BreakID_Side1_int'] = umr_otm_table_final['BreakID_Side1'].apply(lambda x : return_int_list(x))
# ## Removing duplicate IDs from side 0
X_test_left = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(final_umr_table['SideB.ViewData.Side0_UniqueIds']))]
df2['new_pb'] = df2['ViewData.Mapped Custodian Account'].apply(lambda x : x.split('_')[0] if type(x)==str else x)
rr2 = X_test[X_test['Predicted_action_2']=='UMB_One_to_One'].groupby(['SideB.ViewData.Side0_UniqueIds'])['SideA.ViewData.Side1_UniqueIds'].unique().reset_index()
'Asset Type Category','Base Currency','Base Net Amount',
              #'SideA.ViewData.Description',
X_test_left3[~X_test_left3['SideB.ViewData.Side0_UniqueIds'].isin(open_ids_0_last)]
df2['new_pb1'] = df2['new_pb1'].apply(lambda x : x.lower())
final_closed_df['probability_No_pair'] = final_closed_df['probability_No_pair'].astype(str)
# In[202]:
    unique_many_ids_1 = ['None']
umr_mto_table_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'Source Combination Code', 'Predicted_Status', 'ML_flag']] = umr_mto_table_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'Source Combination Code', 'Predicted_Status', 'ML_flag']].astype(str)
# ## UMB one to one (final)
data['monthend marker'] = data['ViewData.Settle Date'].apply(lambda x : 1 if x.day in days else 0)
                              'SideA.predicted category':'predicted category',
data21 = data2[~data2['ViewData.Transaction Type1'].isin(trade_types)]
    call1 = []
#                                                    umr_otm_table_final_new_to_write])
#                 'Final_predicted_break', \
    umr_mto_table_new['probability_UMB'].iloc[i] = float(decimal.Decimal(random.randrange(50, 100))/1000)
    return [int(i) for i in list_x]
    elim3 = elim1[sideB_col]
umr_otm_table_final = normalize_final_no_pair_table_col_names(fun_final_no_pair_table = umr_otm_table_final)
umr_otm_table_final_new[['SetupID']] = umr_otm_table_final_new[['SetupID']].astype(int)
##Fill probability between 80 - 90 for no_pair 
#final_no_pair_table.to_csv(filepaths_final_no_pair_table)
# In[99]:
             ]
comment_df_final_side0 = comment_df_final[comment_df_final['Side1_UniqueIds'] == 'BB']
                    Transaction_type_closed_break_df[ \
    umr_mto_table_new['probability_No_pair'].iloc[i] = float(decimal.Decimal(random.randrange(50, 100))/1000)
    temp_umr_mto_table_message = 'No Many to One found'
X_test_left3 = X_test_left2[~(X_test_left2['SideB.ViewData.Side0_UniqueIds'].isin(final_oto_umb_table['SideB.ViewData.Side0_UniqueIds']))]
    sideB_col = []
    elim1['SideA.predicted category'] = 'Updown'
X_test_umb = X_test_left2[X_test_left2['Predicted_action_2']=='UMB_One_to_One']
pb_ids_otm_left = []
umr_otm_table_final_new['SetupID'] = setup_code 
    for items in elim_col:
    if(fun_setup_code_crucial == '153'):
filepaths_no_pair_table = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\UAT_Run\\X_Test_' + setup_code +'\\final_no_pair_table.csv'
umr_otm_table_final_new['Predicted_action'] = 'UMR_One-Many_to_Many-One'
                                                    umr_mto_table_new_to_write, \
 'SetupID']
for i in unique_many_ids_1:
                                            ]
               'ViewData.Fund',
# In[209]:
# In[136]:
#                 'probability_UMR', \
        comb = id_listB
many_ids_0_list =[] 
                                                    ['ViewData.BreakID'])#        fun_otm_mto_df['BreakID_Side1'].iloc[i] = list(fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin(fun_otm_mto_df['SideA.ViewData.Side1_UniqueIds'].iloc[i])]['ViewData.BreakID'])
 'BusinessDate',
umr_otm_table_final_new[['BreakID', 'Task ID']] = umr_otm_table_final_new[['BreakID', 'Task ID']].astype(float)
db_1_for_MEO_data = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['ReconDB_Soros_ML']
    df[text_field] = df[text_field].astype(str)
        if x == 'db_int':
for i in unique_many_ids_0:
 'TaskID',
        k = 'fund'    
amount_array =[]
umb_carry_forward_df['Final_predicted_break'] = ''
umb_carry_forward_df['PredictedComment'] = ''
umr_otm_table = umr_otm_table[umr_otm_table['SideB.ViewData.Side0_UniqueIds'].isin(one_id_0_final)]
                                            'BreakID_Side1' : 'BreakID',
umb_carry_forward_df['Predicted_Status'] = 'UMB'
        if i in j:
filepaths_umr_mto_table_new = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\UAT_Run\\X_Test_' + setup_code +'\\umr_mto_table_new.csv'
umr_otm_table_final['BreakID_Side1_str'] = [','.join(map(str,lst)) for lst in umr_otm_table_final['BreakID_Side1_int']]
result_trade.to_csv('Comment file soros 2 sep testing p7.csv')
umb_carry_forward_df['BusinessDate'] = pd.to_datetime(umb_carry_forward_df['BusinessDate'])
##This has probability already
umb_carry_forward_df['probability_No_pair'] = ''
#'Activity Code',
### Remove Open IDs
umb_carry_forward_df['probability_UMR'] = ''
result_non_trade.to_csv('Comment file soros 2 sep testing p6.csv')
        net_sum2 = X_test[X_test['SideA.ViewData.Side1_UniqueIds']==key]['SideA.ViewData.B-P Net Amount'].max()
final_oto_umb_table_new['Predicted_Status'] = 'UMB'
            count = count+1
        #memo = dict()
# In[216]:
test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('options','option'))
def subSum(numbers,total):
df2['new_desc_cat'] = df2['new_desc_cat'].apply(lambda x : desccat(x))
final_closed_df['Final_predicted_break'] = final_closed_df['Final_predicted_break'].astype(str)
comment_df_final_list.append(result_trade)
umb_carry_forward_df['probability_UMB'] = ''
# #### 1st for Non Trade
final_table_to_write = final_table_to_write.append([final_oto_umb_table_new_to_write, \
    pool =[]
    dffk2 = dffk2.copy()
dup_ids_0 = []
if(len(many_ids_0) == 0):
dff5 = dff5.reset_index()
X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(umr_ids_a_side))]
    dff5 = dff5.copy()
            'Amount_diff_2', 
#def return_BreakID_list2(list_x, fun_meo_df):
#                 'probability_No_pair', \
        original['predicted comment'] = original.apply(lambda x : cancelcomment(x['ViewData.Transaction ID'],x['ViewData.Settle Date1']), axis = 1)
                                                    .isin(umr_otm_table_final['SideA.ViewData.Side1_UniqueIds'].values[i])]\
final_no_pair_table_copy = final_no_pair_table_copy.append([no_pair_ids_df,no_pair_ids_last_df])
exceptions_1_umb_ids = exceptions_1_umb[exceptions_1_umb['count']>cliff_for_loop]['index'].unique()
# In[108]:
umr_otm_table_final['ViewData.Side1_UniqueIds'] = umr_otm_table_final['ViewData.Side1_UniqueIds'].astype(str)
final_closed_df['BusinessDate'] = final_closed_df['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
            c2 = dummy1[dummy1['final_ID'].isin(id_listA)]
rr2['SideA.ViewData.Side1_UniqueIds'] = rr2['SideA.ViewData.Side1_UniqueIds'].apply(tuple)
            if len(id1_aggregation)>1: 
                              'SideA.ViewData.Side0_UniqueIds' : 'ViewData.Side0_UniqueIds',
                return [number]
coll_1_for_writing_prediction_data.insert_many(data_dict) 
# ### Cleannig of the 4 variables in this
#!/usr/bin/env python
                                                    umr_otm_table_final_new_to_write])
# In[98]:
umr_ids_b_side = final_umr_table['SideB.ViewData.Side0_UniqueIds'].unique()
            if len(id0_aggregation)>1: 
final_oto_umb_table['ViewData.Side1_UniqueIds'] = final_oto_umb_table['ViewData.Side1_UniqueIds'].astype(str)
# In[178]:
final_oto_umb_table_new_to_write = final_oto_umb_table_new[cols_for_database_new]
for key in umr_double_count_left['SideB.ViewData.Side0_UniqueIds'].unique():
        cancellation['predicted category'] = 'Cancelled trade'
# ## Machine generated output
#    return [fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'] == str(i)]['ViewData.BreakID'].unique() for i in list_x]
no_pair_ids_last_df = pd.DataFrame(no_pair_ids_last, columns = ['Side0_1_UniqueIds'])
umr_otm_table_final_new['Task Business Date'] = umr_otm_table_final_new['Task Business Date'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
### IDs left after removing UMR ids from 0 and 1 side
#    umr_mto_table = umr_mto_table[umr_mto_table['SideA.ViewData.Side1_UniqueIds'].isin(one_id_1_final)]
            cancel_fin = pd.concat([c1,c2])
            id_listA = list(set(elim1['SideA.final_ID']))
#umr_otm_table_final['count_side0_ids'] = umr_otm_table_final['SideA.ViewData.Side1_UniqueIds'].apply(lambda x : len(x))
        if(fun_transaction_type_list == ['JNL'] or fun_transaction_type_list == ['MTM']):
final_closed_df['Predicted_action'] = 'Closed'
    if a=='No-Pair':
umr_mto_table['BreakID_Side1'] = meo_df[meo_df['ViewData.Side1_UniqueIds'].isin(list(umr_mto_table['SideA.ViewData.Side1_UniqueIds']))]['ViewData.BreakID'].values
#umr_otm_table_final['count_ids'] = umr_otm_table_final['count_side0_ids'] + 1
 'BreakID',
rr2.groupby(['SideA.ViewData.Side1_UniqueIds'])['SideB.ViewData.Side0_UniqueIds'].unique().reset_index()
dfk_nontrade = df2[~df2['ViewData.Transaction Type1'].isin(trade_types)]
# In[117]:
# In[183]:
            amount_array = subSum(values,net_sum)
    training_df =[]
brk = final_table_to_write.copy()
filtered_otm = [i for i in many_ids_1_list if not i in dup_array_0_list]
data = pd.concat(frames)
#                                                    final_oto_umb_table_new_to_write, \
comment_df_final_list = []
umr_mto_table_new['probability_UMB'] = 0.05
dff4 = dff4.drop('index', axis = 1)
                                                   'ViewData.Source Combination Code', \
# In[149]:
final_closed_df['Predicted_action'] = final_closed_df['Predicted_action'].astype(str)
# In[201]:
dup_array_1_list = []
umr_mto_table['BreakID_Side0_int'] = umr_mto_table['BreakID_Side0'].apply(lambda x : return_int_list(x))
amount_array2 =[]
final_oto_umb_table['probability_UMR'] = 0.00010
             # 'label']
       'ViewData.Description','ViewData.Department', 
            id1_aggregation = X_test[(X_test['SideA.ViewData.B-P Net Amount'].isin(amount_array)) & (X_test['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.Side1_UniqueIds'].values
#UMB Carry Forward Ends
                    (fun_side_meo_combination_df[Side_0_1_UniqueIds_col_name_list[0]].astype(str) != fun_side_meo_combination_df[Side_0_1_UniqueIds_col_name_list[1]].astype(str)) \
'B-P Net Amount',
meo_df.drop_duplicates(keep=False, inplace = True)
    dup_array_1_list.append(list(i))
#final_table_to_write.to_csv(filepaths_final_table_to_write)
            comment_df_final_list.append(cancel_fin)
X_test_left3['SideB.ViewData.Side0_UniqueIds'].nunique() + X_test_left3['SideA.ViewData.Side1_UniqueIds'].nunique()
one_id_1_final = []
    no_pair_ids_last.append(x)
                                            'BreakID_Side1_str' : 'Final_predicted_break',
# #### Cancelled Trade Removal
# In[97]:
umr_mto_table_new[['BreakID', 'Task ID']] = umr_mto_table_new[['BreakID', 'Task ID']].astype(float)
umr_otm_table = pd.DataFrame(one_id_0)
# In[119]:
'ExternalComment3','Fund',
                                                   'ViewData.Side1_UniqueIds', \
        cancel_fin.to_csv('Comment file soros 2 sep testing no original p2.csv')
]
# In[141]:
                                            'BreakID_Side0' : 'BreakID',
    df_213_1 = df_213_1[~df_213_1['SideA.final_ID'].isin(id_listA)]
side0_id = list(set(brk[brk['ViewData.Side1_UniqueIds'] =='BB']['ViewData.Side0_UniqueIds']))
#                 'SourceCombinationCode', \
        original = km[km['ViewData.Transaction ID'].isin(cancel_trade)]
    umr_otm_table_final['BreakID_Side0'].iloc[i] = umr_otm_table_final['BreakID_Side0'][i][0]
#                 .astype(str)
'ViewData.InternalComment2', 'ViewData.Description','new_pb2','new_pb1','comm_marker','monthend marker'
import decimal
result_non_trade = result_non_trade[['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']]
            id0_unique = key       
# In[206]:
# In[110]:
# In[104]:
dff6 = dffk4[sel_col]
# In[115]:
# In[94]:
X_test['SideB.ViewData.Side0_UniqueIds'].nunique() + X_test['SideA.ViewData.Side1_UniqueIds'].nunique()
final_closed_df['SourceCombinationCode'] = final_closed_df['SourceCombinationCode'].astype(str)
if dffk2[dffk2['cancel_marker'] == 1].shape[0]!=0:
filepaths_umr_otm_table_final_new = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\UAT_Run\\X_Test_' + setup_code +'\\umr_otm_table_final_new.csv'
umr_mto_table_new['Task Business Date'] = pd.to_datetime(umr_mto_table_new['Task Business Date'])
    elim = elim.reset_index()
            cancel_fin.to_csv('Comment file soros 2 sep testing p3.csv')
threshold_1_umb = threshold_1[threshold_1['count']>cliff_for_loop]['index'].unique()
umr_otm_table_final_new.rename(columns = change_col_names_umr_otm_final_new_dict, inplace = True)
        c1['predicted comment'] =  c1.apply(lambda x : cancelcomment2(x['ViewData.Settle Date1']),axis = 1)
if(umr_otm_table_final.empty == False):
    df[new_text_field_name] = df[new_text_field_name].str.replace(' usd','')
'Age','Age WK',
    df[new_text_field_name] = df[new_text_field_name].str.replace('eur','')
    umr_mto_table_new['probability_UMR'].iloc[i] = float(decimal.Decimal(random.randrange(900, 1000))/1000)
final_closed_df['probability_UMB'] = ''
final_closed_df['BusinessDate'] = pd.to_datetime(final_closed_df['BusinessDate'])
# In[213]:
# In[155]:
rr8 = rr7[~((rr7['pb_len']==1)|(rr7['acc_len']==1))]
# In[142]:
# In[172]:
    dup_array_0_list.append(list(i))
umr_mto_table_new_to_write = umr_mto_table_new[cols_for_database_new]
data['ViewData.Commission'] = data['ViewData.Commission'].fillna('NA')
one_id_0 = []
final_oto_umb_table_new['SetupID'] = setup_code 
# In[111]:
                                                   'ViewData.Task ID']
umr_ids_a_side = final_umr_table['SideA.ViewData.Side1_UniqueIds'].unique()
comment_df_final = pd.concat(comment_df_final_list)
final_closed_df['PredictedComment'] = ''
            id0_aggregation = X_test[(X_test['SideB.ViewData.Accounting Net Amount'].isin(amount_array2)) & (X_test['SideA.ViewData.Side1_UniqueIds']==key)]['SideB.ViewData.Side0_UniqueIds'].values
umr_otm_table_final['BreakID_Side0'] = ''
                                          'Transaction_Type' : ['MTM'],
            id_listB = list(set(c1['final_ID']))
test_file['description_similarity_score'] = test_file.apply(lambda x: fuzz.token_sort_ratio(x['SideA.ViewData.Description_new'], x['SideB.ViewData.Description_new']), axis=1)
# In[100]:
# ## Sample data on one date
umb_carry_forward_df[['BreakID', 'TaskID']] = umb_carry_forward_df[['BreakID', 'TaskID']].astype(float)
#                                       'SourceCombinationCode', \
result_trade['predicted category'] = pd.Series(demo)
#final_table_to_write = final_table_to_write.append([final_closed_df, \
model_cols = ['SideA.ViewData.B-P Net Amount', 
final_closed_df.rename(columns = change_names_of_final_closed_df_mapping_dict, inplace = True)
#                 'Side0_UniqueIds', \
final_closed_df['probability_UMR'] = final_closed_df['probability_UMR'].astype(str)
meo_df = meo_df.rename(columns ={'ViewData.B-P Net Amount':'ViewData.Cust Net Amount'
# ## One to One UMB segregation
one_id_1 = []
dff5 = pd.concat([dff5,dff7])
#df_mod1['Category'] = df_mod1['Category'].fillna('NA')
threshold_0_umb = threshold_0[threshold_0['count']>cliff_for_loop]['index'].unique()
# In[262]:
    for j in many_ids_0:
# In[135]:
        cancel_broker = pd.concat(appended_data)
rr4['SideA.ViewData.Side1_UniqueIds'] = rr4['SideA.ViewData.Side1_UniqueIds'].apply(tuple)
dff5 = dff5.drop('index', axis = 1)
# In[250]:
# In[264]:
umr_mto_table_new.to_csv(filepaths_umr_mto_table_new)
X_test_left2 = X_test_left2[~(X_test_left2['SideB.ViewData.Side0_UniqueIds'].isin(list(one_id_0)))]
        original['predicted category'] = 'Original of Cancelled trade'
# In[197]:
                                           'Side_meo_training_df' : Acct_meo_training_df }
rr3 = pd.merge(rr2, acc_amount, on='SideB.ViewData.Side0_UniqueIds', how='left')
final_oto_umb_table_new.rename(columns = change_col_names_final_oto_umb_table_new_dict, inplace = True)
#filepaths_MEO = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/MEO/MeoCollections.MEO_HST_RecData_' + setup_code + '_' + str(date_input) + '.csv'
    b_side_agg['No_Pair_flag'] = b_side_agg['Predicted_action'].apply(lambda x: 1 if 'No-Pair' in x else 0)
# In[95]:
# In[190]:
umr_otm_table_double_count.columns = ['SideB.ViewData.Side0_UniqueIds']
                many_ids_0.append(id0_aggregation)
os.chdir('D:\\ViteosModel\\Abhijeet - Comment')
dff4 = dff4.reset_index()
umr_mto_table_new.rename(columns = change_names_of_umr_mto_table_mapping_dict, inplace = True)
# ## Removing all the OTM and MTO Ids
        sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']
# In[204]:
X_test_left3 = X_test_left3[~(X_test_left3['SideA.ViewData.Side1_UniqueIds'].isin(final_oto_umb_table['SideA.ViewData.Side1_UniqueIds']))]
def return_int_list(list_x):
            if subset: 
result_trade['predicted comment'] = 'NA'
        values =  X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.B-P Net Amount'].values
final_df_2.rename(columns = {'Side0_UniqueIds_x' : 'Side0_UniqueIds'}, inplace = True)
final_closed_df['Side1_UniqueIds'] = final_closed_df['Side1_UniqueIds'].astype(str)
    many_ids_0_list.append(list(j))
final_oto_umb_table_new = pd.merge(final_oto_umb_table, meo_df[['ViewData.Side1_UniqueIds','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), on = 'ViewData.Side1_UniqueIds', how='left')
final_df = final_table_to_write.merge(comment_df_final_side0, on = 'Side0_UniqueIds', how = 'left')
        Transaction_type_closed_break_df = \
# In[157]:
'new_desc_cat',
        k = 'positive'
'InternalComment1','InternalComment2',
final_closed_df['probability_UMB'] = final_closed_df['probability_UMB'].astype(str)
# In[123]:
umr_otm_table_final_new['probability_UMR'] = 0.95
    temp_umr_otm_table_message = 'No One to many found'
sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','flag_side1'] = 0
    appended_data = []
umb_carry_forward_df.rename(columns = change_names_of_umb_carry_forward_df_mapping_dict, inplace = True)
final_closed_df = closed_df[closed_columns_for_updation]
rr6 = pd.merge(rr5, rr4.groupby(['SideA.ViewData.Side1_UniqueIds'])['SideB.ViewData.Accounting Net Amount'].sum().reset_index(), on='SideA.ViewData.Side1_UniqueIds', how='left')
filename = 'finalized_model_soros_trade_v9.sav'
            cancel_fin = cancel_fin.reset_index()
### Converting array to list
#UMB Carry Forward Begins
#End of Commenting
            c2['predicted comment'] = c2.apply(lambda x : cancelcomment3(x['ViewData.Settle Date1']), axis = 1)
    umr_otm_table_final_new['probability_No_pair'].iloc[i] = float(decimal.Decimal(random.randrange(50, 100))/1000)
final_oto_umb_table_new['Task Business Date'] = final_oto_umb_table_new['Task Business Date'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
# In[179]:
final_oto_umb_table_new['PredictedComment'] = ''
many_ids_0 = []
    dummy1 = dffk2[dffk2['cancel_marker']==1]
data['ViewData.Settle Date'] = pd.to_datetime(data['ViewData.Settle Date'])
final_df['PredictedComment'] = final_df['PredictedComment_y'].fillna(final_df['PredictedComment_x'])
data['comm_marker'] = data['ViewData.Commission'].apply(lambda x : comfun(x))
result_trade['predicted comment'] = result_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
change_names_of_final_closed_df_mapping_dict = {
umr_mto_table_new['ML_flag'] = 'ML'
umr_otm_table_final = pd.concat([umr_otm_table, umr_otm_table_double_count], axis=0)
                                        'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
umr_otm_table_final_new['PredictedComment'] = ''
umr_otm_table_final_new[['BreakID', 'Task ID']] = umr_otm_table_final_new[['BreakID', 'Task ID']].astype(np.int64)
X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(final_umr_table['SideA.ViewData.Side1_UniqueIds']))]
# In[122]:
    umr_mto_table.columns = ['SideA.ViewData.Side1_UniqueIds']
    if i in filtered_mto:
# ## Removing duplicate IDs from side 1
final_oto_umb_table_new.to_csv(filepaths_final_oto_umb_table_new)
                break             
        one_id_0_final.append(j) 
 'Final_predicted_break',
exceptions_0_umb_ids = exceptions_0_umb[exceptions_0_umb['count']>cliff_for_loop]['index'].unique()
umb_carry_forward_df['Side1_UniqueIds'] = umb_carry_forward_df['Side1_UniqueIds'].astype(str)
                                           'Side_meo_training_df' : BP_meo_training_df },
#remaining_ob_df[]
umr_mto_table_new['Task Business Date'] = umr_mto_table_new['Task Business Date'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
    df_213_1 = df_213_1[~df_213_1['SideB.final_ID'].isin(id_listB)]
#'OTE Custodian Account',
X_test_left['Predicted_action_2'].value_counts()
df2['new_pb1'] = df2.apply(lambda x : x['new_pb'] if x['ViewData.Prime Broker1']=='kkk' else x['ViewData.Prime Broker1'],axis = 1)
# In[106]:
        net_sum = X_test[X_test['SideB.ViewData.Side0_UniqueIds']==key]['SideB.ViewData.Accounting Net Amount'].max()
        cancel_fin.to_csv('Comment file soros 2 sep testing no original p4.csv')
X_test_left3 = X_test_left3.reset_index().drop('index',1)
result_non_trade = result_non_trade.drop('predicted comment', axis = 1)
    elim.to_csv('Comment file soros 2 sep testing p5.csv')
            # 'SideA.ViewData.Investment Type', 
        aa1 = aa1.drop('index',1)
exceptions_1_umb = X_test[X_test['Predicted_action_2']=='UMB_One_to_One']['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count')
              'SideA.ViewData.Status', 'SideA.ViewData.BreakID_A_side']
    if x=="NA":
        values2 =  X_test[(X_test['SideA.ViewData.Side1_UniqueIds']==key) & (X_test['Predicted_action_2']=='UMB_One_to_One')]['SideB.ViewData.Accounting Net Amount'].values
    dff4 = dff4.copy()
# In[191]:
umr_otm_table_final_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'Source Combination Code', 'Predicted_Status', 'ML_flag']] = umr_otm_table_final_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'Source Combination Code', 'Predicted_Status', 'ML_flag']].astype(str)
for i, j in zip(many_ids_0_list, one_id_1):
            fun_side_meo_combination_df[ \
# In[147]:
# In[96]:
'Task ID', 'Source Combination Code',
 'new_pb2','new_pb1','comm_marker','monthend marker'
filepaths_final_no_pair_table = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_no_pair_table.csv'
brk['ViewData.Side1_UniqueIds'] = brk['ViewData.Side1_UniqueIds'].fillna('BB')
for j in many_ids_0:
if elim1.shape[0]!=0:
            c1['predicted comment'] =  c1.apply(lambda x : cancelcomment2(x['ViewData.Settle Date1']),axis = 1)
umr_double_count = umr_double_count[(umr_double_count['Predicted_action']=='UMR_One_to_One') & (umr_double_count['count']==2)]
change_names_of_final_oto_umb_table_new_mapping_dict = {
#filepaths_final_no_pair_table_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_no_pair_table_copy.csv'
    id_listB = list(set(elim1['SideB.final_ID'])) 
umr_mto_table_new['Predicted_Status'] = 'UMR'
#final_oto_umb_table
# ## UMB One to Many and Many to One
                                                ]
 'Side1_UniqueIds',
    dummy = dffk2[dffk2['cancel_marker']!=1]
                one_id_1.append(id1_unique)
# In[256]:
       'ViewData.TaskvsTrade Date','new_desc_cat', 'ViewData.Custodian', 'ViewData.Net Amount Difference Absolute', 'new_pb1'
def comfun(x):
            Transaction_type_closed_break_df = \
                                           'Transaction_Type' : ['JNL'],
                                 'MTM' : {'side' : 'PB',
    umr_otm_table_final_new['probability_UMB'].iloc[i] = float(decimal.Decimal(random.randrange(50, 100))/1000)
        c1['predicted category'] = 'Cancelled trade'
X_test_left = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(umr_ids_b_side))]
    for length in range(1, 4):
com_temp = com_temp.rename(columns = {'Category ':'predicted category','template':'predicted template'})
             # 'SideA.TType', 'SideB.TType',
rr7['diff'] = rr7['SideB.ViewData.Accounting Net Amount'] - rr7['SideA.ViewData.B-P Net Amount']
                                 'DEP_WDRL' : {'side' : 'PB',
# In[193]:
# In[219]:
    no_pair_ids = []
# In[220]:
    df[new_text_field_name] = df[new_text_field_name].apply(lambda x: re.sub(r"(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|^rt|http.+?", "", x))  
 'SourceCombinationCode',
filtered_otm_flat = [item for sublist in filtered_otm for item in sublist]
df2['new_desc_cat'] = df2['desc_cat'].apply(lambda x : catcln1(x,com))
# In[158]:
#        fun_otm_mto_df['BreakID_Side1'].iloc[i] = list(fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin(fun_otm_mto_df['SideA.ViewData.Side1_UniqueIds'].iloc[i])]['ViewData.BreakID'])
frames = [dff4,dff5]
 'Predicted_Status',
    umr_otm_table_final['BreakID_Side1'].iloc[i] = list(meo_df[meo_df['ViewData.Side1_UniqueIds']\
        cancel_fin.to_csv('Comment file soros 2 sep testing p1.csv')
# In[163]:
    sideA_col = []
        dffk3 = dffk3[~dffk3['ViewData.Transaction ID'].isin(cancel_trade)]
# In[150]:
# ### One to Many
acc_id_single = []
for i in range(0,umr_mto_table_new.shape[0]):
                                 'Miscellaneous' : {'side' : 'PB',
umr_otm_table_final_new_to_write.to_csv(filepaths_umr_otm_table_final_new)
    if any(x in dup_ids_1 for x in i):
# ## Including UMR double count into OTM
        cc1 = pd.merge(aa1,bb1, left_on = 'SideB.marker', right_on = 'SideA.marker', how = 'outer')
# In[267]:
umb_carry_forward_df['PredictedCategory'] = ''
    k1 = k
        #        fun_otm_mto_df['BreakID_Side1'].iloc[i] = list(fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin(fun_otm_mto_df['SideA.ViewData.Side1_UniqueIds'].iloc[i])]['ViewData.BreakID'])
                                 'Collateral' : {'side' : 'PB',
                                            'BreakID_Side0_str' : 'Final_predicted_break',
umr_otm_table_final_new['Task Business Date'] = pd.to_datetime(umr_otm_table_final_new['Task Business Date'])
def no_pair_seg2(X_test):
from fuzzywuzzy import fuzz
    dff4 = dff4[dff4['final_ID'].isin(id_listB)]
# ### Separate Prediction of the Trade and Non trade
#meo_df.to_csv(filepaths_meo_df)
# In[192]:
result_non_trade =data21.copy()
        #print(bb1.shape)
umb_carry_forward_df['SourceCombinationCode'] = umb_carry_forward_df['SourceCombinationCode'].astype(str)
# In[261]:
    #if key not in ['1174_379879573_State Street','201_379823765_State Street']:
                                          'Transaction_Type' : ['DEB','CRED'],
    df[new_text_field_name] = df[text_field].str.lower()
        #print(aa1.shape)
final_oto_umb_table = final_oto_umb_table[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action_2','probability_No_pair_2','probability_UMB_2','probability_UMR']]
                 param_MONGO_HOST = '10.1.15.137', param_MONGO_PORT = 27017,
final_oto_umb_table_new[['BreakID', 'Task ID']] = final_oto_umb_table_new[['BreakID', 'Task ID']].astype(np.int64)
        c1 = dummy
umr_mto_table_new['probability_No_pair'] = 0.05
df2['desc_cat'] = df2['desc_cat'].apply(lambda x : currcln(x))
    'ViewData.Department',
                                          'Transaction_Type' : ['REORG'],
test_file =  clean_text(test_file,'SideB.ViewData.Description', 'SideB.ViewData.Description_new') 
# In[274]:
    df[new_text_field_name] = df[new_text_field_name].apply(lambda x: re.sub(r"\d+", "", x))
            c2['predicted category'] = 'Original of Cancelled trade'
for i, j in zip(many_ids_1_list, one_id_0):
            if length == 1 and np.isclose(number, total,atol=0.25).any():
# ### Cleaning of Description
                                        'predicted category' : 'PredictedCategory',
# In[103]:
#                 final_table_to_write[[\
                              'SideB.ViewData.Side0_UniqueIds' : 'ViewData.Side0_UniqueIds',
umr_mto_table['BreakID_Side0_str'] = [','.join(map(str,lst)) for lst in umr_mto_table['BreakID_Side0_int']]
one_side_unique_umb_ids = one_to_one_umb(X_test_umb)
threshold_0 = X_test['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count')
#                                       'Predicted_Status', \
# In[265]:
#                                       'Predicted_action', \
dffk5 = dfk_nontrade[dfk_nontrade['ViewData.Side1_UniqueIds']=='BB']
change_names_of_umr_mto_table_mapping_dict = {
        km = dffk3[dffk3['cancel_marker'] != 1]
    elim2 = elim1[sideA_col]
# In[257]:
filename = 'Soros_final_model2.sav'
umr_otm_table_final_new.to_csv(filepaths_umr_otm_table_final_new)
umr_otm_table_final_new['ML_flag'] = 'ML'
# coding: utf-8
            #print("There are no valid subsets.")
#                 'ML_flag']] = \
final_oto_umb_table = X_test_umb[X_test_umb['SideA.ViewData.Side1_UniqueIds'].isin(one_side_unique_umb_ids)]
                                        ]
final_closed_df.to_csv(filepaths_final_closed_df)
#                 'Predicted_action', \
# In[210]:
umr_double_count_left = umr_double_count[~umr_double_count['SideB.ViewData.Side0_UniqueIds'].isin(umr_otm_table['SideB.ViewData.Side0_UniqueIds'].unique())]
                              'SideB.predicted comment':'predicted comment'})
umr_double_count = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].value_counts().reset_index(name='count')
# In[146]:
rr7['pb_len'] = rr7['SideA.ViewData.Side1_UniqueIds'].apply(lambda x: len(x))
    if x == 'Geneva':
filename2 = 'Soros_final_model2_step_two.sav'
def comgen(x,y,z,k):
    umr_otm_table_final = umr_otm_table_final.reset_index().drop('index',1)
    df[new_text_field_name] = df[new_text_field_name].str.replace('usd','')
    a_side_agg['len'] = a_side_agg['Predicted_action'].str.len()
umr_mto_table['ViewData.Side1_UniqueIds'] = umr_mto_table['ViewData.Side1_UniqueIds'].astype(str)
cb_predictions = clf.predict(data211)#.astype(str)
import datetime
final_df_2['PredictedComment'] = final_df_2['PredictedComment_y'].fillna(final_df_2['PredictedComment_x'])
#                                       'Side1_UniqueIds', \
    elim = elim.drop('index', axis = 1)
X_test_left = X_test[~(X_test['SideB.ViewData.Side0_UniqueIds'].isin(no_pair_ids_b_side))]
        cancellation = dffk3[dffk3['cancel_marker'] == 1]
    elim2 = elim2.rename(columns= {'SideA.final_ID':'final_ID',
# In[154]:
umb_carry_forward_df['ML_flag'] = 'ML'
# In[212]:
#filepaths_final_no_pair_table = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_no_pair_table.csv'
final_df.rename(columns = {'Side1_UniqueIds_x' : 'Side1_UniqueIds'}, inplace = True)
              # 'ViewData.ExpiryDate', 
# In[92]:
        aa1 = aa1.reset_index()
    dffk3 = dffk3.copy()
            subset = subSum(numbers[index+1:],total-number)
umr_otm_table_final_new = pd.merge(umr_otm_table_final, meo_df[['ViewData.Side0_UniqueIds','ViewData.BreakID','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), on = 'ViewData.Side0_UniqueIds', how='left')
# In[144]:
brk['ViewData.Side0_UniqueIds'] = brk['ViewData.Side0_UniqueIds'].replace('nan','AA')
# ## Read testing data 
# ### Duplicate OB removal
comment_df_final_side1 = comment_df_final[comment_df_final['Side0_UniqueIds'] == 'AA']
#umr_mto_table['BreakID_Side0'] = umr_mto_table['SideB.ViewData.Side0_UniqueIds'].apply(lambda x : return_BreakID_list(list_x=x,fun_meo_df = meo_df))
                         })
        #print(values)
umr_mto_table_new[['BreakID', 'Task ID']] = umr_mto_table_new[['BreakID', 'Task ID']].astype(np.int64)
test_file['tt_map_flag'] = test_file.apply(lambda x: 1 if x['ViewData.Combined Transaction Type'] in Soros_umr['ViewData.Combined Transaction Type'].unique() else 0, axis=1)
# In[269]:
#                                       'probability_UMR', \
              #'SideB.Date',
umr_mto_table = pd.DataFrame(one_id_1)
        bb1 = bb1.reset_index()
for x in list(open_ids_0_last):
# In[118]:
        aa1['marker'] = 1
        if len(numbers) < length or length < 1:
# In[229]:
                         'Side1_UniqueIds':'ViewData.Side1_UniqueIds'})
# In[125]:
    elim1['SideB.predicted category'] = 'Updown'
closed_columns_for_updation = ['ViewData.BreakID','ViewData.Task Business Date','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.Source Combination Code','ViewData.Task ID']
# In[181]:
# In[203]:
umr_otm_table_double_count['SideA.ViewData.Side1_UniqueIds'] = pb_ids_otm_left
        dup_array_1.append(i)
# In[109]:
    umr_otm_table.columns = ['SideB.ViewData.Side0_UniqueIds']
X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side))]
umr_otm_table_final_new['probability_No_pair'] = 0.05
umr_mto_table_new[['SetupID']] = umr_mto_table_new[['SetupID']].astype(int)
                Transaction_type_closed_break_df[ \
umr_otm_table_final['BreakID_Side0'] = umr_otm_table_final['BreakID_Side0'].astype(int)
# In[121]:
# In[114]:
df_mod1['ViewData.Department'] = df_mod1['ViewData.Department'].fillna('nn')
#Closed Begins
data_dict = final_df_2.to_dict("records_final")
#Fill probability between 90 - 100 for umr
# In[152]:
data = data.reset_index()
        item = 'SideB.'+items
                                                   'ViewData.Side0_UniqueIds', \
# In[107]:
    if(fun_setup_code == '153'):
                                          'Transaction_Type' : ['Transfer'],
umr_otm_table_final['BreakID_Side1'] = umr_otm_table_final.apply(lambda x: list(meo_df[meo_df['ViewData.Side1_UniqueIds'].isin(umr_otm_table_final['SideA.ViewData.Side1_UniqueIds'])]['ViewData.BreakID']), axis=1)
final_closed_df[['SetupID']] = final_closed_df[['SetupID']].astype(int)
        k = 'NA'
dup_array_0_list = []
#    return [int(i) for i in list_x]
    if any(x in dup_ids_0 for x in i):
#remaining_ob_df['Side0_UniqueIds']
# In[205]:
#remaining_ob_df = pd.DataFrame()
    b_side_agg['len'] = b_side_agg['Predicted_action'].str.len()
#                                       'ML_flag']] \
final_closed_df['Predicted_Status'] = final_closed_df['Predicted_Status'].astype(str)
        bb1.columns = ['SideA.' + x  for x in bb1.columns]
#def return_int_list(list_x):
            comb = id_listB + id_listA
# In[170]:
    umr_otm_table_final_new['probability_UMR'].iloc[i] = float(decimal.Decimal(random.randrange(900, 1000))/1000)
final_closed_df['Side0_UniqueIds'] = final_closed_df['Side0_UniqueIds'].astype(str)
umr_mto_table_new.rename(columns = change_col_names_umr_mto_table_new_dict, inplace = True)
#                                       'probability_UMB', \
final_closed_df['ML_flag'] = 'ML'
threshold_0_without_umb = threshold_0[threshold_0['count']<=cliff_for_loop]['index'].unique()
    key_index =[]
# In[143]:
data222 = data22[cols]
    pb_ids_otm = X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key) & ((X_test['SideB.ViewData.Accounting Net Amount']==X_test['SideA.ViewData.B-P Net Amount']) | (X_test['SideB.ViewData.Accounting Net Amount']== (-1)*X_test['SideA.ViewData.B-P Net Amount']))]['SideA.ViewData.Side1_UniqueIds'].values
              'Trade_date_diff', 
brk = brk.rename(columns ={'Side0_UniqueIds':'ViewData.Side0_UniqueIds',
    if key in threshold_1_umb:
# In[177]:
no_pair_ids_last = list(open_ids_1_last)
X_test_left2 = X_test_left2[~(X_test_left2['SideA.ViewData.Side1_UniqueIds'].isin(list(one_id_1)))]
    many_ids_1_list.append(list(j))
