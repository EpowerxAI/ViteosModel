# -*- coding: utf-8 -*-
"""
Created on Tue Jun  1 16:29:22 2021

@author: riteshkumar.patra
"""

import os
import numpy as np

#dir_path = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Weiss\\'
dir_path = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Weiss\\aua_data\\'
os.chdir(dir_path)
all_files_in_dir = os.listdir()

setup_code = '125'
prefix_of_files_to_remove = ['final_umb_ob_table_copy_setup_',
                             'final_smb_ob_table_copy_setup_',
                             'umb_carry_forward_df_setup_',
                             'final_umr_table_copy_new_to_write_setup_',
                             'final_oto_umb_table_copy_new_to_write_setup_',
                             'final_closed_df_setup_',
                             'ob_carry_forward_df_setup_',
                             'final_mtm_table_copy_new_to_write_setup_',
                             'final_umt_table_copy_new_to_write_setup_',
                             'df_to_append_setup_',
                             'final_mto_table_copy_setup_',
                             'umb_smb_duplication_issue_df_setup_',
                             'mtm_df_ex_and_fx_copy_new_setup_',
                             'mtm_df_ex_and_fx_copy_new_setup_',
                             'final_table_to_write_setup_',
                             'final_mtm_table_copy_new_setup_',
                             'dup_otm_table_new_final_setup_',
                             'comment_table_eq_swap_copy_to_write_setup_',
                             'aua_df_setup_',
                             'dup_otm_table_new_raw_setup_',
                             'final_otm_table_copy_new_to_write_setup_',
                             'final_mto_table_copy_new_to_write_setup_',
                             'X_Test_for_Pratik_setup_',
                             'mtm_df_eqs_setup_',
                             'final_df_2_after_making_umt_from_umr_setup_',
                             'dup_otm_table_new_setup_',
                             'final_pair_table_umr_one_to_one_setup_',
                             'aua_df__setup_'
                             ]

prefix_list = [i + str(setup_code) + '_date_' for i in prefix_of_files_to_remove]

all_files_to_remove = []
for prefix in prefix_list:
    files_to_remove_with_prefix = [filename for filename in all_files_in_dir if filename.startswith(prefix)]
    all_files_to_remove.append(files_to_remove_with_prefix)

substring_in_files_to_remove = ['_TaskID_']
for substring in substring_in_files_to_remove:
    files_to_remove_with_substring = [filename for filename in all_files_in_dir if substring in filename]
    all_files_to_remove.append(files_to_remove_with_substring)
    
all_files_to_remove_updated = sum(all_files_to_remove, [])
print(str(len(all_files_to_remove_updated)))

for filename in all_files_to_remove_updated:
    os.remove(filename)

 
#MEO_Collections
#meo_datewise_dir_path = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Weiss\\JuneData\\MEO\\'
meo_datewise_dir_path = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Weiss\\JuneData\\Both_MEO_AUA_combined\\'
os.chdir(meo_datewise_dir_path)
all_files_in_dir = os.listdir()

#setup_code = '531'
Weiss_setup_code_list = ['125','123','531','170','833','1200']
#prefix_of_meo_files_to_remove = ['MeoCollections.MEO_HST_RecData_']
prefix_of_meo_files_to_remove = ['MeoCollections.HST_RecData_']
for setup_code in Weiss_setup_code_list:
    print(str(setup_code))    
    prefix_meo_list = [i + str(setup_code) for i in prefix_of_meo_files_to_remove]
    
    all_files_to_remove = []
    
    for prefix in prefix_meo_list:
        files_to_remove_with_prefix_meo = [filename for filename in all_files_in_dir if filename.startswith(prefix)]
        all_files_to_remove.append(files_to_remove_with_prefix_meo)
        
    all_files_to_remove_updated = sum(all_files_to_remove, [])
    print(str(len(all_files_to_remove_updated)))
    
    for filename in all_files_to_remove_updated:
        os.remove(filename)
    


print('Deleting aua datewise files Weiss')
aua_datewise_Weiss_dir_path = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Weiss\\JuneData\\AUA\\'
os.chdir(aua_datewise_Weiss_dir_path)
all_files_in_dir = os.listdir()

Weiss_setup_code_list = ['125','123','531','170','833','1200']
prefix_of_aua_files_to_remove = ['AUACollections.AUA_HST_RecData_']
for setup_code in Weiss_setup_code_list:
    print(str(setup_code))    
    prefix_meo_list = [i + str(setup_code) for i in prefix_of_aua_files_to_remove]
    
    all_files_to_remove = []
    
    for prefix in prefix_meo_list:
        files_to_remove_with_prefix_meo = [filename for filename in all_files_in_dir if filename.startswith(prefix)]
        all_files_to_remove.append(files_to_remove_with_prefix_meo)
        
    all_files_to_remove_updated = sum(all_files_to_remove, [])
    print(str(len(all_files_to_remove_updated)))
    
    for filename in all_files_to_remove_updated:
        os.remove(filename)
    
print('Deleting aua datefile files Lombard')
aua_Lombard_datewise_dir_path = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Lombard\\aua_data\\'
os.chdir(aua_Lombard_datewise_dir_path)
all_files_in_dir = os.listdir()
print(str(len(all_files_in_dir)))
Lombard_setup_code_list = ['455','249']
prefix_of_aua_files_to_remove = ['aua_df_setup_']
for setup_code in Lombard_setup_code_list:
    print(str(setup_code))    
    prefix_meo_list = [i + str(setup_code) for i in prefix_of_aua_files_to_remove]
    
    all_files_to_remove = []
    
    for prefix in prefix_meo_list:
        files_to_remove_with_prefix_meo = [filename for filename in all_files_in_dir if filename.startswith(prefix)]
        all_files_to_remove.append(files_to_remove_with_prefix_meo)
        
    all_files_to_remove_updated = sum(all_files_to_remove, [])
    print(str(len(all_files_to_remove_updated)))
    
    for filename in all_files_to_remove_updated:
        os.remove(filename)

print('Deleting meo datefile files Lombard')
meo_Lombard_datewise_dir_path = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Lombard\\meo_data\\'
os.chdir(meo_Lombard_datewise_dir_path)
all_files_in_dir = os.listdir()
print(str(len(all_files_in_dir)))
Lombard_setup_code_list = ['455','249']
prefix_of_meo_files_to_remove = ['meo_df_setup_']
for setup_code in Lombard_setup_code_list:
    print(str(setup_code))    
    prefix_meo_list = [i + str(setup_code) for i in prefix_of_meo_files_to_remove]
    
    all_files_to_remove = []
    
    for prefix in prefix_meo_list:
        files_to_remove_with_prefix_meo = [filename for filename in all_files_in_dir if filename.startswith(prefix)]
        all_files_to_remove.append(files_to_remove_with_prefix_meo)
        
    all_files_to_remove_updated = sum(all_files_to_remove, [])
    print(str(len(all_files_to_remove_updated)))
    
    for filename in all_files_to_remove_updated:
        os.remove(filename)


print('Deleting Lombard main folder files')
dir_path = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Lombard\\'
os.chdir(dir_path)
all_files_in_dir = os.listdir()

Lombard_setup_code_list = ['455','249']
for setup_code in Lombard_setup_code_list:
    print(setup_code)
    prefix_of_files_to_remove = ['final_df_copy_setup_',
                                 'final_df_2_setup_',
                                 'final_umb_ob_table_copy_setup_',
                                 'final_smb_ob_table_copy_setup_',
                                 'umb_carry_forward_df_setup_',
                                 'final_umr_table_copy_new_to_write_setup_',
                                 'final_oto_umb_table_copy_new_to_write_setup_',
                                 'final_closed_df_setup_',
                                 'ob_carry_forward_df_setup_',
                                 'final_mtm_table_copy_new_to_write_setup_',
                                 'final_umt_table_copy_new_to_write_setup_',
                                 'df_to_append_setup_',
                                 'final_mto_table_copy_setup_',
                                 'umb_smb_duplication_issue_df_setup_',
                                 'mtm_df_ex_and_fx_copy_new_setup_',
                                 'mtm_df_ex_and_fx_copy_new_setup_',
                                 'final_table_to_write_setup_',
                                 'final_mtm_table_copy_new_setup_',
                                 'dup_otm_table_new_final_setup_',
                                 'comment_table_eq_swap_copy_to_write_setup_',
                                 'aua_df_setup_',
                                 'dup_otm_table_new_raw_setup_',
                                 'final_otm_table_copy_new_to_write_setup_',
                                 'final_mto_table_copy_new_to_write_setup_',
                                 'X_Test_for_Pratik_setup_',
                                 'mtm_df_eqs_setup_',
                                 'final_df_2_after_making_umt_from_umr_setup_',
                                 'dup_otm_table_new_setup_',
                                 'final_pair_table_umr_one_to_one_setup_',
                                 'aua_df__setup_'
                                 ]
    
    prefix_list = [i + str(setup_code) + '_date_' for i in prefix_of_files_to_remove]
    
    all_files_to_remove = []
    for prefix in prefix_list:
        files_to_remove_with_prefix = [filename for filename in all_files_in_dir if filename.startswith(prefix)]
        all_files_to_remove.append(files_to_remove_with_prefix)
    
    substring_in_files_to_remove = ['_TaskID_']
    for substring in substring_in_files_to_remove:
        files_to_remove_with_substring = [filename for filename in all_files_in_dir if substring in filename]
        all_files_to_remove.append(files_to_remove_with_substring)
        
    all_files_to_remove_updated = sum(all_files_to_remove, [])
    print(str(len(all_files_to_remove_updated)))
    
    for filename in all_files_to_remove_updated:
        os.remove(filename)






print('Deleting Soros main folder files')
dir_path = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Soros\\'
os.chdir(dir_path)
all_files_in_dir = os.listdir()

Soros_setup_code_list = ['149','153','179','205','206','239','']
for setup_code in Soros_setup_code_list:
    print(setup_code)
    prefix_of_files_to_remove = ['final_df_copy_setup_',
                                 'final_df_2_setup_',
                                 'final_umb_ob_table_copy_setup_',
                                 'final_smb_ob_table_copy_setup_',
                                 'umb_carry_forward_df_setup_',
                                 'final_umr_table_copy_new_to_write_setup_',
                                 'final_oto_umb_table_copy_new_to_write_setup_',
                                 'final_closed_df_setup_',
                                 'ob_carry_forward_df_setup_',
                                 'final_mtm_table_copy_new_to_write_setup_',
                                 'final_umt_table_copy_new_to_write_setup_',
                                 'df_to_append_setup_',
                                 'final_mto_table_copy_setup_',
                                 'umb_smb_duplication_issue_df_setup_',
                                 'mtm_df_ex_and_fx_copy_new_setup_',
                                 'mtm_df_ex_and_fx_copy_new_setup_',
                                 'final_table_to_write_setup_',
                                 'final_mtm_table_copy_new_setup_',
                                 'dup_otm_table_new_final_setup_',
                                 'comment_table_eq_swap_copy_to_write_setup_',
                                 'aua_df_setup_',
                                 'dup_otm_table_new_raw_setup_',
                                 'final_otm_table_copy_new_to_write_setup_',
                                 'final_mto_table_copy_new_to_write_setup_',
                                 'X_Test_for_Pratik_setup_',
                                 'mtm_df_eqs_setup_',
                                 'final_df_2_after_making_umt_from_umr_setup_',
                                 'dup_otm_table_new_setup_',
                                 'final_pair_table_umr_one_to_one_setup_',
                                 'aua_df__setup_',
                                 'meo_df_setup_'
                                 ]
    
    prefix_list = [i + str(setup_code) + '_date_' for i in prefix_of_files_to_remove]
    
    all_files_to_remove = []
    for prefix in prefix_list:
        files_to_remove_with_prefix = [filename for filename in all_files_in_dir if filename.startswith(prefix)]
        all_files_to_remove.append(files_to_remove_with_prefix)
    
    substring_in_files_to_remove = ['_TaskID_']
    for substring in substring_in_files_to_remove:
        files_to_remove_with_substring = [filename for filename in all_files_in_dir if substring in filename]
        all_files_to_remove.append(files_to_remove_with_substring)
        
    all_files_to_remove_updated = sum(all_files_to_remove, [])
    print(str(len(all_files_to_remove_updated)))
    
    for filename in all_files_to_remove_updated:
        os.remove(filename)


print('Deleting meo datewise files Soros')
meo_datewise_Soros_dir_path = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Soros\\JuneData\\MEO\\'
os.chdir(meo_datewise_Soros_dir_path)
all_files_in_dir = os.listdir()

Soros_setup_code_list = ['149','153','179','205','206','239','']
prefix_of_meo_files_to_remove_Soros = ['MeoCollections_SOROS.MEO_HST_RecData_','MEO_HST_RecData_']
for setup_code in Soros_setup_code_list:
    print(str(setup_code))    
    prefix_meo_list = [i + str(setup_code) for i in prefix_of_meo_files_to_remove_Soros]
    
    all_files_to_remove = []
    
    for prefix in prefix_meo_list:
        files_to_remove_with_prefix_meo = [filename for filename in all_files_in_dir if filename.startswith(prefix)]
        all_files_to_remove.append(files_to_remove_with_prefix_meo)
        
    all_files_to_remove_updated = sum(all_files_to_remove, [])
    print(str(len(all_files_to_remove_updated)))
    
    for filename in all_files_to_remove_updated:
        os.remove(filename)

print('Deleting aua datewise files Soros')
aua_datewise_Soros_dir_path = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Soros\\JuneData\\AUA\\'
os.chdir(aua_datewise_Soros_dir_path)
all_files_in_dir = os.listdir()

Soros_setup_code_list = ['149','153','179','205','206','239','']
prefix_of_aua_files_to_remove_Soros = ['AUACollections_SOROS.AUA_HST_RecData_','AUA_HST_RecData_']
for setup_code in Soros_setup_code_list:
    print(str(setup_code))    
    prefix_meo_list = [i + str(setup_code) for i in prefix_of_aua_files_to_remove_Soros]
    
    all_files_to_remove = []
    
    for prefix in prefix_meo_list:
        files_to_remove_with_prefix_meo = [filename for filename in all_files_in_dir if filename.startswith(prefix)]
        all_files_to_remove.append(files_to_remove_with_prefix_meo)
        
    all_files_to_remove_updated = sum(all_files_to_remove, [])
    print(str(len(all_files_to_remove_updated)))
    
    for filename in all_files_to_remove_updated:
        os.remove(filename)

print('Deleting X_Test datewise files Soros')
x_test_datewise_Soros_dir_path = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Soros\\JuneData\\'
Soros_setup_code_list = ['149','153','179','205','206','239','213','173']
for setup_code in Soros_setup_code_list:
    print(str(setup_code))    
    x_test_setup_code_folder = str(x_test_datewise_Soros_dir_path) + 'X_Test_' + str(setup_code)
    os.chdir(str(x_test_datewise_Soros_dir_path) + 'X_Test_' + str(setup_code))
    
    all_files_in_dir = os.listdir()
    if(len(all_files_in_dir) != 0):
        prefix_of_X_test_files_to_remove_Soros = ['x_test_']
        
        prefix_X_test_list = [i + str(setup_code) for i in prefix_of_aua_files_to_remove_Soros]
        
        all_files_to_remove = []
        
        for prefix in prefix_X_test_list:
            files_to_remove_with_prefix_X_test = [filename for filename in all_files_in_dir if filename.startswith(prefix)]
            all_files_to_remove.append(files_to_remove_with_prefix_X_test)
            
        all_files_to_remove_updated = sum(all_files_to_remove, [])
        print(str(len(all_files_to_remove_updated)))
        
        for filename in all_files_to_remove_updated:
            os.remove(filename)

print('Deleting Schonfeld main folder files')
dir_path = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Schonfeld\\'
os.chdir(dir_path)
all_files_in_dir = os.listdir()

Schonfeld_setup_code_list = ['897','85']
for setup_code in Schonfeld_setup_code_list:
    print(setup_code)
    prefix_of_files_to_remove = ['final_df_copy_setup_',
                                 'final_df_2_setup_',
                                 'final_umb_ob_table_copy_setup_',
                                 'final_smb_ob_table_copy_setup_',
                                 'umb_carry_forward_df_setup_',
                                 'final_umr_table_copy_new_to_write_setup_',
                                 'final_oto_umb_table_copy_new_to_write_setup_',
                                 'final_closed_df_setup_',
                                 'ob_carry_forward_df_setup_',
                                 'final_mtm_table_copy_new_to_write_setup_',
                                 'final_umt_table_copy_new_to_write_setup_',
                                 'df_to_append_setup_',
                                 'final_mto_table_copy_setup_',
                                 'umb_smb_duplication_issue_df_setup_',
                                 'mtm_df_ex_and_fx_copy_new_setup_',
                                 'mtm_df_ex_and_fx_copy_new_setup_',
                                 'final_table_to_write_setup_',
                                 'final_mtm_table_copy_new_setup_',
                                 'dup_otm_table_new_final_setup_',
                                 'comment_table_eq_swap_copy_to_write_setup_',
                                 'aua_df_setup_',
                                 'dup_otm_table_new_raw_setup_',
                                 'final_otm_table_copy_new_to_write_setup_',
                                 'final_mto_table_copy_new_to_write_setup_',
                                 'X_Test_for_Pratik_setup_',
                                 'mtm_df_eqs_setup_',
                                 'final_df_2_after_making_umt_from_umr_setup_',
                                 'dup_otm_table_new_setup_',
                                 'final_pair_table_umr_one_to_one_setup_',
                                 'aua_df__setup_',
                                 'meo_df_setup_',
                                 'df2_setup_',
                                 'df3_setup_',
                                 'df4_setup'
                                 ]
    
    prefix_list = [i + str(setup_code) + '_date_' for i in prefix_of_files_to_remove]
    
    all_files_to_remove = []
    for prefix in prefix_list:
        files_to_remove_with_prefix = [filename for filename in all_files_in_dir if filename.startswith(prefix)]
        all_files_to_remove.append(files_to_remove_with_prefix)
    
    substring_in_files_to_remove = ['_TaskID_']
    for substring in substring_in_files_to_remove:
        files_to_remove_with_substring = [filename for filename in all_files_in_dir if substring in filename]
        all_files_to_remove.append(files_to_remove_with_substring)
        
    all_files_to_remove_updated = sum(all_files_to_remove, [])
    print(str(len(all_files_to_remove_updated)))
    
    for filename in all_files_to_remove_updated:
        os.remove(filename)


print('Deleting OakTree main folder files')
dir_path = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\OakTree\\'
os.chdir(dir_path)
all_files_in_dir = os.listdir()

OakTree_setup_code_list = ['379']
for setup_code in OakTree_setup_code_list:
    print(setup_code)
    prefix_of_files_to_remove = ['final_df_copy_setup_',
                                 'final_df_2_setup_',
                                 'final_umb_ob_table_copy_setup_',
                                 'final_smb_ob_table_copy_setup_',
                                 'umb_carry_forward_df_setup_',
                                 'final_umr_table_copy_new_to_write_setup_',
                                 'final_oto_umb_table_copy_new_to_write_setup_',
                                 'final_closed_df_setup_',
                                 'ob_carry_forward_df_setup_',
                                 'final_mtm_table_copy_new_to_write_setup_',
                                 'final_umt_table_copy_new_to_write_setup_',
                                 'df_to_append_setup_',
                                 'final_mto_table_copy_setup_',
                                 'umb_smb_duplication_issue_df_setup_',
                                 'mtm_df_ex_and_fx_copy_new_setup_',
                                 'mtm_df_ex_and_fx_copy_new_setup_',
                                 'final_table_to_write_setup_',
                                 'final_mtm_table_copy_new_setup_',
                                 'dup_otm_table_new_final_setup_',
                                 'comment_table_eq_swap_copy_to_write_setup_',
                                 'aua_df_setup_',
                                 'dup_otm_table_new_raw_setup_',
                                 'final_otm_table_copy_new_to_write_setup_',
                                 'final_mto_table_copy_new_to_write_setup_',
                                 'X_Test_for_Pratik_setup_',
                                 'mtm_df_eqs_setup_',
                                 'final_df_2_after_making_umt_from_umr_setup_',
                                 'dup_otm_table_new_setup_',
                                 'final_pair_table_umr_one_to_one_setup_',
                                 'aua_df__setup_',
                                 'meo_df_setup_',
                                 'df1_setup_',
                                 'df2_setup_',
                                 'df3_setup_',
                                 'df4_setup',
                                 'umb_subsum_df_copy_setup_',
                                 'Raman_meo_df_setup_',
                                 'mtm_df_full_umb_copy_setup_',
                                 'final_umr_table_setup_',
                                 'final_table_to_write_before_comment_setup_',
                                 'final_oto_umb_table_new_to_write_setup_',
                                 'final_oto_umb_table_new_setup_',
                                 'final_no_pair_table_setup_',
                                 'final_df_setup_',
                                 'final_df_2_before_making_umt_from_umr_setup_',
                                 'comment_df_final_setup_',
                                 'brk_setup_'
                                 ]
    
    prefix_list = [i + str(setup_code) + '_date_' for i in prefix_of_files_to_remove]
    
    all_files_to_remove = []
    for prefix in prefix_list:
        files_to_remove_with_prefix = [filename for filename in all_files_in_dir if filename.startswith(prefix)]
        all_files_to_remove.append(files_to_remove_with_prefix)
    
    substring_in_files_to_remove = ['_TaskID_']
    for substring in substring_in_files_to_remove:
        files_to_remove_with_substring = [filename for filename in all_files_in_dir if substring in filename]
        all_files_to_remove.append(files_to_remove_with_substring)
        
    all_files_to_remove_updated = sum(all_files_to_remove, [])
    print(str(len(all_files_to_remove_updated)))
    
    for filename in all_files_to_remove_updated:
        os.remove(filename)
