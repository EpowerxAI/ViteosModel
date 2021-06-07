# -*- coding: utf-8 -*-
"""
Created on Fri Jul 10 13:09:07 2020

@author: consultant138
"""
import pandas as pd
#Find AUA data statuses for Setups 170,1200,531 and 833

# Setup 170
import sys
import os
print(os.getcwd())
os.chdir('C:\\Users\\consultant138\\Downloads\\Viteos_Rohit\\ViteosModel')
print(os.getcwd())

orig_stdout = sys.stdout
f = open('AUA_MEO_data_statuses_for_Weiss_Setups_170_1200_531_833.txt', 'w')
sys.stdout = f

date_numbers_list_all = [1,2,3,4,
                     7,8,9,10,11,
                     14,16,17,18,
                     21,23,25,
                     28,29,30]


#Setup 170
date_numbers_list_170 = [2,10,11,21,22]
filepaths_AUA_170 = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/AUA/AUACollections.AUA_HST_RecData_170_2020-06-' + str(date_numbers_list_170[i]) + '.csv' for i in range(0,len(date_numbers_list_170))]

i = 0
print('AUA Setup 170')
for i in range(0,len(date_numbers_list_170)):
    print('For AUA 170, Date = 2020-06-' + str(date_numbers_list_170[i]))
    aua = pd.read_csv(filepaths_AUA_170[i],usecols = ['ViewData.Status'])
    print(aua['ViewData.Status'].value_counts())


filepaths_MEO_170 = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/MEO/MeoCollections.MEO_HST_RecData_170_2020-06-' + str(date_numbers_list_all[i]) + '.csv' for i in range(0,len(date_numbers_list_all))]
i = 0
print('MEO Setup 170')
for i in range(0,len(date_numbers_list_all)):
    meo = pd.read_csv(filepaths_MEO_170[i],usecols = ['ViewData.Status','ViewData.Age'])
    print('For MEO 170, Date = 2020-06-' + str(date_numbers_list_all[i]))
    print(meo[(meo['ViewData.Status'] == 'UMR') | (meo['ViewData.Status'] == 'UDB') | (meo['ViewData.Status'] == 'UMB') | (meo['ViewData.Status'] == 'UCB')].groupby(['ViewData.Status','ViewData.Age']).size())
    i = i+1

#Setup 1200
date_numbers_list_1200 = [9,10,11,18,23]
filepaths_AUA_1200 = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/AUA/AUACollections.AUA_HST_RecData_1200_2020-06-' + str(date_numbers_list_1200[i]) + '.csv' for i in range(0,len(date_numbers_list_1200))]

i = 0
for i in range(0,len(date_numbers_list_1200)):
    print('For AUA 1200, Date = 2020-06-' + str(date_numbers_list_all[i]))
    aua = pd.read_csv(filepaths_AUA_1200[i],usecols = ['ViewData.Status'])
    print(aua['ViewData.Status'].value_counts())
    i = i+1

filepaths_MEO_1200 = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/MEO/MeoCollections.MEO_HST_RecData_1200_2020-06-' + str(date_numbers_list_all[i]) + '.csv' for i in range(0,len(date_numbers_list_all))]
i = 0
print('MEO Setup 1200')
for i in range(0,len(date_numbers_list_all)):
    meo = pd.read_csv(filepaths_MEO_1200[i],usecols = ['ViewData.Status','ViewData.Age'])
    print('For MEO 1200, Date = 2020-06-' + str(date_numbers_list_all[i]))
    print(meo[(meo['ViewData.Status'] == 'UMR') | (meo['ViewData.Status'] == 'UDB') | (meo['ViewData.Status'] == 'UMB') | (meo['ViewData.Status'] == 'UCB')].groupby(['ViewData.Status','ViewData.Age']).size())
    i = i+1

#Setup 531
date_numbers_list_531 = [2,3,4,7,8,9,10,11,14,15,17,18,21,22]
filepaths_AUA_531 = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/AUA/AUACollections.AUA_HST_RecData_531_2020-06-' + str(date_numbers_list_531[i]) + '.csv' for i in range(0,len(date_numbers_list_531))]
i = 0
for i in range(0,len(date_numbers_list_531)):
    print('For 531, Date = 2020-06-' + str(date_numbers_list_531[i]))
    aua = pd.read_csv(filepaths_AUA_531[i],usecols = ['ViewData.Status'])
    print(aua['ViewData.Status'].value_counts())
    i = i+1

filepaths_MEO_531 = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/MEO/MeoCollections.MEO_HST_RecData_531_2020-06-' + str(date_numbers_list_all[i]) + '.csv' for i in range(0,len(date_numbers_list_all))]
i = 0
print('MEO Setup 1200')
for i in range(0,len(date_numbers_list_all)):
    meo = pd.read_csv(filepaths_MEO_531[i],usecols = ['ViewData.Status','ViewData.Age'])
    print('For MEO 531, Date = 2020-06-' + str(date_numbers_list_all[i]))
    print(meo[(meo['ViewData.Status'] == 'UMR') | (meo['ViewData.Status'] == 'UDB') | (meo['ViewData.Status'] == 'UMB') | (meo['ViewData.Status'] == 'UCB')].groupby(['ViewData.Status','ViewData.Age']).size())
    i = i+1



#Setup 833
date_numbers_list_833 = [1,2,4,8,9,10,11,14,15,18,23,24]
filepaths_AUA_833 = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/AUA/AUACollections.AUA_HST_RecData_833_2020-06-' + str(date_numbers_list_833[i]) + '.csv' for i in range(0,len(date_numbers_list_833))]
i = 0
for i in range(0,len(date_numbers_list_833)):
    print('For AUA 833, Date = 2020-06-' + str(date_numbers_list_833[i]))
    aua = pd.read_csv(filepaths_AUA_833[i],usecols = ['ViewData.Status'])
    print(aua['ViewData.Status'].value_counts())
    i = i+1
 
filepaths_MEO_833 = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/MEO/MeoCollections.MEO_HST_RecData_833_2020-06-' + str(date_numbers_list_all[i]) + '.csv' for i in range(0,len(date_numbers_list_all))]
i = 0
print('MEO Setup 833')
for i in range(0,len(date_numbers_list_all)):
    meo = pd.read_csv(filepaths_MEO_833[i],usecols = ['ViewData.Status','ViewData.Age'])
    print('For MEO 833, Date = 2020-06-' + str(date_numbers_list_all[i]))
    print(meo[(meo['ViewData.Status'] == 'UMR') | (meo['ViewData.Status'] == 'UDB') | (meo['ViewData.Status'] == 'UMB') | (meo['ViewData.Status'] == 'UCB')].groupby(['ViewData.Status','ViewData.Age']).size())
    i = i+1

sys.stdout = orig_stdout
f.close()