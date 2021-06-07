# -*- coding: utf-8 -*-
"""
Created on Tue Dec  8 17:47:00 2020

@author: consultant138
"""
from xlrd import *
import win32com.client
import csv
import sys
import pandas as pd
from tempfile import NamedTemporaryFile
import os
xlApp = win32com.client.Dispatch("Excel.Application")
print("Excel library version:", xlApp.Version)
filename,password = r"\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\Files and Rules for Lookup\\Rules delivery from Ronson\\Schonfeld\\Schonfeld ML final\\57 Position Recon\\57 Position recon - 12042020.xlsx", 'SSA2020'
xlwb = xlApp.Workbooks.Open(filename, False, True, None, password)

xlws = xlwb.Sheets(1) # index is from 1
print (xlws.Name)
print (xlws.Cells(1, 1)) # if you need cell values

f = NamedTemporaryFile(delete=False, suffix='.csv')
f.close()
os.unlink(f.name)  

xlCSVWindows = 0x17  # CSV file format, from enum XlFileFormat
xlwb.SaveAs(Filename=f.name, FileFormat=xlCSVWindows) # Save as CSV
df = pd.read_csv(f.name)  
print(df.head())
df.to_csv('myoutput.csv',index=False)

xlws2 = xlwb.Sheets(2) # index is from 1
print (xlws2.Name)
print (xlws2.Cells(1, 1)) # if you need cell values

f2 = NamedTemporaryFile(delete=False, suffix='.csv')
f2.close()
os.unlink(f2.name)  

xlCSVWindows = 0x17  # CSV file format, from enum XlFileFormat
xlwb.SaveAs(Filename=f2.name, FileFormat=xlCSVWindows) # Save as CSV
df2 = pd.read_csv(f2.name)  
df2 = pd.read_csv(xlws2)
