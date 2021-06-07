# -*- coding: utf-8 -*-
"""
Created on Sat May 22 17:39:07 2021

@author: riteshkumar.patra
"""

import win32com.client as win32

def openWorkbook(xlapp, xlfile):
    try:        
        xlwb = xlapp.Workbooks(xlfile)            
    except Exception as e:
        try:
            xlwb = xlapp.Workbooks.Open(xlfile)
        except Exception as e:
            print(e)
            xlwb = None                    
    return(xlwb)

try:
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    wb = openWorkbook(excel, '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\Files and Rules for Lookup\\Rules delivery from Ronson\\Schonfeld\\Schonfeld ML final\\57 Position Recon\\57 Position recon - 05192021.xlsx')
    ws = wb.Worksheets('FIFTY_SEVEN') 
    excel.Visible = True

except Exception as e:
    print(e)

finally:
    # RELEASES RESOURCES
    ws = None
    wb = None
    excel = None
    
import pandas as pd
import xlwings as xw

PATH = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\Files and Rules for Lookup\\Rules delivery from Ronson\\Schonfeld\\Schonfeld ML final\\57 Position Recon\\57 Position recon - 05192021.xlsx'
wb = xw.Book(PATH)
FIFTY_SEVEN_sheet = wb.sheets['FIFTY_SEVEN']
FIFTY_SEVENCAY_sheet = wb.sheets['FIFTY_SEVENCAY']

df_FIFTY_SEVEN = FIFTY_SEVENCAY_sheet[]
df_FIFTY_SEVENCAY = FIFTY_SEVENCAY_sheet['A1:C4'].options(pd.DataFrame, index=False, header=True).value
df    



import pandas as pd
import win32com.client
import os
import getpass
full_name = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\Files and Rules for Lookup\\Rules delivery from Ronson\\Schonfeld\\Schonfeld ML final\\57 Position Recon\\57 Position recon - 05192021.xlsx'
xl_app = win32com.client.Dispatch('Excel.Application')
#pwd = getpass.getpass('Enter file password: ')
pwd = 'SSA2020'

xl_wb = xl_app.Workbooks.Open(full_name, False, True, None, pwd)
xl_app.Visible = False
xl_sh = xl_wb.Worksheets('FIFTY_SEVEN')

row_num = 4
cell_val = ''
while cell_val != None:
#    row_num += 1
#    cell_val = xl_sh.Cells(row_num, 1).Value
    cell_val = xl_sh.Cells(row_num, 1).Value
    # print(row_num, '|', cell_val, type(cell_val))
    row_num = row_num + 1
#last_row = row_num - 1
last_row = row_num - 2
# print(last_row)

# Get last_column
col_num = 1
cell_val = ''
while cell_val != None:
#    col_num += 1
    cell_val = xl_sh.Cells(4, col_num).Value
    col_num = col_num + 1
    # print(col_num, '|', cell_val, type(cell_val))
#last_col = col_num - 1
last_col = col_num - 2
# print(last_col)

content = xl_sh.Range(xl_sh.Cells(4, 1), xl_sh.Cells(last_row, last_col)).Value
# list(content)
df = pd.DataFrame(list(content[1:]), columns=content[0])
df.head()