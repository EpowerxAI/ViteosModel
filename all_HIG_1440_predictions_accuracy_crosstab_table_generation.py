# -*- coding: utf-8 -*-
"""
Created on Fri Jul 10 19:58:50 2020

@author: consultant138
"""

import pandas as pd

#from sklearn.metrics import accuracy_score 
from sklearn.metrics import classification_report

filepaths_accuracy_table_1440 = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/HIG_Capital/JuneData/Final_Predictions_1440/Accuracy_Table_HST_RecData_1440_all_June_data.csv'
filepaths_crosstab_table_1440 = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/HIG_Capital/JuneData/Final_Predictions_1440/Crosstab_Table_HST_RecData_1440_all_June_data.csv'

final_1440 = pd.read_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/HIG_Capital/JuneData/Final_Predictions_1440/All_June_predictions_1440.csv')
print('classification_report')
print(classification_report(final_1440[final_1440['Type']=='One_to_One/Open']['Actual_Status'], final_1440[final_1440['Type']=='One_to_One/Open']['Predicted_Status']))


# In[5675]:


report = classification_report(final_1440[final_1440['Type']=='One_to_One/Open']['Actual_Status'], final_1440[final_1440['Type']=='One_to_One/Open']['Predicted_Status'], output_dict=True)
accuracy_table = pd.DataFrame(report).transpose()
    
print('accuracy_table')
print(accuracy_table)

# In[5676]:


crosstab_table = pd.crosstab(final_1440[final_1440['Type']=='One_to_One/Open']['Actual_Status'], final_1440[final_1440['Type']=='One_to_One/Open']['Predicted_Status'])
    
    
    # In[5678]:
    
print('crosstab_table')
print(crosstab_table)

accuracy_table.to_csv(filepaths_accuracy_table_1440)
    
    
    # In[ ]:
    
    
crosstab_table.to_csv(filepaths_crosstab_table_1440)

