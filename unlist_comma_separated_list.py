# -*- coding: utf-8 -*-
"""
Created on Thu Oct 29 10:53:54 2020

@author: consultant138
"""

lst = ["'CITI_485 Mellon'",\
       "'1_1251077133_US BANK', '2_1251077133_US BANK', '3_1251077133_US BANK'", \
       "'1326_125890550_Morgan Stanley', '1327_125890550_Morgan Stanley', '1328_125890550_Morgan Stanley'",
       ]

def unlist_comma_separated_single_quote_string_lst(list_obj):
    new_list = []
    for i in list_obj:
        list_i = list(i.replace('\'','').split(', '))
        for j in list_i:
            new_list.append(j)
    return new_list

new_lst = unlist_comma_separated_single_quote_string_lst(lst)
