#!/usr/bin/env python
# coding: utf-8
""" 
Created on Tue May 28 19:35:29 2020

@author: consultant138
"""
import json

class Read_Class:
	
	def __init__(self, param_columns_path = '../columns_json.json', param_add_columns_path = '../add_columns_json.json', param_default_ssh_path = '../default_ssh.json', param_default_mongo_path = '../default_mongo.json', param_client_info_path = '../weiss_client_info_json.json', param_common_cols_path = '../common_cols_json.json'):
	
		with open(param_columns_path) as infile:
			self.columns_from_json = json.load(infile)
		
		with open(param_add_columns_path) as infile:
			self.add_columns_from_json = json.load(infile)
		
		with open(param_common_cols_path) as infile:
			self.common_columns_from_json = json.load(infile)

		with open(param_default_ssh_path) as infile:
			self.default_ssh_from_json = json.load(infile)
		
		with open(param_default_mongo_path) as infile:
			self.default_mongo_from_json = json.load(infile)
		
				
		self.columns_list = json.loads(self.columns_from_json)
		self.add_columns_list = json.loads(self.add_columns_from_json)
		self.common_columns_list = json.loads(self.common_columns_from_json)
		self.all_columns_list = self.columns_list + self.add_columns_list

		with open(param_client_info_path) as infile:
			self.client_info_dict = json.load(infile)
		
	def columns_list(self):
		return self.columns_list

	def add_columns_list(self):
		return self.add_columns_list
	
	def common_columns_list(self):
		return self.common_columns_list

	def columns_json(self):
		return self.columns_from_json

	def add_columns_json(self):
		return self.add_columns_from_json

	def common_columns_json(self):
		return self.common_columns_from_json

	def all_columns_list(self):
		return self.all_columns_list

	def all_columns_json(self):
		return self.columns_from_json + self.add_columns_from_json

	def list_check_and_add_str(self, param_check_string, param_check_and_add_list):
		list_startwith_string = [i for i in param_check_and_add_list if i.startswith(param_check_string)]
		list_not_startwith_string = [i for i in param_check_and_add_list if i not in list_startwith_string]
		list_all_startwith_string = [param_check_string + i for in list_not_startwith_string]
		list_all_startwith_string = list_startwith_string + list_all_startwith_string
		return list_all_startwith_string

	def all_columns_query(self):
		self.all_columns_startwith_ViewData_list = self.list_check_and_add_str(param_check_string = 'ViewData.', param_check_and_add_list = self.columns_list) + self.list_check_and_add_str(param_check_string = 'ViewData.', param_check_and_add_list = self.add_columns_list)
		self.all_columns_startwith_ViewData_dict = {i : 1 for i in self.all_columns_startwith_ViewData_list}
		return self.all_columns_startwith_ViewData_dict

	def common_columns_query(self):
		self.common_columns_startwith_ViewData_list = self.list_check_and_add_str(param_check_string = 'ViewData.', param_check_and_add_list = self.common_columns_list)
		self.common_columns_startwith_ViewData_dict = {i : 1 for i in self.common_columns_startwith_ViewData_list}
		return self.common_columns_startwith_ViewData_dict

	def ssh_host(self):
		return self.default_ssh_from_json['DEFAULT_SSH_HOST']

	def ssh_port(self):
		return self.default_ssh_from_json('DEFAULT_SSH_PORT')

	def ssh_username(self):
		return self.default_ssh_from_json('DEFAULT_SSH_USERNAME')

	def ssh_password(self):
		return self.default_ssh_from_json('DEFAULT_SSH_PASSWORD')

	def mongo_host(self):
		return self.default_mongo_from_json('DEFAULT_MONGO_HOST')

	def mongo_port(self):
		return self.default_mongo_from_json('DEFAULT_MONGO_PORT')

	def mongo_username(self):
		return self.default_mongo_from_json('DEFAULT_MONGO_USERNAME')
	
	def mongo_password(self):
		return self.default_mongo_from_json('DEFAULT_MONGO_PASSWORD')
	
	def client_info(self):
		return self.client_info_dict

if __name__ == '__main__':
	ReadColsTest_obj = ReadCols_Class(param_columns_path = 'columns_json.json',
					  param_add_columns_path = 'add_columns_json.json')
	print(ReadColsTest_obj.columns_from_json)
	print(type(ReadColsTest_obj.columns_from_json))
	print(ReadColsTest_obj.add_columns_from_json)
	print(type(ReadColsTest_obj.add_columns_from_json))
	print(ReadColsTest_obj.columns_list())
	print(ReadColsTest_obj.add_columns_list())

