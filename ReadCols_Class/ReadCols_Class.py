#!/usr/bin/env python
# coding: utf-8

import json

class ReadCols_Class:
	
	def __init__(self, param_columns_path, param_add_columns_path):

		with open(param_columns_path) as infile:
			self.columns_from_json = json.load(infile)

		with open(param_add_columns_path) as infile:
			self.add_columns_from_json = json.load(infile)
		
	def columns_list(self):
		return json.loads(self.columns_from_json)

	def add_columns_list(self):
		return json.loads(self.add_columns_from_json)

	def columns_json(self):
		return self.columns_from_json

	def add_columns_json(self):
		return self.add_columns_from_json



if __name__ == '__main__':
	ReadColsTest_obj = ReadCols_Class(param_columns_path = 'columns_json.json',
					  param_add_columns_path = 'add_columns_json.json')
	print(ReadColsTest_obj.columns_from_json)
	print(type(ReadColsTest_obj.columns_from_json))
	print(ReadColsTest_obj.add_columns_from_json)
	print(type(ReadColsTest_obj.add_columns_from_json))
	print(ReadColsTest_obj.columns_list())
	print(ReadColsTest_obj.add_columns_list())

