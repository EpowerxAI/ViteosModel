#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 27 12:09:45 2018

@author: shweta
"""

#!/usr/bin/env python

dbconnection = {
        'meo_connection_host': 'vitblrrecdb01.viteos.com:27017',
        'aua_connection_host': 'vitblrrecdb01.viteos.com:27017',
        'pred_connection_host': 'vitblrrecdb01.viteos.com:27017'
        #'aua_connection_host': 'localhost:27017',
}


dbname = { 
        'meo_db':'ReconDB_Soros',
        'aua_db':'ReconDB_Soros',
        'pred_db':'ReconDB_Soros'
}

collection_names = { 
        'collection_meo':'RecData_9_copy',
        'collection_aua_Cash':'AfterUserActionDataForTrainingCash',
        'collection_aua_Position':'BreakManagementOutput_Position',
        'collection_aua_Trade':'BreakManagementOutput_Trade',

	'collection_to_save_predictions_cash': 'predictions_cash'
}

model_save_path = "./"

Query = {"ViewData.Account Type": 1, "ViewData.Account Type Difference": 1, "ViewData.Account Type Difference Absolute": 1, "ViewData.Accounting Account Type": 1, "ViewData.Accounting Activity Code": 1,
"ViewData.Accounting Asset Type Category": 1, "ViewData.Accounting Base Currency": 1, "ViewData.Accounting Base Net Amount": 1, "ViewData.Accounting Bloomberg_Yellow_Key": 1, "ViewData.Accounting Break Tag": 1,
"ViewData.Accounting CUSIP": 1, "ViewData.Accounting Call Put Indicator": 1, "ViewData.Accounting Cancel Amount": 1, "ViewData.Accounting Cancel Flag": 1, "ViewData.Accounting Commission": 1, "ViewData.Accounting Currency": 1,"ViewData.Accounting Custodian": 1, "ViewData.Accounting Custodian Account": 1, "ViewData.Accounting Department": 1, "ViewData.Accounting Description": 1, "ViewData.Accounting ExpiryDate": 1, "ViewData.Accounting FX Rate": 1, "ViewData.Accounting Fund": 1, "ViewData.Accounting Group": 1, "ViewData.Accounting ISIN": 1, "ViewData.Accounting Investment ID": 1, "ViewData.Accounting Investment Type": 1, "ViewData.Accounting Net Amount": 1,
"ViewData.Accounting Non Trade Description": 1, "ViewData.Accounting OTE Ticker": 1, "ViewData.Accounting OTEIncludeFlag": 1, "ViewData.Accounting PB Account Numeric": 1, "ViewData.Accounting Portfolio ID": 1,
"ViewData.Accounting Portolio": 1, "ViewData.Accounting Price": 1, "ViewData.Accounting Prime Broker": 1, "ViewData.Accounting Quantity": 1, "ViewData.Accounting SEDOL": 1, "ViewData.Accounting Sec Fees": 1,
"ViewData.Accounting Settle Date": 1, "ViewData.Accounting Strategy": 1, "ViewData.Accounting Strike Price": 1, "ViewData.Accounting Ticker": 1, "ViewData.Accounting Trade Date": 1,
"ViewData.Accounting Transaction Category": 1, "ViewData.Accounting Transaction ID": 1, "ViewData.Accounting Transaction Type": 1, "ViewData.Accounting Underlying Cusip": 1, "ViewData.Accounting Underlying ISIN": 1,
"ViewData.Accounting Underlying Investment ID": 1, "ViewData.Accounting Underlying Sedol": 1, "ViewData.Accounting Underlying Ticker": 1, "ViewData.Accounting UserTran2": 1, "ViewData.Accounting Value Date": 1,
"ViewData.Activity Code": 1, "ViewData.Activity Code Difference": 1, "ViewData.Activity Code Difference Absolute": 1, "ViewData.Actual Fund": 1, "ViewData.Age": 1, "ViewData.Alt ID 1": 1, "ViewData.Asset Type Category": 1,
"ViewData.Asset Type Category Difference": 1, "ViewData.Asset Type Category Difference Absolute": 1, "ViewData.Assigned To": 1, "ViewData.B-P Account Type": 1, "ViewData.B-P Activity Code": 1,
"ViewData.B-P Asset Type Category": 1, "ViewData.B-P Base Currency": 1, "ViewData.B-P Base Net Amount": 1, "ViewData.B-P Bloomberg_Yellow_Key": 1, "ViewData.B-P Break Tag": 1, "ViewData.B-P CUSIP": 1,
"ViewData.B-P Call Put Indicator": 1, "ViewData.B-P Cancel Amount": 1, "ViewData.B-P Cancel Flag": 1, "ViewData.B-P Commission": 1, "ViewData.B-P Currency": 1, "ViewData.B-P Custodian": 1, "ViewData.B-P Custodian Account": 1, "ViewData.B-P Department": 1, "ViewData.B-P Description": 1, "ViewData.B-P ExpiryDate": 1, "ViewData.B-P FX Rate": 1, "ViewData.B-P Fund": 1, "ViewData.B-P Group": 1, "ViewData.B-P ISIN": 1, "ViewData.B-P Investment ID": 1,
"ViewData.B-P Investment Type": 1, "ViewData.B-P Net Amount": 1, "ViewData.B-P Non Trade Description": 1, "ViewData.B-P OTE Ticker": 1, "ViewData.B-P OTEIncludeFlag": 1, "ViewData.B-P PB Account Numeric": 1,
"ViewData.B-P Portfolio ID": 1, "ViewData.B-P Portolio": 1, "ViewData.B-P Price": 1, "ViewData.B-P Prime Broker": 1, "ViewData.B-P Quantity": 1, "ViewData.B-P SEDOL": 1, "ViewData.B-P Sec Fees": 1,
"ViewData.B-P Settle Date": 1, "ViewData.B-P Strategy": 1, "ViewData.B-P Strike Price": 1, "ViewData.B-P Ticker": 1, "ViewData.B-P Trade Date": 1, "ViewData.B-P Transaction Category": 1, "ViewData.B-P Transaction ID": 1,
"ViewData.B-P Transaction Type": 1, "ViewData.B-P Underlying Cusip": 1, "ViewData.B-P Underlying ISIN": 1, "ViewData.B-P Underlying Investment ID": 1, "ViewData.B-P Underlying Sedol": 1, "ViewData.B-P Underlying Ticker": 1,
"ViewData.B-P UserTran2": 1, "ViewData.B-P Value Date": 1, "ViewData.Balance FX Rate": 1, "ViewData.BalanceAcc": 1, "ViewData.BalancePB": 1, "ViewData.Balances Account Name": 1, "ViewData.Balances Alt ID 1": 1,
"ViewData.Balances Balance FX Rate": 1, "ViewData.Balances Fund": 1, "ViewData.Balances Fund Structure": 1, "ViewData.Balances PB Account Number": 1, "ViewData.Base Closing Balance": 1, "ViewData.Base Currency": 1,
"ViewData.Base Currency Difference": 1, "ViewData.Base Currency Difference Absolute": 1, "ViewData.Base Net Amount": 1, "ViewData.Base Net Amount Difference": 1, "ViewData.Base Net Amount Difference Absolute": 1,
"ViewData.BaseBalanceAcc": 1, "ViewData.BaseBalancePB": 1, "ViewData.Bloomberg_Yellow_Key": 1, "ViewData.Bloomberg_Yellow_Key Difference": 1, "ViewData.Bloomberg_Yellow_Key Difference Absolute": 1, "ViewData.Break Tag": 1,
"ViewData.Break Tag Difference": 1, "ViewData.Break Tag Difference Absolute": 1, "ViewData.BreakID": 1, "ViewData.Business Date": 1, "ViewData.CUSIP": 1, "ViewData.CUSIP Difference": 1, "ViewData.CUSIP Difference Absolute": 1,"ViewData.Call Put Indicator": 1, "ViewData.Call Put Indicator Difference": 1, "ViewData.Call Put Indicator Difference Absolute": 1, "ViewData.Cancel Amount": 1, "ViewData.Cancel Amount Difference": 1,
"ViewData.Cancel Amount Difference Absolute": 1, "ViewData.Cancel Flag": 1, "ViewData.Cancel Flag Difference": 1, "ViewData.Cancel Flag Difference Absolute": 1, "ViewData.Client": 1, "ViewData.ClientShortCode": 1,
"ViewData.Closing Balance": 1, "ViewData.Closing Date": 1, "ViewData.ClusterID": 1, "ViewData.CombinedAndIsPaired": 1, "ViewData.Commission": 1, "ViewData.Commission Difference": 1,
"ViewData.Commission Difference Absolute": 1, "ViewData.Currency": 1, "ViewData.Currency Difference": 1, "ViewData.Currency Difference Absolute": 1, "ViewData.Custodian": 1, "ViewData.Custodian Account": 1,
"ViewData.Custodian Account Difference": 1, "ViewData.Custodian Account Difference Absolute": 1, "ViewData.Custodian Difference": 1, "ViewData.Custodian Difference Absolute": 1, "ViewData.Department": 1,
"ViewData.Department Difference": 1, "ViewData.Department Difference Absolute": 1, "ViewData.Description": 1, "ViewData.Description Difference": 1, "ViewData.Description Difference Absolute": 1, "ViewData.ETL File Code": 1,
"ViewData.ETL Package Code": 1, "ViewData.ExpiryDate": 1, "ViewData.ExpiryDate Difference": 1, "ViewData.ExpiryDate Difference Absolute": 1, "ViewData.ExternalComment1": 1, "ViewData.ExternalComment2": 1,
"ViewData.ExternalComment3": 1, "ViewData.FX Rate": 1, "ViewData.FX Rate Difference": 1, "ViewData.FX Rate Difference Absolute": 1, "ViewData.Fund": 1, "ViewData.Fund Difference": 1, "ViewData.Fund Difference Absolute": 1,
"ViewData.Group": 1, "ViewData.Group Difference": 1, "ViewData.Group Difference Absolute": 1, "ViewData.ISIN": 1, "ViewData.ISIN Difference": 1, "ViewData.ISIN Difference Absolute": 1, "ViewData.InternalComment1": 1,
"ViewData.InternalComment2": 1, "ViewData.InternalComment3": 1, "ViewData.Investment ID": 1, "ViewData.Investment ID Difference": 1, "ViewData.Investment ID Difference Absolute": 1, "ViewData.Investment Type": 1,
"ViewData.Investment Type Difference": 1, "ViewData.Investment Type Difference Absolute": 1, "ViewData.IsPublished": 1, "ViewData.Keys": 1, "ViewData.Knowledge Date": 1, "ViewData.Label": 1,
"ViewData.Mapped Custodian Account": 1, "ViewData.Net Amount Difference": 1, "ViewData.Net Amount Difference Absolute": 1, "ViewData.Net Amount Tolerance Color": 1, "ViewData.Non Trade Description": 1,
"ViewData.Non Trade Description Difference": 1, "ViewData.Non Trade Description Difference Absolute": 1, "ViewData.OTE Ticker": 1, "ViewData.OTE Ticker Difference": 1, "ViewData.OTE Ticker Difference Absolute": 1,
"ViewData.OTEIncludeFlag": 1, "ViewData.OTEIncludeFlag Difference": 1, "ViewData.OTEIncludeFlag Difference Absolute": 1, "ViewData.PB Account": 1, "ViewData.PB Account Numeric": 1, "ViewData.PB Account Numeric Difference": 1, "ViewData.PB Account Numeric Difference Absolute": 1, "ViewData.Portfolio ID": 1, "ViewData.Portfolio ID Difference": 1, "ViewData.Portfolio ID Difference Absolute": 1, "ViewData.Portolio": 1,
"ViewData.Portolio Difference": 1, "ViewData.Portolio Difference Absolute": 1, "ViewData.Price": 1, "ViewData.Price Difference": 1, "ViewData.Price Difference Absolute": 1, "ViewData.Prime Broker": 1,
"ViewData.Prime Broker Difference": 1, "ViewData.Prime Broker Difference Absolute": 1, "ViewData.Prior Knowledge Date": 1, "ViewData.Priority": 1, "ViewData.Quantity": 1, "ViewData.Quantity Difference": 1,
"ViewData.Quantity Difference Absolute": 1, "ViewData.RecData_ID": 1, "ViewData.Recon Data Model": 1, "ViewData.Recon Purpose": 1, "ViewData.Recon Setup": 1, "ViewData.Recon Setup Code": 1, "ViewData.Reviewer": 1,
"ViewData.Rule And Key": 1, "ViewData.SEDOL": 1, "ViewData.SEDOL Difference": 1, "ViewData.SEDOL Difference Absolute": 1, "ViewData.SPM ID": 1, "ViewData.Sec Fees": 1, "ViewData.Sec Fees Difference": 1,
"ViewData.Sec Fees Difference Absolute": 1, "ViewData.Settle Date": 1, "ViewData.Settle Date Difference": 1, "ViewData.Settle Date Difference Absolute": 1, "ViewData.Source": 1, "ViewData.Source Combination": 1,
"ViewData.Source Combination Code": 1, "ViewData.SourceTypeCode": 1, "ViewData.Status": 1, "ViewData.Strategy": 1, "ViewData.Strategy Difference": 1, "ViewData.Strategy Difference Absolute": 1, "ViewData.Strike Price": 1,
"ViewData.Strike Price Difference": 1, "ViewData.Strike Price Difference Absolute": 1, "ViewData.System Comments": 1, "ViewData.TabID": 1, "ViewData.Task Business Date": 1, "ViewData.Task ID": 1,
"ViewData.Task Knowledge Date": 1, "ViewData.Ticker": 1, "ViewData.Ticker Difference": 1, "ViewData.Ticker Difference Absolute": 1, "ViewData.Trade Date": 1, "ViewData.Trade Date Difference": 1,
"ViewData.Trade Date Difference Absolute": 1, "ViewData.Transaction Category": 1, "ViewData.Transaction Category Difference": 1, "ViewData.Transaction Category Difference Absolute": 1, "ViewData.Transaction ID": 1,
"ViewData.Transaction ID Difference": 1, "ViewData.Transaction ID Difference Absolute": 1, "ViewData.Transaction Type": 1, "ViewData.Transaction Type Difference": 1, "ViewData.Transaction Type Difference Absolute": 1,
"ViewData.Type": 1, "ViewData.Underlying Cusip": 1, "ViewData.Underlying Cusip Difference": 1, "ViewData.Underlying Cusip Difference Absolute": 1, "ViewData.Underlying ISIN": 1, "ViewData.Underlying ISIN Difference": 1,
"ViewData.Underlying ISIN Difference Absolute": 1, "ViewData.Underlying Investment ID": 1, "ViewData.Underlying Investment ID Difference": 1, "ViewData.Underlying Investment ID Difference Absolute": 1,
"ViewData.Underlying Sedol": 1, "ViewData.Underlying Sedol Difference": 1, "ViewData.Underlying Sedol Difference Absolute": 1, "ViewData.Underlying Ticker": 1, "ViewData.Underlying Ticker Difference": 1,
"ViewData.Underlying Ticker Difference Absolute": 1, "ViewData.UserTran1": 1, "ViewData.UserTran2": 1, "ViewData.UserTran2 Difference": 1, "ViewData.UserTran2 Difference Absolute": 1, "ViewData.Value Date": 1,
"ViewData.Value Date Difference": 1, "ViewData.Value Date Difference Absolute": 1, "ViewData.Workflow Remark": 1, "ViewData.Workflow Status": 1, "ViewData._Actions": 1, "ViewData._GroupID": 1, "ViewData._createdAt": 1,
"ViewData._id": 1, "ViewData.Age WK": 1, "ViewData.Side0_UniqueIds": 1,"ViewData.Side1_UniqueIds": 1}

