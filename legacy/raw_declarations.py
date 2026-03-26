# Databricks notebook source
'''
Contains the raw file name, encoding, expected fields and datatypes mapping for all client files.
Serves as a reference to validate incoming data and to cast, prune, and relabel the data as appropriate and to the extent possible.
'''

# COMMAND ----------

raw_declarations = {
  "ALSAC": {
      "Data.csv": {
          "encoding": "utf-8",
          "dtypes": {
              "PCADonorLoyaltyNDMRolling" : "object",
              "PCAAcqSrcNDMProgram" : "object",
              "SrcActivityID" : "object",
              "SrcCampaignID" : "object",
              "SrcInitiativeID" : "int64",
              "PCADonorSegment" : "object",
              "PCARFMRecency" : "object",
              "PCARFMFrequency24Mo" : "object",
              "PCARFMMonetary" : "object",
              "PCACost" : "float64",
              "PCANbrOfGifts" : "int64",
              "PCANbrMailed" : "int64",
              "PCARevenue" : "float64",
              "SrcMailDate" : "object",
              "SrcFiscalYr" : "int64",
              "SrcPackageDesc" : "object",
              "Ethnicity" : "object",
              "Dataset" : "object",
              "DateModified" : "object",
              "filename" : "object"
            }
        }
    }      
}

# COMMAND ----------

