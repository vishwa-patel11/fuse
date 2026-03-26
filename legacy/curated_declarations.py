# Databricks notebook source
'''
Contains the dataset to datatype mapping for all fields.
Used as a reference to ensure expected fields and typing in the SQL database.
'''

# COMMAND ----------

curated_declarations = {
  "all_clients": {
    "Data": {
      "AppealID": "object",
      "CampaignName": "object",
      "DaysOut": "Int64",
      "FHGroup": "object",
      "FHGroupDetail": "object",
      "Fiscal": "Int64",
      "ForSort": "int64",
      "FreqID": "object",
      "GiftDate": "datetime64[ns]",
      "GiftType": "object",
      "GiftFiscal": "Int64",
      "GiftLevelID": "object",
      "GiftMonth": "object",
      "JoinSource": "object",
      "MailDate": "datetime64[ns]",
      "AmountIDLookup": "object",
      "MrcID": "object",
      "Program": "object",
      "SourceCodeID": "object",
      "State": "object",
      "SustMrcID": "object",
      "SustainerJoinSrc": "object",
      "SustainerFlag": "object",
      "SustainerGiftFlag": "object",
      "SustainerJoinFiscal": "Int64",
      "SustainerJoinMonth": "Int64",
      "JoinFiscal": "Int64",
      "JoinMonth": "Int64",
      "JoinChannel": "object",
      "GiftChannel": "object",
      "ChannelBehavior": "object",
      "Freq12ID": "object",
      "HpcID": "object",
      "Hpc12ID": "object",
      "Hpc24ID": "object",
      "Cume12ID": "object"
    },
    "FHGroup": {
      "FHgroup": "object"
    },
    "FHGroupDetail": {
      "FHGroupDetail": "object"
    },    
    "FileHealth": {
      "AppealID": "object",
      "ChannelBehavior": "object",
      "calcDate": "datetime64[ns]",
      "EndofRptPeriod": "datetime64[ns]",
      "FHGroup": "object",
      "FHGroupDetail": "object",
      "GiftDate": "object",
      "GiftType": "object",
      "GiftChannel": "object",
      "GiftFiscal": "Int64",
      "GiftLevelID": "object",
      "GiftMonth": "Int64",
      "JoinChannel": "object",
      "JoinFiscal": "Int64",
      "JoinMonth": "Int64",
      "JoinSource": "object",
      "Period": "object",
      "Program": "object",
      "ReportPeriod": "object",
      "State": "object",
      "SustainerJoinSrc": "object",
      "SustainerFlag": "object",
      "SustainerGiftFlag": "object",
      "SustainerJoinFiscal": "Int64",
      "SustainerJoinMonth": "Int64"
    },
    "FreqID": {
      "Alpha": "object",
      "Min": "float64",
      "Max": "float64",
      "FreqID": "object"
    },
    "HpcID": {
      "Alpha": "object",
      "Min": "float64",
      "Max": "float64",
      "HpcID": "object"
    },
    "MrcAmtID": {
      "Alpha": "object",
      "Min": "float64",
      "Max": "float64",
      "MrcAmtID": "object"
    },
    "MrcID": {
      "Alpha": "object",
      "Min": "int64",
      "Max": "int64",
      "MrcID": "object"
    },    
    "SustMrcID": {
      "Alpha": "object",
      "Min": "int64",
      "Max": "int64",
      "SustMrcID": "object"
    },
    "Sustainer": {
      "SustainerFlag": "object"
    }
  }
}

# COMMAND ----------

# AppealID                       object
# CampaignName                   object
# CostPerPiece                  float64
# CtrlTest                       object
# List                           object
# ListCostPerPiece               object
# ListSegment                    object
# MailDate               datetime64[ns]
# Package                        object
# Quantity                      float64
# # RolloutCostPerPiece           float64
# SourceCodeID                   object
# Fiscal                        float64
# Program                        object
# Acq                            object
# MailingMonth                  float64