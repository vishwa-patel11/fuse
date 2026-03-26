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
      "MrcAmtID": "object",
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
  },
    "TSE": {
      "Data": {
        "BvaFilter": "object",
        "CampaignID": "object",
        "DonorID": "object",
        "GiftAmount": "float64",
        "GiftDatePosted": "datetime64[ns]",
        "GiftPostedFiscal": "Int64",
        "GiftPostedMonth": "object",
        "GLCode": "object",
        "MsdDmFilter": "object"
        },
      "PromoHistory": {
        "Acq": "object",
        "AppealID": "object",
        "Day1Fiscal": "datetime64[ns]",
        "DonorID": "object",
        "EndFiscal": "datetime64[ns]",
        "FHGroup": "object",
        "FHGroupDetail": "object",
        "Fiscal": "Int64",
        "FreqID": "object",
        "JoinSource": "object",
        "MailDate": "datetime64[ns]",
        "MrcAmtID": "object",
        "MrcID": "object",
        "SourceCodeID": "object",
        "SustMrcID": "object",
        "SustainerFlag": "object"
      },
      "SourceCode": {
        "Acq": "object",
        "AppealID": "object",
        "CampaignName": "object",
        "CostPerPiece": "float64",
        "CtrlTest": "object",
        "Fiscal": "int64",
        "List": "object",
        "ListCostPerPiece": "float64",
        "ListSegment": "float64",
        "MailDate": "datetime64[ns]",
        "MailingMonth": "object",
        "Package": "object",
        "Program": "object",
        "Quantity": "float64",
        "RolloutCostPerPiece": "float64",
        "SourceCodeID": "object"
      }
  },
    "DAV": {
      "Cashflow": {
        "CampaignFiscal" : "int64",
        "CampaignMonth" : "int64",
        "CAMPAIGN_TYPE_CD" : "object",
        "ACTUAL_MAIL_DATE" : "datetime64[ns]",
        "Revenue" : "float64",
        "Responses" : "int64",
        "Days_Out_from_MailDate" : "int64",
        "Days_Out_from_FirstGift" : "int64"
      }
   }
}

# COMMAND ----------

