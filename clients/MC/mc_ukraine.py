# Databricks notebook source
scs = [
  'MAZWUMSTOPEZZ0310221', 'MQZWUMSTOMNZZ0516222',
  'MQZWUMSTOMNZZ0516221', 'MQZWUMSTOMNZZ0613221',
  'MQZWUMSTOMNZZ0613222', 'MQZWUMUTORNZZ0701221',
  'MQZWUMUTORNZZ0801221', 'MQZWUMUTORNZZ0830221',
  'MQZWUMUTORNZZ1003221', 'MQZWUMUTOMNZZ1003221',
  'MQZWUMUTORNZZ1107221', 'MQZWUMUTORNZZ1206221',
  'MQZWUMSTORNZZ1206221'
]

def isResUkrFund(df):
  
  funds = ['68700', '68702', '68703']
  for fund in funds:
    df['is%s' %fund] = df.FundID.str.contains(fund).fillna(value=False)

  feature = 'ResUkrFund'

  c1 = (df.GiftDate > '2022-02-24') & (df.is68700)
  c2 = (df.is68702) | (df.is68703)

  df[feature] = c1|c2
  return df


def isUAFlagged(df, scs):
  
  df['UAFlaggedOffline'] = df.CampaignCode.isin(scs)
  df['UAFlaggedOffline'].value_counts()
  
  mask = df.CampaignCode.str.contains('EZZ').fillna(value=False)
  df['UAFlaggedOnline'] = (mask) & (~df['UAFlaggedOffline']) & (~df['ResUkrFund'])
  
  mask = (df['UAFlaggedOffline']) | (df['UAFlaggedOnline'])
  df['UAFlagged'] = (mask) & (df.GiftDate>'2022-02-24')
  
  return df


def isDateFlagged(df):
  start = '2022-02-24'
  end = '2022-03-31'
  df['DateFlag'] = ((df.GiftDate >= start) & (df.GiftDate <= end)) & (~df['UAFlagged']) & (~df['ResUkrFund'])
  return df


def isUkraine(df, feature):
  df = isResUkrFund(df)
  df = isUAFlagged(df, scs)
  df = isDateFlagged(df)
  df[feature] = (df['ResUkrFund']) | (df['UAFlagged']) | (df['DateFlag'])
  return df