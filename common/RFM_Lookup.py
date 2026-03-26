# Databricks notebook source
# dictionary of max value to label
# null	Z: NoMRCid
recency = {
  3: 'A: 0-3',
  6: 'B: 4-6',
  12: 'C: 7-12',
  18: 'D: 13-18',
  24: 'E: 19-24',
  36: 'F: 25-36',
  48: 'G: 37-48',
  60: 'H: 49-60',
  72: 'I: 61-72',
  84: 'J: 73-84',
  96: 'K: 85-96',
  108: 'L: 97-108',
  120: 'M: 109-120',
  132: 'N: 121-132',
  144: 'O: 133-144',
  500000: 'P: 145+'
}

dav_recency = {
  'A': '0-3 months',
  'B': '4-6 months',
  'C': '7-9 months',
  'D': '10-12 months',
  'E': '13-18 months',
  'F': '19-24 months',
  'G': '25-30 months',
  'H': '31-36 months',
  'I': '37-42 months',
  'J': '43-48 months',
  'K': '49-60 months',
  'L': '61-72 months',
  'M': '73-84 months',
  'N': '85-96 months',
  'O': '97-108 months',
  'P': '109-120 months',
  'Q': '121-144 months',
  'R': '145+ months'
}

# COMMAND ----------

# null	Z: NoFreqid
frequency = {
  1: 'A: 1 Gift',
  2: 'B: 2 Gifts',
  4: 'C: 3-4 Gifts',
  9: 'D: 5-9 Gifts',
  100000: 'E: 10+ Gifts'
}

dav_frequency = {
  'A': '1 gift',
  'B': '2 gifts',
  'C': '3 gifts',
  'D': '4 gifts',
  'E': '5 gifts',
  'F': '6 gifts',
  'G': '7 gifts',
  'H': '8 gifts',
  'I': '9 gifts',
  'J': '10+ gifts'
}

# COMMAND ----------

# null Z: NoHPCid
# monetary = {
#   5: 'A: 0-4.99',
#   10: 'B: 5-9.99',
#   15:	'C: 10-14.99',
#   20:	'D: 15-19.99',
#   25:	'E: 20-24.99',
#   50:	'F: 25-49.99',
#   100: 'G: 50-99.99',
#   250: 'H: 100-249.99',
#   500: 'I: 250-499.99',
#   1000: 'J: 500-999.99',
#   2500: 'K: 1000-2499.99',
#   5000: 'L: 2500-4999.99',
#   10000000: 'M: 5000+'
# }


monetary = {
  4:    'A: 0-4.99',
  9:    'B: 5-9.99',
  14:	'C: 10-14.99',
  19:	'D: 15-19.99',
  24:	'E: 20-24.99',
  49:	'F: 25-49.99',
  99:   'G: 50-99.99',
  249:  'H: 100-249.99',
  499:  'I: 250-499.99',
  999:  'J: 500-999.99',
  2499: 'K: 1000-2499.99',
  4999: 'L: 2500-4999.99',
  9999: 'M: 5000-9999.99',
  9999999: 'N: 10000+'
}

dav_monetary = {
  'A': '<$2',
  'B': '$2-$4.99',
  'C': '$5-$9.99',
  'D': '$10-$14.99',
  'E': '$15-$19.99',
  'F': '$20-$24.99',
  'G': '$25-$49.99',
  'H': '$50-$99.99',
  'I': '$100-$249.99',
  'J': '$250-$499.99',
  'K': '$500-$999.99',
  'L': '$1,000+'
}

# COMMAND ----------

mc = {
  0: {
    'Name': 'DonorGroup',
    'Values': {
      'D': 'PIMs',
      'M': 'ML',
      'G': 'MM MRC $1000-$9999',
      'H': 'MM MRC $100-$999',
      'K': 'MM MRC $5-$99',
      'J': 'Many - catchall',
      'B': 'PAX Designators',
      'C': 'Workplace Giving Donors',
      'A': 'Major Gift Pool',
      'S': 'Seeds',
      'Z': 'Reserve for Testing'
    }
  },
  1: {
    'Name': 'GivingChannel',
    'Values': {
      'A': 'All Channels',
      'B': 'Online Only Regular (less New)',
      'C': 'Online Only LT',
      'D': 'DMDnrs Resp 0-24 mo',
      'E': 'DMDnrs Resp 25-48 mo',
      'F': 'DMDnrs Resp LT',
      'G': 'Misc New',
      'H': 'Misc Regular (less New)',
      'I': 'Misc LT',
      'J': 'DMDnrs Resp New',
      'K': 'Multi - LT DM and Online',
      'S': 'Seeds',
      'U': 'Placeholder for WGD',
      'Z': 'Reserve for Testing'
    }
  },
  2: {
    'Name': 'MRC Amount',
    'Values': {
      'A': '$10000+',
      'B': '$5000-9999',
      'C': '$2500-4999',
      'D': '$1000-2499',
      'E': '$500-999',
      'F': '$250-499',
      'G': '$100-249',
      'H': '$50-99',
      'I': '$25-49',
      'J': '$10-24',
      'K': '$5-9.99',
      'L': '$.01-4.99',
      'S': 'Seeds',
      'U': 'Placeholder for WGD',
      'Z': 'Reserve for Testing'
    }
  },
  3: {
    'Name': 'RF',
    'Values': {
      'A': '0-6 mo multis',
      'B': '0-6 mo singles',
      'C': '7-12 mo multis',
      'D': '7-12 mo singles',
      'E': '13-24 mo multis',
      'F': '13-24 mo singles',
      'G': '25-36 mo multis',
      'H': '25-36 mo singles',
      'I': '37-48 mo multis',
      'J': '37-48 mo singles',
      'K': '49-60 mo multis',
      'L': '49-60 mo singles',
      'M': '61-96 mo multis',
      'N': '61-96 mo singles',
      'O': '97+ mo multis',
      'P': '97+ mo singles',
      'S': 'Seeds',
      'U': 'Placeholder for WGD',
      'Z': 'Reserve for Testing'
    }
  }
}