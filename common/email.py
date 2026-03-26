# Databricks notebook source
# MAGIC %md # Email Functions

# COMMAND ----------

# MAGIC %md ## Imports and Runs

# COMMAND ----------

# DBTITLE 1,transfer_files
# MAGIC %run ../python_modules/transfer_files

# COMMAND ----------

# DBTITLE 1,Imports
import smtplib
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
# from email.utils import COMMASPACE, formatdate
from email import encoders

# COMMAND ----------

# MAGIC %md ## Email Functions

# COMMAND ----------

# DBTITLE 1,Data Team Recipients
# Send email to Data team
def data_team_recipients():
  to = [
    'chrisc@fusefundraising.com', 'samc@fusefundraising.com'
  ]
  bcc = [
  ]
  return to, bcc

print(data_team_recipients())


# COMMAND ----------

# DBTITLE 1,Analytics Team Recipients
# Send email to Analytics team
def analytics_team_recipients():
  to = [
    'AnalyticsTeam@fusefundraising.com'
  ]
  bcc = [
  ]
  return to, bcc

print(analytics_team_recipients())

# COMMAND ----------

# DBTITLE 1,send_email
def configure_message(user, to, subject, body, cc=[]):
  # build the email
  msg = MIMEMultipart()
  msg['From'] = user
  msg['To'] = ', '.join(to)
  msg['Subject'] = subject
  if len(cc)>0:
    msg['Cc'] = ', '.join(cc)
  msg.attach(MIMEText(body))
  return msg

def attach_file(msg, files):
  # add attachments
  for path in files:
    part = MIMEBase('application', "octet-stream")
    with open(path, 'rb') as file:
        part.set_payload(file.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition',
                    'attachment; filename="{}"'.format(Path(path).name))
    msg.attach(part)
  return msg

def sign_in(user, pw, server='smtp.office365.com', port=587):
  # sign in to the mail server
  mailserver = smtplib.SMTP(server, port)
  mailserver.ehlo()
  mailserver.starttls()
  mailserver.ehlo()
  mailserver.login(user, pw)
  return mailserver


def send_email(user, to, subject, body, files, pw=None, cc=[], bcc=[], message=None):
  if pw is None:
    user,pw = retrieve_creds()
  
  msg = configure_message(user, to, subject, body, cc=cc)   
  msg = attach_file(msg, files) 
  mailserver = sign_in(user, pw)
  full_recipient_list = to+cc+bcc
  mailserver.sendmail(user, full_recipient_list, msg.as_string())
  mailserver.quit()
  return True
  

# COMMAND ----------

# DBTITLE 1,inline_formatter
def inline_formatter(inp, css=None):
  import pandas as pd
  import os
  try:
    css_path = css
    if css_path is None:
      for p in ['config/Shared Formatting.css', 'Shared Formatting.css',
                '/Workspace/Repos/Shared/config/Shared Formatting.css',
                '/Workspace/Shared/config/Shared Formatting.css']:
        if p and os.path.exists(p):
          css_path = p
          break
    if css_path is None or not os.path.exists(css_path):
      raise FileNotFoundError("Shared Formatting.css not found")
    replace = pd.read_csv(css_path, sep='|', header=None).reset_index()
    # Set up the find/replace statements from the css file
    find = pd.DataFrame('<' + replace[0].str.split('{').str[0]).reset_index()
    find[0] = find[0] + '>'
    replace[0] = replace[0].str.replace('{', ' style="')
    replace[0] = replace[0].str.replace('}', '"')
    join = pd.merge(replace, find, on='index', suffixes=['_replace', '_find'])
    #
    if isinstance(inp, pd.DataFrame) == True:
      inp.columns = inp.columns.str.replace("_", " ")
      inp.columns = inp.columns.str.title()
      html = inp.to_html(index=False, justify='left', formatters=None)
      html = html.replace(' border="1" class="dataframe"', '')
      html = html.replace('&lt;', '<')
      html = html.replace('&gt;', '>')
    else:
      html = inp
    for row, iterrow in join.iterrows():
      html = html.replace(iterrow['0_find'], '<' + iterrow['0_replace'] + '>')
    print('Complete: Inline formatting')
  except Exception:
    if isinstance(inp, pd.DataFrame):
      html = inp.to_html(index=False)
    else:
      html = inp
    print('Error: Inline formatting (using plain HTML)')
  return html

# COMMAND ----------

# DBTITLE 1,send_email_from_html
def send_email_from_html(to, subject, contents, cc=None, attachment_list=[], from_label = 'Fuse Reporting <reportingadmin@fusefundraising.com>'):
  import pandas as pd
  import smtplib
  from email.mime.text import MIMEText
  from email.mime.multipart import MIMEMultipart
  from email.mime.base import MIMEBase
  from email import encoders

  # If contents are dataframe, format
  if isinstance(contents, pd.DataFrame):
    contents = inline_formatter(contents)
  
  # If right 5 = .html then load the file else use the string passed as the html
  elif contents[-5:] == '.html':
    file = open(contents, "r")
    contents = file.read()
  
  # to - if a list is passed, convert to comma sep. If comma sep, re-format
  if type(to) == list:
    to = ", ".join(to)
  elif ',' in to:
    to = list(to.split(','))
    to = ", ".join(to)
  
  # PW
  un, pw = retrieve_creds()

  message = MIMEMultipart("alternative")
  #
  message["From"] = from_label
  message["Subject"] = subject
  message["To"] = to
  message["Cc"] = cc

  #
  part2 = MIMEText(contents, 'html')
  message.attach(part2)

  # Add attachments
  if len(attachment_list)>0:
    for item in attachment_list:
      print(item)
      rght = item[::-1].find('/', 1)
      length = len(item) - rght
      file_name = item[length:]
      part = MIMEBase('application', "octet-stream")
      part.set_payload(open(item, "rb").read())
      encoders.encode_base64(part)
      part.add_header('Content-Disposition', 'attachment; filename=' + file_name)  # attachment had been attachment
      message.attach(part)

  # Send the email
  server = smtplib.SMTP('smtp.office365.com', 587)#smtplib.SMTP_SSL('smtp.office365.com', port=587, timeout=120)
  server.ehlo()  # Identify yourself to an ESMTP server
  server.starttls()
  server.ehlo()
  server.login(un, pw)
  server.sendmail(from_label, message["To"].split(","), message.as_string())
  server.quit()
  print('email sent')