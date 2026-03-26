# Databricks notebook source
# This file provides a means to connecting the local Databricks file system to the Azure Datalake:
# -  Authenticates with ADLS2
# -  Allows data transfer to ADLS2 to be treated as typical IO calls like read and write to the local file system.
# -  Provides a common class to be used by all clients to facilitate simple mapping to the storage system


# COMMAND ----------

import os

# COMMAND ----------

# # uncomment this to unmount the mountPoint
# MOUNT_POINT = "/mnt/msd-datalake"
# dbutils.fs.unmount(MOUNT_POINT)

# COMMAND ----------

# Configure the mount point in dbfs
DATALAKE_URL = "abfss://msd-datalake@msdstore.dfs.core.windows.net"
MOUNT_POINT = "/mnt/msd-datalake"

try:
  mounts = dbutils.fs.mounts()
  mount_list = [att.mountPoint for att in mounts]
  if MOUNT_POINT in mount_list:
    print ("Datalake already mounted!")
  else:
    print ("Mounting Datalake")
    ServicePrincipalId = dbutils.secrets.get(
      scope='fuse-etl-key-vault',
      key='msd-databricks-service-principal-id'
    )
    ServicePrincipalKey = dbutils.secrets.get(
      scope='fuse-etl-key-vault',
      key='msd-databricks-service-principal-key'
    )
    DirectoryID = dbutils.secrets.get(
      scope='fuse-etl-key-vault',
      key='fuse-fundraising-tenant-id'
    )
    Directory = "https://login.microsoftonline.com/{}/oauth2/token".format(DirectoryID)
    configs = {
      "fs.azure.account.auth.type": "OAuth",
      "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
      "fs.azure.account.oauth2.client.id": ServicePrincipalId,
      "fs.azure.account.oauth2.client.secret": ServicePrincipalKey,
      "fs.azure.account.oauth2.client.endpoint": Directory
    }
    dbutils.fs.mount(
      source=DATALAKE_URL,
      mount_point=MOUNT_POINT,
      extra_configs=configs
    )
except Exception as e:
  print(f"Skipping ADLS mount (dbutils.fs.mounts not available or mount failed): {e}")

# COMMAND ----------

# DBTITLE 1,TEST


# COMMAND ----------

# DBTITLE 1,Filemap
class Filemap():
  '''
  Provides an object to store the appropriate path details for each client and/or notebook.
  '''
  def __init__(self, client):
    '''
    Leverages the msd-datalake file system organization to develop the path name from each client.
    Arguments:
    -  client: a string representing the Mindset Direct client alias, e.g. "The Seeing Eye" -> "TSE"
    '''
    global MOUNT_POINT
   
    self.MOUNT = '/dbfs' + MOUNT_POINT + '/'
    self.RAW = self.MOUNT + 'Raw/' + client + '/'
    self.STAGED = self.MOUNT + 'Staged/' + client + '/'
    self.CURATED = self.MOUNT + 'Curated/' + client + '/'
    self.ARCHIVE = self.MOUNT + 'Archive/' + client + '/'
    # self.LOGS = self.MOUNT + 'Logs/' + client + '/'
    self.HOME = '/databricks/driver/'
    self.SHARED = self.MOUNT + 'Shared/'
    self.SCHEMA = self.MOUNT + 'Schema/' + client + '/'
    self.SUPPRESSIONS = self.MOUNT + 'Suppressions/' + client + '/'
    self.MASTER = self.MOUNT + 'Master/' + client + '/'

# COMMAND ----------

