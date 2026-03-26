# Databricks notebook source
'''
Registers scripts to add linux packages on all nodes of the cluster at initialization.
These cells only need to be run to register the scripts with Databricks, after which they are accessed in clusters -> advanced options -> init scripts.
'''

# COMMAND ----------

dbutils.fs.put("/databricks/init/DAV_ETL/installp7zip-full.sh","""
#!/bin/bash
sudo apt-get -y install p7zip-full
""", True)

# COMMAND ----------

dbutils.fs.put("/databricks/init/DAV_ETL/installsshpass.sh","""
#!/bin/bash
sudo apt-get install sshpass -y
""", True)

# COMMAND ----------

dbutils.fs.put("/databricks/init/TSE_ETL/installsqldriver.sh", """
#!/bin/bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17
sudo ACCEPT_EULA=Y apt-get install mssql-tools
""", True)

# COMMAND ----------

dbutils.fs.put("/databricks/init/DAV_ETL/remdbjars.sh","""
#!/bin/bash
rm /databricks/jars/*mssql*
""", True)

# COMMAND ----------

# MAGIC %sh lsb_release -a 

# COMMAND ----------

