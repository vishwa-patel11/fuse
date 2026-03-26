# Databricks notebook source
# MAGIC %md ## Imports and Runs

# COMMAND ----------

# DBTITLE 1,Info
# This file provides functionality to transfer all files (including those in subdirectories) from the Sharepoint directory to Azure Datalake:
# - Periodic (monthly) data files
# - Special data files
# - Ancillary or support files

# Inputs:
# - MindSET Direct Sharepoint site:
#   - https://mindsetdirect.sharepoint.com/ + client directory details

# Outputs:
# - The appropriate client directory in msd-datalake/Raw

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import datetime
import json
import os
import time
import pandas as pd
import pytz

# sleep for office365 delay
time.sleep(5)

from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file_creation_information import FileCreationInformation
from office365.sharepoint.files.file import File
from office365.runtime.auth.authentication_context import AuthenticationContext
#Added 9/12/25 for authentication
from office365.runtime.auth.user_credential import UserCredential


# sleep for office365 delay
time.sleep(5)

# COMMAND ----------

# DBTITLE 1,Installs
# # pip install --upgrade office365-rest-python-client
# %pip install -U office365-rest-python-client azure-identity requests


# COMMAND ----------

# DBTITLE 1,Filemap
# MAGIC %run ./filemap

# COMMAND ----------

# DBTITLE 1,utilities
# MAGIC %run ./utilities

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

# DBTITLE 1,Prep variables
# get connection constants
main_sharepoint_url = "https://mindsetdirect.sharepoint.com/"
client_data_share_url = "https://mindsetdirect.sharepoint.com/sites/ClientDataShare" # trx files

_filemap = Filemap('Fuse')
with open(os.path.join(_filemap.SCHEMA, 'schema.json'), 'r') as f:
  schema = json.load(f)
SHAREPOINT_UID = schema['SharePointUID']
SHAREPOINT_PWD = dbutils.secrets.get(
  scope='fuse-etl-key-vault', 
  key='sharepoint-pwd'
  ) 

# client inherited from calling notebook
try:
  CLIENT = client
except NameError:
  CLIENT = ''

# COMMAND ----------

# DBTITLE 1,Sharepoint helpers (moved from utilities)
# Sharepoint helpers

'''
BASE_URL = "https://mindsetdirect.sharepoint.com/"
client_context = get_sharepoint_context(BASE_URL)
relative_url = 'Shared Documents/Reporting 2.0/Fuse/SourcecodeDocsLog'
relative_url = 'Shared Documents/Reporting 2.0/SourceCodeDocs/MC'
get_all_contents(client_context, relative_url)
'''

# get the un and pw
def retrieve_creds():
  filemap = Filemap('Fuse') # mount_datalake
  with open(os.path.join(filemap.SCHEMA, 'schema.json'), 'r') as f:
    USER = json.load(f)['SharePointUID']
  PW = dbutils.secrets.get(scope='fuse-etl-key-vault', key='sharepoint-pwd')
  return USER, PW

# # # get a token to authorize access
def get_sharepoint_context(site_url,username=None,password=None):
  if username is None or password is None:
      username,password = retrieve_creds()
  ctx_auth = AuthenticationContext(site_url)
  if ctx_auth.acquire_token_for_user(username, password):
      ctx = ClientContext(site_url, ctx_auth)
      return ctx
  else:
      raise Exception("Authentication failed")


# COMMAND ----------

# DBTITLE 1,Authorize Sharepoint Access (Deprecated Authentication)
# # get a token to authorize access
# def get_sharepoint_context(site_url,username=None,password=None):
#   if username is None or password is None:
#       username,password = retrieve_creds()
#       print(username)
#   ctx_auth = AuthenticationContext(site_url)
#   print(site_url)
#   print(ctx_auth)
#   if ctx_auth.acquire_token_for_user(username, password):
#       ctx = ClientContext(site_url, ctx_auth)
#       return ctx
#   else:
#       raise Exception("Authentication failed")

# # set main context
# client_context = get_sharepoint_context(main_sharepoint_url)

# COMMAND ----------

# DBTITLE 1,Sharepoint Certification and App Authentication
# pip install -U office365-rest-python-client azure-identity requests

import base64, time
from azure.identity import CertificateCredential
from office365.sharepoint.client_context import ClientContext

MAIN_SITE = "https://mindsetdirect.sharepoint.com/"
CLIENTDATA_SITE = "https://mindsetdirect.sharepoint.com/sites/ClientDataShare"
SP_TENANT = "https://mindsetdirect.sharepoint.com"  # audience for SPO tokens

TENANT_ID = dbutils.secrets.get('fuse-etl-key-vault','Azure-Tenant-ID')
CLIENT_ID = dbutils.secrets.get('fuse-etl-key-vault','Fuse-Sharepoint-App-ClientID')

# PFX from Key Vault (base64) + password
PFX_B64   = dbutils.secrets.get('fuse-etl-key-vault','fuse-spo-app-only-pfx')
PFX_PASS  = dbutils.secrets.get('fuse-etl-key-vault','fuse-pfx-password')  # "FUSE_data_2520"
PFX_BYTES = base64.b64decode(PFX_B64)

_cred = CertificateCredential(
    tenant_id=TENANT_ID,
    client_id=CLIENT_ID,
    certificate_data=PFX_BYTES,
    password=(PFX_PASS or None)
)

# --- minimal wrapper the library expects ---
class SPOToken:
    def __init__(self, token_str, expires_on=None):
        self.tokenType = "Bearer"
        self.accessToken = token_str
        if expires_on is not None:
            # library may look at expiresIn to cache
            self.expiresIn = max(1, int(expires_on - time.time()))

def _acquire_spo_token_obj():
    at = _cred.get_token(f"{SP_TENANT}/.default")  # AccessToken from azure-identity
    return SPOToken(at.token, at.expires_on)

# --- drop-in: same signature as before ---
def get_sharepoint_context_app_only(site_url):
    return ClientContext(site_url).with_access_token(_acquire_spo_token_obj)  # pass CALLABLE

# use it like before
client_context = get_sharepoint_context_app_only(MAIN_SITE)
ctx = get_sharepoint_context_app_only(CLIENTDATA_SITE)

web = ctx.web.get().execute_query()
print("Connected to site:", web.properties.get("Title"))

# COMMAND ----------

# DBTITLE 1,Application TEST
# from office365.sharepoint.client_context import ClientContext
# from office365.runtime.auth.client_credential import ClientCredential

# SP_SITE = "https://mindsetdirect.sharepoint.com/sites/ClientDataShare"
# #SP_SITE = "https://graph.microsoft.com/v1.0/sites/mindsetdirect.sharepoint.com:/sites/ClientDataShare"


# CLIENT_ID = dbutils.secrets.get('fuse-etl-key-vault', 'Fuse-Sharepoint-App-ClientID')
# CLIENT_SECRET = dbutils.secrets.get('fuse-etl-key-vault', 'Fuse-Sharepoint-App')



# def get_sharepoint_context_app_only(site_url=SP_SITE):
#     creds = ClientCredential(CLIENT_ID, CLIENT_SECRET)
#     return ClientContext(site_url).with_credentials(creds)
  

# ctx = get_sharepoint_context_app_only()
# client_context = get_sharepoint_context_app_only()


# #Sanity test
# web = ctx.web.get().execute_query()
# print("Connected to site:", web.properties.get("Title"))

# COMMAND ----------

# DBTITLE 1,APP REGISTRATION CREDENTIALS
# App registration credentials
CLIENT_ID = dbutils.secrets.get('fuse-etl-key-vault', 'Fuse-Sharepoint-App-ClientID')
CLIENT_SECRET = dbutils.secrets.get('fuse-etl-key-vault', 'Fuse-Sharepoint-App')
TENANT_ID = dbutils.secrets.get('fuse-etl-key-vault','Azure-Tenant-ID')

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,get_all_contents
# https://github.com/vgrem/Office365-REST-Python-Client/issues/98


'''
relative_url = 'Shared Documents/{client}/Transaction Files'.format(client=client)

BASE_URL = "https://mindsetdirect.sharepoint.com/"
client_context = get_sharepoint_context(BASE_URL)
relative_url = 'Shared Documents/Reporting 2.0/SourceCodeDocs/CHOA'
return_metadata=True

get_all_contents(client_context, relative_url, return_metadata=True)
print('********')
print(client_files)
'''

client_files = []
client_files_metadata = [] 
# def get_all_contents(client_context, relative_url, check_csv=False, return_metadata=False):
#     global client_files
#     global client_files_metadata
#     try:
#         # Load the Root
#         libraryRoot = client_context.web.get_folder_by_server_relative_url(relative_url)
#         client_context.load(libraryRoot)
#         client_context.execute_query()

#         # Prep for Folders
#         folders = libraryRoot.folders
#         client_context.load(folders)
#         client_context.execute_query()

#         # Load the Subfolders
#         for client_folder in folders:
#             #print(client_folder.properties['Name'])
#             get_all_contents(client_context, relative_url + '/' + client_folder.properties["Name"], return_metadata=return_metadata)

#         # Prep for Files
#         files = libraryRoot.files
#         client_context.load(files)
#         client_context.execute_query()
        
#         # Compile into one list
#         for client_file in files:
#             x = client_file.properties['ServerRelativeUrl']
#             #print(x)
#             if return_metadata:
#                 #print('return_metadata')
#                 created = parser.parse(client_file.properties['TimeCreated']).astimezone(pytz.timezone("US/Eastern"))
#                 mod = parser.parse(client_file.properties['TimeLastModified']).astimezone(pytz.timezone("US/Eastern"))
#                 tmp_list = [x, created, mod]
#                 client_files.append(tmp_list)
#             else:
#                 client_files.append(x)
#                 # added 4/19/22 for NJH
#                 if check_csv:
#                     client_files = [x for x in client_files if '.csv' in x.lower()]
    
#     except Exception as e:
#         #print('Error in get_all_contents(): ', e)
#         print(f'Error in get_all_contents(): {e}')
#         print(f'Failed at: {relative_url}')
#         return None

#     return client_files

import time
import random
from urllib.error import HTTPError

def get_all_contents(client_context, relative_url, check_csv=False, return_metadata=False, max_retries=5):
    global client_files
    global client_files_metadata
    try:
        retries = 0

        # Helper function to execute queries with retries
        def execute_with_retry(client_context):
            nonlocal retries
            while retries < max_retries:
                try:
                    client_context.execute_query()
                    return  # Success
                except Exception as e:
                    if "429" in str(e):  # Detect 429 Too Many Requests
                        wait_time = 2 ** retries + random.uniform(0, 1)  # Exponential backoff
                        print(f"Throttled (429). Retrying in {wait_time:.2f} seconds... (Attempt {retries + 1}/{max_retries})")
                        time.sleep(wait_time)
                        retries += 1
                    else:
                        raise e  # Re-raise non-429 errors
            raise Exception("Max retries exceeded. Throttling persists.")

        # Load the Root
        libraryRoot = client_context.web.get_folder_by_server_relative_url(relative_url)
        client_context.load(libraryRoot)
        execute_with_retry(client_context)

        # Prep for Folders
        folders = libraryRoot.folders
        client_context.load(folders)
        execute_with_retry(client_context)

        # Load the Subfolders
        for client_folder in folders:
            get_all_contents(client_context, relative_url + '/' + client_folder.properties["Name"], return_metadata=return_metadata)

        # Prep for Files
        files = libraryRoot.files
        client_context.load(files)
        execute_with_retry(client_context)

        # Compile into one list
        for client_file in files:
            x = client_file.properties['ServerRelativeUrl']
            if return_metadata:
                created = parser.parse(client_file.properties['TimeCreated']).astimezone(pytz.timezone("US/Eastern"))
                mod = parser.parse(client_file.properties['TimeLastModified']).astimezone(pytz.timezone("US/Eastern"))
                tmp_list = [x, created, mod]
                client_files.append(tmp_list)
            else:
                client_files.append(x)
                if check_csv:
                    client_files = [x for x in client_files if '.csv' in x.lower()]
    
    except Exception as e:
        if "429" in str(e):
            print("Throttling error persists despite retries.")
        else:
            print(f"Error in get_all_contents(): {e}")
        print(f"Failed at: {relative_url}")
        return None

    return client_files

def get_folder_files(client_context, relative_url, check_csv=False, return_metadata=False):
    """
    Get only the files directly inside the given SharePoint folder.
    """
    try:
        libraryRoot = client_context.web.get_folder_by_server_relative_url(relative_url)
        client_context.load(libraryRoot)
        client_context.execute_query()

        files = libraryRoot.files
        client_context.load(files)
        client_context.execute_query()

        result = []
        for f in files:
            x = f.properties['ServerRelativeUrl']
            if return_metadata:
                created = parser.parse(f.properties['TimeCreated']).astimezone(pytz.timezone("US/Eastern"))
                mod = parser.parse(f.properties['TimeLastModified']).astimezone(pytz.timezone("US/Eastern"))
                result.append([x, created, mod])
            else:
                result.append(x)

        if check_csv:
            result = [x for x in result if '.csv' in x.lower()]

        return result

    except Exception as e:
        print(f"Error accessing folder {relative_url}: {e}")
        return None



# COMMAND ----------

# DBTITLE 1,get_all_contents_2
def get_all_contents_2(client_context, relative_url, check_csv=False, return_metadata=False, max_retries=5):
    """
    Recursively list ALL files under a SharePoint folder (including subfolders).

    Returns:
      - if return_metadata=False: [ServerRelativeUrl, ...]
      - if return_metadata=True:  [(url, created_dt, modified_dt), ...]
    """
    from dateutil import parser
    import pytz, time, random

    est = pytz.timezone("US/Eastern")

    def execute_with_retry(ctx):
        retries = 0
        while True:
            try:
                ctx.execute_query()
                return
            except Exception as e:
                msg = str(e)
                # throttle / transient patterns
                transient = ("429" in msg) or ("503" in msg) or ("504" in msg) or ("temporarily" in msg.lower())
                if transient and retries < max_retries:
                    wait = (2 ** retries) + random.uniform(0, 1)
                    print(f"SharePoint throttled/transient. Retrying in {wait:.2f}s... ({retries+1}/{max_retries})")
                    time.sleep(wait)
                    retries += 1
                    continue
                raise

    results = []

    def walk(folder_url):
        # Load folder
        folder = client_context.web.get_folder_by_server_relative_url(folder_url)
        client_context.load(folder)
        execute_with_retry(client_context)

        # Subfolders
        subfolders = folder.folders
        client_context.load(subfolders)
        execute_with_retry(client_context)

        for sf in subfolders:
            name = sf.properties.get("Name", "")
            if name.lower() == "forms":
                continue
            walk(folder_url.rstrip("/") + "/" + name)

        # Files in this folder
        files = folder.files
        client_context.load(files)
        execute_with_retry(client_context)

        for f in files:
            url = f.properties.get("ServerRelativeUrl")
            if not url:
                continue

            if check_csv and ".csv" not in url.lower():
                continue

            if return_metadata:
                created_raw = f.properties.get("TimeCreated")
                modified_raw = f.properties.get("TimeLastModified")
                created = parser.parse(created_raw).astimezone(est) if created_raw else None
                modified = parser.parse(modified_raw).astimezone(est) if modified_raw else None
                results.append((url, created, modified))
            else:
                results.append(url)

    try:
        walk(relative_url)
        return results
    except Exception as e:
        print(f"Error in get_all_contents(): {e}")
        print(f"Failed at: {relative_url}")
        return None


# COMMAND ----------

# DBTITLE 1,get_all_contents_trx_file_only
# needed imports
from dateutil import parser
import pytz
import random, time

def get_all_contents_trx_file_only(
    client_context,
    relative_url,
    check_csv: bool = False,
    return_metadata: bool = False,
    max_retries: int = 5,
    target_folder: str = 'Transaction Files',   # case-insensitive match
    library_root: str = 'Shared Documents'      # the library you started from
):
    """
    Traverse only: <library_root>/<client>/<target_folder>
      - At <library_root>: only recurse into client folders.
      - At <library_root>/<client>: only recurse into <target_folder>.
      - Any other second-level (<library_root>/<client>/<XXX != target_folder>) is pruned (stop).
    Prints when a target folder is reached and lists files inside.
    """
    global client_files
    global client_files_metadata  # kept for compatibility if used elsewhere

    try:
        retries = 0

        def execute_with_retry(ctx):
            nonlocal retries
            while retries < max_retries:
                try:
                    ctx.execute_query()
                    return
                except Exception as e:
                    if "429" in str(e):
                        wait_time = 2 ** retries + random.uniform(0, 1)
                        print(f"Throttled (429). Retrying in {wait_time:.2f}s... (Attempt {retries + 1}/{max_retries})")
                        time.sleep(wait_time)
                        retries += 1
                    else:
                        raise
            raise Exception("Max retries exceeded. Throttling persists.")

        # Load the current folder
        libraryRoot = client_context.web.get_folder_by_server_relative_url(relative_url)
        client_context.load(libraryRoot)
        execute_with_retry(client_context)

        # Figure out where we are relative to the library root
        parts = [p for p in relative_url.strip('/').split('/') if p]
        try:
            lib_idx = parts.index(library_root)
        except ValueError:
            # If library_root isn't in the path, just don't recurse further.
            lib_idx = None

        depth_after_library = None if lib_idx is None else len(parts) - (lib_idx + 1)
        current_folder_name = parts[-1] if parts else ""

        # If THIS folder is the target, collect files (no further recursion).
        if current_folder_name.lower() == str(target_folder).lower():
            print("📂 Traversing Transaction Files folder:", relative_url)

            files = libraryRoot.files
            client_context.load(files)
            execute_with_retry(client_context)

            est = pytz.timezone("US/Eastern")

            for client_file in files:
                url = client_file.properties.get('ServerRelativeUrl')
                if not url:
                    continue
                if check_csv and '.csv' not in url.lower():
                    continue

                if return_metadata:
                    created = parser.parse(client_file.properties['TimeCreated']).astimezone(est)
                    modified = parser.parse(client_file.properties['TimeLastModified']).astimezone(est)
                    client_files.append([url, created, modified])
                    print(f"   ↳ File: {url} (Created: {created}, Modified: {modified})")
                else:
                    client_files.append(url)
                    print(f"   ↳ File: {url}")

            return client_files  # done at target level

        # Otherwise, decide whether/where to recurse
        folders = libraryRoot.folders
        client_context.load(folders)
        execute_with_retry(client_context)

        # Depth logic:
        # depth_after_library == 0  -> at <library_root> (only traverse CLIENT folders)
        # depth_after_library == 1  -> at <library_root>/<client> (only traverse <target_folder>)
        # depth_after_library >= 2  -> prune unless already at target (handled above)
        for client_folder in folders:
            folder_name = client_folder.properties.get("Name", "")
            if folder_name.lower() == "forms":
                continue  # skip system folder

            # At library root: go into client folders
            if depth_after_library == 0:
                new_relative_url = relative_url.rstrip('/') + '/' + folder_name
                get_all_contents_trx_file_only(
                    client_context,
                    new_relative_url,
                    check_csv=check_csv,
                    return_metadata=return_metadata,
                    max_retries=max_retries,
                    target_folder=target_folder,
                    library_root=library_root
                )

            # At <library_root>/<client>: only go into <target_folder>, skip others
            elif depth_after_library == 1:
                if folder_name.lower() == str(target_folder).lower():
                    new_relative_url = relative_url.rstrip('/') + '/' + folder_name
                    get_all_contents_trx_file_only(
                        client_context,
                        new_relative_url,
                        check_csv=check_csv,
                        return_metadata=return_metadata,
                        max_retries=max_retries,
                        target_folder=target_folder,
                        library_root=library_root
                    )
                # else: prune (do not recurse into non-target second-level)
                # i.e., skip Shared Documents/<client>/<XXX != target_folder>

            # Deeper than <library_root>/<client>/<...> and not target -> prune
            else:
                # Do not recurse further
                continue

    except Exception as e:
        if "429" in str(e):
            print("Throttling error persists despite retries.")
        else:
            print(f"Error in get_all_contents_trx_file_only(): {e}")
        print(f"Failed at: {relative_url}")
        return None

    return client_files





# COMMAND ----------

# DBTITLE 1,get_all_contents_digital_reporting_only
# needed imports
from dateutil import parser
import pytz
import random, time

def get_all_contents_digital_reporting_only(
    client_context,
    relative_url,
    check_csv: bool = False,
    return_metadata: bool = False,
    max_retries: int = 5,
    target_folder: str = 'Digital Reporting',   # case-insensitive match
    library_root: str = 'Shared Documents'      # the library you started from
):
    """
    Traverse only: <library_root>/<client>/<target_folder>
      - At <library_root>: only recurse into client folders.
      - At <library_root>/<client>: only recurse into <target_folder>.
      - Any other second-level (<library_root>/<client>/<XXX != target_folder>) is pruned (stop).
    Prints when a target folder is reached and lists files inside.
    """
    global client_files
    global client_files_metadata  # kept for compatibility if used elsewhere

    try:
        retries = 0

        def execute_with_retry(ctx):
            nonlocal retries
            while retries < max_retries:
                try:
                    ctx.execute_query()
                    return
                except Exception as e:
                    if "429" in str(e):
                        wait_time = 2 ** retries + random.uniform(0, 1)
                        print(f"Throttled (429). Retrying in {wait_time:.2f}s... (Attempt {retries + 1}/{max_retries})")
                        time.sleep(wait_time)
                        retries += 1
                    else:
                        raise
            raise Exception("Max retries exceeded. Throttling persists.")

        # Load the current folder
        libraryRoot = client_context.web.get_folder_by_server_relative_url(relative_url)
        client_context.load(libraryRoot)
        execute_with_retry(client_context)

        # Figure out where we are relative to the library root
        parts = [p for p in relative_url.strip('/').split('/') if p]
        try:
            lib_idx = parts.index(library_root)
        except ValueError:
            # If library_root isn't in the path, just don't recurse further.
            lib_idx = None

        depth_after_library = None if lib_idx is None else len(parts) - (lib_idx + 1)
        current_folder_name = parts[-1] if parts else ""

        # If THIS folder is the target, collect files (no further recursion).
        if current_folder_name.lower() == str(target_folder).lower():
            print("📂 Traversing Digital Reporting folder:", relative_url)

            files = libraryRoot.files
            client_context.load(files)
            execute_with_retry(client_context)

            est = pytz.timezone("US/Eastern")

            for client_file in files:
                url = client_file.properties.get('ServerRelativeUrl')
                if not url:
                    continue
                if check_csv and '.csv' not in url.lower():
                    continue

                if return_metadata:
                    created = parser.parse(client_file.properties['TimeCreated']).astimezone(est)
                    modified = parser.parse(client_file.properties['TimeLastModified']).astimezone(est)
                    client_files.append([url, created, modified])
                    print(f"   ↳ File: {url} (Created: {created}, Modified: {modified})")
                else:
                    client_files.append(url)
                    print(f"   ↳ File: {url}")

            return client_files  # done at target level

        # Otherwise, decide whether/where to recurse
        folders = libraryRoot.folders
        client_context.load(folders)
        execute_with_retry(client_context)

        # Depth logic:
        # depth_after_library == 0  -> at <library_root> (only traverse CLIENT folders)
        # depth_after_library == 1  -> at <library_root>/<client> (only traverse <target_folder>)
        # depth_after_library >= 2  -> prune unless already at target (handled above)
        for client_folder in folders:
            folder_name = client_folder.properties.get("Name", "")
            if folder_name.lower() == "forms":
                continue  # skip system folder

            # At library root: go into client folders
            if depth_after_library == 0:
                new_relative_url = relative_url.rstrip('/') + '/' + folder_name
                get_all_contents_digital_reporting_only(
                    client_context,
                    new_relative_url,
                    check_csv=check_csv,
                    return_metadata=return_metadata,
                    max_retries=max_retries,
                    target_folder=target_folder,
                    library_root=library_root
                )

            # At <library_root>/<client>: only go into <target_folder>, skip others
            elif depth_after_library == 1:
                if folder_name.lower() == str(target_folder).lower():
                    new_relative_url = relative_url.rstrip('/') + '/' + folder_name
                    get_all_contents_digital_reporting_only(
                        client_context,
                        new_relative_url,
                        check_csv=check_csv,
                        return_metadata=return_metadata,
                        max_retries=max_retries,
                        target_folder=target_folder,
                        library_root=library_root
                    )
                # else: prune (do not recurse into non-target second-level)
                # i.e., skip Shared Documents/<client>/<XXX != target_folder>

            # Deeper than <library_root>/<client>/<...> and not target -> prune
            else:
                # Do not recurse further
                continue

    except Exception as e:
        if "429" in str(e):
            print("Throttling error persists despite retries.")
        else:
            print(f"Error in get_all_contents_digital_reporting_only(): {e}")
        print(f"Failed at: {relative_url}")
        return None

    return client_files


# COMMAND ----------

# DBTITLE 1,Source Code get_all_contents Functions
#Function to traverse Source Code Folders

from dateutil import parser
import pytz, re, random, time

ISO_SUFFIX_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", re.IGNORECASE)

def get_all_contents_sc_file_only(
    client_context,
    relative_url: str,
    *,
    library_root: str = "Shared Documents",                 # case-insensitive
    mid_path: tuple = ("Reporting 2.0", "SourceCodeDocs"),  # fixed middle path
    allowed_subfolders: tuple = ("Counts", "List", "Package", "Campaign Costs", "Additional_Revenue"),         # folders to search under each client
    clients: tuple = None,                                   # e.g. ("CARE","CHOA","FFB"); None = all
    recurse_within_allowed: bool = True,                     # follow subfolders inside Counts/List
    return_metadata: bool = False,
    check_csv: bool = False,                                 # legacy: only csv
    extensions: tuple = (".xlsx", ".xlsm", ".xls", ".csv"),  # honored if check_csv=False
    strip_iso_suffix: bool = True,                           # fix filenames like foo.xlsx2023-...
    timezone_str: str = "US/Eastern",
    max_retries: int = 5,
):
    """
    Traverse only:
      <library_root>/<mid_path[0]>/<mid_path[1]>/<CLIENT>/(Counts|List)[/**]
    Returns list of urls or (url, created, modified).
    """

    tz = pytz.timezone(timezone_str)

    def _execute_with_retry(ctx):
        retries = 0
        while True:
            try:
                ctx.execute_query()
                return
            except Exception as e:
                s = str(e)
                if "429" in s and retries < max_retries:
                    wait_time = 2 ** retries + random.uniform(0, 1)
                    print(f"Throttled (429). Retrying in {wait_time:.2f}s... (Attempt {retries + 1}/{max_retries})")
                    time.sleep(wait_time)
                    retries += 1
                else:
                    raise

    def _case_insensitive_index(parts, needle):
        nl = needle.lower()
        for i, p in enumerate(parts):
            if p.lower() == nl:
                return i
        return None

    def _is_ext_ok(url):
        ul = url.lower()
        if check_csv:
            return ul.endswith(".csv")
        return any(ul.endswith(e.lower()) for e in extensions)

    def _fix_iso_suffix(name):
        if not strip_iso_suffix:
            return name
        # If a timestamp is glued after the extension, strip it
        return ISO_SUFFIX_RE.sub("", name)

    def _load_folder(path):
        fld = client_context.web.get_folder_by_server_relative_url(path)
        client_context.load(fld)
        _execute_with_retry(client_context)
        return fld

    def _collect_files(folder_obj, folder_url, acc):
        files = folder_obj.files
        client_context.load(files)
        _execute_with_retry(client_context)

        for f in files:
            url = f.properties.get("ServerRelativeUrl")
            if not url:
                continue
            if not _is_ext_ok(url):
                continue

            # Optionally normalize odd filenames (visual/logging only)
            if strip_iso_suffix:
                name = f.properties.get("Name", "")
                fixed = _fix_iso_suffix(name)
                if fixed != name:
                    print(f"   ⚠️ Renaming view: {name} -> {fixed}")

            if return_metadata:
                created_raw = f.properties.get("TimeCreated")
                modified_raw = f.properties.get("TimeLastModified")
                created = parser.parse(created_raw).astimezone(tz) if created_raw else None
                modified = parser.parse(modified_raw).astimezone(tz) if modified_raw else None
                acc.append((url, created, modified))
                print(f"   ↳ File: {url} (Created: {created}, Modified: {modified})")
            else:
                acc.append(url)
                print(f"   ↳ File: {url}")

        if recurse_within_allowed:
            subs = folder_obj.folders
            client_context.load(subs)
            _execute_with_retry(client_context)
            for sf in subs:
                name = sf.properties.get("Name", "")
                if name.lower() == "forms":
                    continue
                new_url = folder_url.rstrip("/") + "/" + name
                sf_obj = _load_folder(new_url)
                _collect_files(sf_obj, new_url, acc)

    # ---- determine where we are in the tree ----
    results = []
    try:
        # Ensure current folder is loaded
        folder = _load_folder(relative_url)

        parts = [p for p in relative_url.strip("/").split("/") if p]
        lib_idx = _case_insensitive_index(parts, library_root)
        depth_after_library = None if lib_idx is None else len(parts) - (lib_idx + 1)
        current_name = parts[-1] if parts else ""

        # Build the expected path steps after library_root
        # We want: <lib>/<mid_path[0]>/<mid_path[1]>/<CLIENT>/<allowed_subfolders>
        after_lib = parts[lib_idx + 1 :] if lib_idx is not None else []

        # If we are exactly at an allowed subfolder (Counts/List), collect (and maybe dive further)
        if after_lib[-1].lower() in {s.lower() for s in allowed_subfolders} and len(after_lib) >= 3:
            print("📂 Traversing allowed folder:", relative_url)
            _collect_files(folder, relative_url, results)
            return _dedupe_sc_pull(results)

        # Otherwise, recurse selectively
        subfolders = folder.folders
        client_context.load(subfolders)
        _execute_with_retry(client_context)

        for sf in subfolders:
            name = sf.properties.get("Name", "")
            lname = name.lower()
            if lname == "forms":
                continue

            # Level: <lib> -> only enter mid_path[0]
            if depth_after_library == 0:
                if lname == mid_path[0].lower():
                    new_url = relative_url.rstrip("/") + "/" + name
                    results.extend(get_all_contents_sc_file_only(
                        client_context, new_url,
                        library_root=library_root, mid_path=mid_path,
                        allowed_subfolders=allowed_subfolders, clients=clients,
                        recurse_within_allowed=recurse_within_allowed,
                        return_metadata=return_metadata, check_csv=check_csv,
                        extensions=extensions, strip_iso_suffix=strip_iso_suffix,
                        timezone_str=timezone_str, max_retries=max_retries
                    ) or [])

            # Level: <lib>/<mid_path[0]> -> only enter mid_path[1]
            elif depth_after_library == 1:
                if lname == mid_path[1].lower():
                    new_url = relative_url.rstrip("/") + "/" + name
                    results.extend(get_all_contents_sc_file_only(
                        client_context, new_url,
                        library_root=library_root, mid_path=mid_path,
                        allowed_subfolders=allowed_subfolders, clients=clients,
                        recurse_within_allowed=recurse_within_allowed,
                        return_metadata=return_metadata, check_csv=check_csv,
                        extensions=extensions, strip_iso_suffix=strip_iso_suffix,
                        timezone_str=timezone_str, max_retries=max_retries
                    ) or [])

            # Level: <lib>/<mid0>/<mid1> -> client folders
            elif depth_after_library == 2:
                if clients and name not in clients:
                    continue  # client allowlist
                new_url = relative_url.rstrip("/") + "/" + name
                results.extend(get_all_contents_sc_file_only(
                    client_context, new_url,
                    library_root=library_root, mid_path=mid_path,
                    allowed_subfolders=allowed_subfolders, clients=clients,
                    recurse_within_allowed=recurse_within_allowed,
                    return_metadata=return_metadata, check_csv=check_csv,
                    extensions=extensions, strip_iso_suffix=strip_iso_suffix,
                    timezone_str=timezone_str, max_retries=max_retries
                ) or [])

            # Level: <lib>/<mid0>/<mid1>/<CLIENT> -> only enter allowed_subfolders
            elif depth_after_library == 3:
                if lname in {s.lower() for s in allowed_subfolders}:
                    new_url = relative_url.rstrip("/") + "/" + name
                    results.extend(get_all_contents_sc_file_only(
                        client_context, new_url,
                        library_root=library_root, mid_path=mid_path,
                        allowed_subfolders=allowed_subfolders, clients=clients,
                        recurse_within_allowed=recurse_within_allowed,
                        return_metadata=return_metadata, check_csv=check_csv,
                        extensions=extensions, strip_iso_suffix=strip_iso_suffix,
                        timezone_str=timezone_str, max_retries=max_retries
                    ) or [])
                # else: prune any other folders (e.g., "Creative", etc.)

            # Deeper: prune unless already handled by the allowed-folder block above
            else:
                continue

    except Exception as e:
        msg = str(e)
        if "429" in msg:
            print("Throttling error persists despite retries.")
        else:
            print(f"Error in get_all_contents_sc_file_only(): {msg}")
        print(f"Failed at: {relative_url}")
        return None

    return _dedupe_sc_pull(results)


def _dedupe_sc_pull(items):
    # Deduplicate while preserving order
    seen = set()
    out = []
    for x in items or []:
        key = x if not isinstance(x, (list, tuple)) else tuple(x)
        if key in seen:
            continue
        seen.add(key)
        out.append(x)
    return out
  


#Function to build df of Source Code Files

def get_sc_files_df(client_context, relative_url="Shared Documents/Reporting 2.0/SourceCodeDocs", **kwargs):
    """
    Wrapper around get_all_contents_sc_file_only that returns a DataFrame.
    Columns: filename, client, folder, created, modified
    """

    # Force metadata return so we capture created/modified
    files_meta = get_all_contents_sc_file_only(
        client_context,
        relative_url,
        return_metadata=True,
        **kwargs
    )

    if not files_meta:
        return pd.DataFrame(columns=["filename", "client", "folder", "created", "modified"])

    rows = []
    for url, created, modified in files_meta:
        parts = url.strip("/").split("/")
        # Example: Shared Documents / Reporting 2.0 / SourceCodeDocs / CHOA / Counts / file.xlsx
        try:
            client_idx = parts.index("SourceCodeDocs") + 1
            client = parts[client_idx]
            folder = parts[client_idx + 1] if len(parts) > client_idx + 1 else None
        except ValueError:
            client = None
            folder = None

        filename = parts[-1]
        rows.append({
            "filename": filename,
            "client": client,
            "folder": folder,
            "created": created,
            "modified": modified
        })

    df = pd.DataFrame(rows)
    return df

# COMMAND ----------

# DBTITLE 1,Get Trx Files
def get_trx_files(client):
  client_data_share_context = get_sharepoint_context(client_data_share_url)
  relative_url = 'Shared Documents/{client}/Transaction Files'.format(client=client)
  try:
    client_files = []
    files = get_all_contents(client_data_share_context,relative_url,return_metadata=True)
  except Exception as e:
    print(repr(e))
  return files

def get_all_trx_files():
  clients = client_list()
  clients.remove('DAV')
  for client in clients:
    print(client)
    try:
      loop_df = get_trx_files(client)
      loop_df['client']=client

      # Start new dataframe if doesn't exist
      if ('all_files' in locals()) | ('all_files' in globals()):
        #print('concat')
        all_files = pd.concat([all_files,loop_df])
      else:
        #print('new')
        all_files = loop_df
    except Exception as e:
      print(repr(e))
  return all_files

# x = get_all_trx_files()
# del all_files

# COMMAND ----------

# DBTITLE 1,Testing: Get file metadata
#file_name = '/Shared Documents/AFHU/Transaction Files/Fuse - Transaction History 10-1-2023 to Present 1-22-2024.CSV'
def get_file_edit_times(file_name, ctx=None):
  if ctx is None:
    # default to client data share if no context is provided
    ctx = get_sharepoint_context(client_data_share_url)
  source_file = ctx.web.get_file_by_server_relative_url(file_name)
  return source_file.time_created, source_file.time_last_modified


# COMMAND ----------

# DBTITLE 1,check_file_name
def check_file_name(f, kws):
  '''
  Checks to see if certain keywords are in the filename.
  Arguments:
  - f: a string representing a file name
  - kws: a list of keyword strings
  Returns:
  - True if any keywords are present in the file name
  - False otherwise
  '''
  if any([x in f for x in kws]):
    return True
  return False


# COMMAND ----------

# DBTITLE 1,execute_transfer
def execute_transfer(source_file, local_file_name, check=False):
  '''
  Transfers a file from SharePoint to the local dbfs directory.
  Arguments:
  - source_file: an office365.sharepoint ClientContext object
  - local_file_name: a string representing the local file name in dbfs
  - check: a flag to control printing to the console
  Returns nothing
  '''
  with open(local_file_name, "wb") as local_file:
    source_file.download(local_file).execute_query()
  if check:
    print("[Ok] file has been downloaded: {0}".format(local_file_name))

# COMMAND ----------

# DBTITLE 1,Download/Transfer Files
# transfer files
def transfer_file(file_name, check=False):
  '''
  Wrapper for transferring individual files from SharePoint to dbfs.
  When file names contain any of the keywords specified, the SharePoint
    directory structure is retained.  Otherwise, the files are written to 
    the root of the client Raw directory.
  Arguments:
  - file_name: a string representing the file path in SharePoint
  - check: a flg to control printing to the console
  Returns nothing
  '''
  try:
    source_file = client_context.web.get_file_by_server_relative_url(file_name)
    keywords = ['Matchback', 'Data', 'Email']
    if check_file_name(file_name, keywords):
      local_file_name = filemap.RAW + file_name.replace('/' + relative_url, '').replace('//', '/')
    else:
      local_file_name = filemap.RAW + file_name.split('/')[-1].replace('//', '/')
    if check:
      print('SharePoint file name: ', file_name)
      print('Local file name: ', local_file_name)
    
    # transfer file
    execute_transfer(source_file, local_file_name, check=check)
  except Exception as e:
    raise(e)
    
    
def transfer_file_basic(file_name, check=False):
  '''
  TODO
  '''
  try:
    if '#' in file_name:
      source_file = client_context.web.get_file_by_server_relative_path(file_name)
    else:
      source_file = client_context.web.get_file_by_server_relative_url(file_name)
    print('**new comment** source file: ', source_file)
    local_file_name = filemap.RAW + file_name.split('/')[-1].replace('//', '/')
    if check:
      print('SharePoint file name: ', file_name)
      print('Local file name: ', local_file_name)
    
    # transfer file
    execute_transfer(source_file, local_file_name, check=check)
  except Exception as e:
    raise(e)
    
    
def transfer_zip(file_name, check=False):
  '''
  Wrapper for transferring individual zip files from SharePoint to dbfs.
  Arguments:
  - file_name: a string representing the file path in SharePoint
  - check: a flg to control printing to the console
  Returns nothing
  '''
  file_name = file_name[0]
  try:
    source_file = client_context.web.get_file_by_server_relative_url(file_name)
    local_file_name = filemap.RAW + file_name.split('/')[-1].replace('//', '/')
    local_file_name = local_file_name.replace(' ', '_')
    
    if check:
      print('SharePoint file name: ', file_name)
      print('Local file name: ', local_file_name)
    
    # transfer file    
    execute_transfer(source_file, local_file_name, check=check)
    
    return local_file_name
  except Exception as e:
    raise(e)

# pass file name and url


# download a file from within an authorized context
def download_from_sp(ctx, tx_file):
  #ctx = get_sharepoint_context(site_url)

  # write flat file to SharePoint
  client = tx_file.split('/')[1]
  print(client)

  file_name = tx_file.split('/')[-1]
  print(file_name)
  
  # point to the individual log file
  source_file = ctx.web.get_file_by_server_relative_url('/sites/ClientDataShare/' + tx_file) #this needs to be generalized
  local_file_name = os.path.join('/tmp', file_name)
  
  # transfer file from Share Point to dbfs
  execute_transfer(source_file, local_file_name, check=True)

  res = 'Downloaded %s' %tx_file
  return res, client, file_name

def download_from_sharepoint(relative_file_url, context_url="https://mindsetdirect.sharepoint.com/sites/ClientDataShare"):
  import os
  context = get_sharepoint_context(context_url)
  source_file = context.web.get_file_by_server_relative_url(relative_file_url)
  file_name = os.path.basename(relative_file_url) #relative_file_url.split('/')[-1]

  local_file_name = os.path.join('/tmp', file_name)
  execute_transfer(source_file, local_file_name, check=True)
  
  res = 'Downloaded %s' %relative_file_url
  return res, file_name, local_file_name

def upload_to_sharepoint(client_context, relative_url, file_name): 
  try:
    # establish share point context

    libraryRoot = client_context.web.get_folder_by_server_relative_url(relative_url)
    libraryRoot

    # create and load file object
    info = FileCreationInformation()    
    with open(os.path.join('/tmp', file_name), 'rb') as content_file:
      info.content = content_file.read()

    info.url = file_name
    info.overwrite = True
    upload_file = libraryRoot.files.add(info)

    # transfer file to share point
    client_context.execute_query()
    
    return 'Transfered %s to %s' %(file_name, relative_url)
    
  except Exception as e:
    raise e

# remove after all references are switched over to upload_to_sharepoint
def transfer_from_cluster(client_context, relative_url, file_name): 
  upload_to_sharepoint(client_context, relative_url, file_name)
  print('Developer Note: Update the reference to "transfer_from_cluster" in this code to "upload_to_sharepoint"')

# COMMAND ----------

# DBTITLE 1,Digital Transfers

# def transfer_digital_programmatic_ads_data(digital_client):
#   digital_data_url= f'Shared Documents/{client}/Digital Reporting/Programmatic Ads Data'
#   # if isinstance(df, pd.DataFrame) and client == 'ASJ':

#   # Sharepoint setup 
#   import os  # Import os at the top of the file
#   import json
#   import pandas as pd
#   import io
#   from office365.sharepoint.files.file import File

#   # SharePoint settings
#   SHAREPOINT_URL = "https://mindsetdirect.sharepoint.com/sites/ClientDataShare"
#   filemap_sp = Filemap('Fuse')
#   with open(os.path.join(filemap_sp.SCHEMA, 'schema.json'), 'r') as f:
#     USER = json.load(f)['SharePointUID']

#   # initiate client context (transfer_files)
#   client_context = get_sharepoint_context_app_only(SHAREPOINT_URL)
#   print("Get Sharepoint contect - Confirmed")

#   # get site contents (transfer_files)
#   client_files = []
#   client_files = get_folder_files(client_context, digital_data_url)

#   # Find the first Excel file in the list
#   # Define the target file prefix
#   TARGET_PREFIX = "MiQ_Just Safe_Fuse Fundraising"
    
#     # Initialize a variable to hold the path of the target file
#   target_file_path = None

#     # Iterate through the list of file paths to find a match
#   for file_path in client_files:
#         file_name = os.path.basename(file_path)
        
#         # Check if the file name starts with the target prefix AND is an Excel file
#         if file_name.startswith(TARGET_PREFIX) and file_name.endswith('.xlsx'):
#             target_file_path = file_path
#             # NOTE: If multiple files match, this will select the FIRST one returned by get_all_contents.
#             break 
    
#       # Check if the target file was found
#         if not target_file_path:
#           raise FileNotFoundError(f"No Excel file found starting with '{TARGET_PREFIX}' in {digital_data_url}")

#       # --- REVISED LOGIC END ---

#       # Download and read the specific Excel file
#   from office365.sharepoint.files.file import File
#   import pandas as pd
#   import io
#   import os

#   print(f"Reading file: {target_file_path}")