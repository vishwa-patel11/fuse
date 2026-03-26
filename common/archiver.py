# Databricks notebook source
'''
This script serves provides functionality for use accross all clients to archive raw data and clean up any artifacts at the completion of an ETL job.
'''

# COMMAND ----------

# DBTITLE 1,imports
from datetime import datetime
import os
import shutil
import zipfile
import zlib

# COMMAND ----------

## Compress raw files in zip archive

# COMMAND ----------

# make archive directory given date - reuse code from DAV

def make_target_directory(filemap):
  '''
  Creates the target directory and file name given the current date at runtime.
  Arguments:
  -  filemap: a ./mount_datalake/Filemap object containing storage path details
  Returns: 
  -  target: a string containing the client specific archive location
  -  file_name: a string representing the date the archive file was created  
  '''
  # get year, month and day and create the archive directory path
  now = datetime.now()
  year = str(now.year).zfill(2)
  month = str(now.month).zfill(2)
  day = str(now.day).zfill(2)
  hour = str(now.hour).zfill(2)
  minute = str(now.minute).zfill(2)
  second = str(now.second).zfill(2)

  path = '/'.join((year, month, day, hour, minute)) + '/'
  print("log path: ", path)
  file_name = ''.join((year, month, day, hour, minute))
  print("file name: ", file_name + '.log')

  # create the target archive directory
  target = filemap.ARCHIVE + path
  try:
    os.makedirs(target)
  #   open(ARCHIVE + path + 'init.txt', 'w').close()
    return target, file_name
  except FileExistsError as e:
    print('Target file already exists!')
    return target, file_name
  except Exception as e:
    raise(e)
  return None
  

# file_name = make_target_directory(filemap)

# COMMAND ----------

# create the zip archive locally
def create_local_archive(target, file_name):
  '''
  Creates an archive file
  Arguments:
  -  target: a string containing the client specific archive location
  -  file_name: a string representing the date the archive file was created  
  Returns:
  -  True if success, nothing otherwise
  '''
  # target = filemap.ARCHIVE + path
  try:
    shutil.make_archive(base_dir = '', 
                        root_dir = target, 
                        format='zip', 
                        base_name = file_name)
  except Exception as e:
    raise(e)
  return True

# if file_name:
#   local = create_local_archive(target, file_name)

# COMMAND ----------

# write each of the raw files to the local archive
def write_local_zip(filemap, file_name):
  '''
  Loops over the files in the Raw directory and adds them to the local archive.
  Arguments: 
  -  filemap: a ./mount_datalake/Filemap object containing storage path details 
  -  file_name: a string representing the date the archive file was created  
  Returns:
  -  zip_filename: a string representing the name of the local zip file
  '''
  files = os.listdir(filemap.RAW)
  #print(files)

  os.chdir(filemap.RAW)

  try:
    zip_filename = filemap.HOME + file_name + '.zip'
    with zipfile.ZipFile(zip_filename,'w', 
                         compresslevel=zipfile.ZIP_DEFLATED) as zip: 
      # writing each file one by one 
      for fi in files: 
        if os.path.isfile(fi):
          zip.write(fi, compress_type=zipfile.ZIP_DEFLATED, 
                    compresslevel=zipfile.ZIP_DEFLATED)
    return zip_filename
  except Exception as e:
    raise(e)
  return None

# if local:
#   write_local_zip(filemap, file_name)

# COMMAND ----------

# copy the zipped raw files to the archive directory in datalake
def archive_zip(filemap, zip_filename, file_name, target):
  '''
  Transfers the zip file from the ephemeral cluster memory to Azure Datalake
  Arguments:
  -  filemap: a ./mount_datalake/Filemap object containing storage path details 
  -  zip_filename: a string representing the name of the local zip file
  -  file_name: a string representing the date the archive file was created 
  -  target: a string containing the client specific archive location
  Returns:
  -  Nothing
  '''
#   target = filemap.ARCHIVE + path
  if os.listdir(target) == []:
    shutil.copyfile(zip_filename, target + file_name + '.zip')
  else:
    desc = 'Data is already archived in this directory: %s!' % target
    print(desc)
  
# if zip_filename:
#   archive_zip(filemap, zip_filename, file_name)

# COMMAND ----------

# assert (roughly) that the file transfer is successful
def assert_size_match(file_1, file_2):
  '''
  Crude assertion that the files were archived correctly, using file size as the metric for comparison.
  Arguments:
  -  file_1: a string containing the full relative path to the local file
  -  file_2: a string containing the full relative path to the remote file
  Returns:
  -  True if the file sizes are equal, otherwise raises an exception
  '''
  try:
    assert os.path.getsize(file_1) == os.path.getsize(file_2)
  except AssertionError as e:
    raise(e)
    
# assert_size_match(zip_filename, target + file_name + '.zip')

# COMMAND ----------

# Clean up local files
def cleanup_raw(filemap, subs=False):
  '''
  TODO
  e.g. subs=['Data/', 'Matchbacks/', 'Reference/']
  '''
  try: 
    local_dir = filemap.RAW
    [os.remove(local_dir + f) for f in os.listdir(local_dir) if os.path.isfile(local_dir + f)]
    if subs:     
      for sub in subs:
        path = local_dir + sub
        [os.remove(path + f) for f in os.listdir(path) if os.path.isfile(path + f)]
#       [os.rmdir(local_dir + f) for f in os.listdir(local_dir) if os.path.isdir(local_dir + f)]
      
  except Exception as e:
    raise(e)
    
  return os.listdir(local_dir)
       

# COMMAND ----------

# Clean up local files
def cleanup_local(filemap, file_name, subdirectory = False):
  '''
  Cleans up original, decompressed files.
  Arguments:
  -  filemap: a ./mount_datalake/Filemap object containing storage path details 
  -  file_name: a string representing the date the archive file was created
  -  subdirectory (optional): a list of subdirectories in Raw to be purged.
  Returns:
  -  Nothing
  '''
  try:
    local = filemap.HOME + file_name + '.zip'
    os.remove(local)
    # os.listdir(filemap.HOME)
  except Exception as e:
    raise(e)

  # Clean up /Raw directory
  try: 
    [os.remove(f) for f in os.listdir(filemap.RAW) if os.path.isfile(f)]
    if subdirectory:
      for sub in subdirectory:
        path = filemap.RAW + sub
        [os.remove(path + f) for f in os.listdir(path) if os.path.isfile(path + f)]
      
  except Exception as e:
    raise(e)
    
    
# cleanup_local(filemap, file_name, subdirectory = '/Matchbacks')    

# COMMAND ----------

# run archiving task
def run_archiver(filemap, subs = False):
  '''
  Orchestrator function to control each of the archiving steps.
  Arguments:
  -  filemap: a ./mount_datalake/Filemap object containing storage path details 
  Returns:
  -  True if archiving was successful, otherwise raises an exception
  '''
  try:
    # derive the target directory and the file_name from the current timestamp
    target, file_name = make_target_directory(filemap)

    # create the local archive file
    create_local_archive(target, file_name)

    # write to the local archive file
    zip_filename = write_local_zip(filemap, file_name)

    # transfer the archive file to the Datalake
    archive_zip(filemap, zip_filename, file_name, target)

    # assert some measure of integrity after the transfer
    assert_size_match(zip_filename, target + file_name + '.zip')

    # clean up local artifacts
    cleanup_local(filemap, file_name, subdirectory = subs)  
    
    return True
  except Exception as e:
    raise(e)