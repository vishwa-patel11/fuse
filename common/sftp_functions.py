# Databricks notebook source
def get_sharepoint_context(site_url, username, password):
    ctx_auth = AuthenticationContext(site_url)
    if ctx_auth.acquire_token_for_user(username, password):
        ctx = ClientContext(site_url, ctx_auth)
        return ctx
    else:
        raise Exception("Authentication failed")


def transfer_from_cluster(client_context, relative_url, local_file_path):
    try:
        if os.path.isfile(local_file_path):
            file_name = os.path.basename(local_file_path)
            library_root = client_context.web.get_folder_by_server_relative_url(relative_url)
            info = FileCreationInformation()
            with open(local_file_path, 'rb') as content_file:
                info.content = content_file.read()
            info.url = file_name
            info.overwrite = True
            upload_file = library_root.files.add(info)
            client_context.execute_query()
            return 'Transferred %s to %s' % (local_file_path, relative_url)
        else:
            print(f"Skipping directory: {local_file_path}")
    except Exception as e:
        print("Error during transfer:", e)
        traceback.print_exc()



def ensure_folder(client_context, folder_url):
    try:
        folder = client_context.web.get_folder_by_server_relative_url(folder_url)
        client_context.load(folder)
        client_context.execute_query()

  
    except Exception:
      try:
        # Folder does not exist, create it
        parent_url, folder_name = os.path.split(folder_url.rstrip('/'))
        parent_folder = client_context.web.get_folder_by_server_relative_url(parent_url)
        new_folder = parent_folder.folders.add(folder_name)
        client_context.execute_query()
      except:
        raise
    return True 




def initialize_upload_directory_to_sharepoint(local_path, sharepoint_relative_url, client_context):
    try:
        if os.path.isdir(local_path):
            # Ensure the SharePoint folder exists
            if ensure_folder(client_context, sharepoint_relative_url):
               #TODO uncomment ensure_folder make sure it works, then ensure folder returns something


              # Upload files in the directory to the specified SharePoint folder
              for item in os.listdir(local_path):
                  local_item_path = os.path.join(local_path, item)
                  if os.path.isfile(local_item_path):
                      # If it's a file, upload it to the SharePoint folder
                      res = transfer_from_cluster(client_context, sharepoint_relative_url, local_item_path)
                      print("Transfer successful:", res)
                  elif os.path.isdir(local_item_path):
                      # If it's a directory, recursively call this function
                      new_sharepoint_relative_url = f"{sharepoint_relative_url}/{item}"
                      upload_directory_to_sharepoint(local_item_path, new_sharepoint_relative_url, client_context)
        else:
            # Single file upload scenario
            res = transfer_from_cluster(client_context, sharepoint_relative_url, local_path)
            print("Transfer successful:", res)
    except Exception as e:
        print("Error during upload:", e)
        traceback.print_exc()


def upload_directory_to_sharepoint(local_path, sharepoint_relative_url, client_context, archive_files):
    try:
        if os.path.isdir(local_path):
            # Ensure the SharePoint folder exists
            if ensure_folder(client_context, sharepoint_relative_url):
                # Upload files in the directory to the specified SharePoint folder
                for item in os.listdir(local_path):
                    local_item_path = os.path.join(local_path, item)
                    if os.path.isfile(local_item_path):
                        # Check if the file is already in the archive
                        relative_path_after_cfg = '/'.join(local_item_path.split('/CFG/')[1:])
                        if relative_path_after_cfg in { '/'.join(y.split('/CFG/')[1:]) for y in archive_files }:
                            print(f"Not uploading {local_item_path} because it is already in the archive")
                        else:
                            # If it's a new file, upload it to the SharePoint folder
                            res = transfer_from_cluster(client_context, sharepoint_relative_url, local_item_path)
                            print(f"Upload successful: {res}")
                    elif os.path.isdir(local_item_path):
                        # If it's a directory, recursively call this function
                        new_sharepoint_relative_url = f"{sharepoint_relative_url}/{item}"
                        upload_directory_to_sharepoint(local_item_path, new_sharepoint_relative_url, client_context, archive_files)
        else:
            # Single file upload scenario
            res = transfer_from_cluster(client_context, sharepoint_relative_url, local_path)
            print(f"Upload successful: {res}")
    except Exception as e:
        print(f"Error during upload: {e}")
        traceback.print_exc()


def download_remote_directory(sftp, remote_path, local_path):
    downloaded_files = []
    try:
        os.makedirs(local_path, exist_ok=True)
        folder_contents = sftp.listdir(remote_path)
        for item in folder_contents:
            remote_item_path = f"{remote_path}/{item}"
            local_item_path = os.path.join(local_path, item)
            remote_item_attr = sftp.stat(remote_item_path)
            if stat.S_ISDIR(remote_item_attr.st_mode):
                # If it's a directory, recursively call this function and extend the list
                downloaded_files.extend(download_remote_directory(sftp, remote_item_path, local_item_path))
            else:
                sftp.get(remote_item_path, local_item_path)
                downloaded_files.append(local_item_path)  # Add the file path to the list
                print(f"Downloaded: {remote_item_path}")
    except Exception as e:
        print(f"Error: {e}")

    return downloaded_files


def list_files_in_directory(local_path):
    files_list = []
    try:
        if os.path.isdir(local_path):
            # List files in the directory
            for item in os.listdir(local_path):
                local_item_path = os.path.join(local_path, item)
                if os.path.isfile(local_item_path):
                    # If it's a file, add it to the list
                    files_list.append(local_item_path)
                elif os.path.isdir(local_item_path):
                    # If it's a directory, recursively call this function
                    files_list.extend(list_files_in_directory(local_item_path))
        else:
            # Single file scenario
            files_list.append(local_path)
    except Exception as e:
        print(f"Error during listing files: {e}")

    return files_list

def move_directory_to_archive(source_dir, archive_dir):
    if os.path.exists(source_dir) and os.path.isdir(source_dir):
        # Ensure the archive directory exists
        os.makedirs(archive_dir, exist_ok=True)
        
        # Determine the new location for the directory
        basename = os.path.basename(source_dir)
        new_location = os.path.join(archive_dir, basename)

        # Remove the existing directory at the destination if it exists
        if os.path.exists(new_location):
            shutil.rmtree(new_location)
            print(f"Removed existing directory at {new_location}")

        # Move the directory
        shutil.move(source_dir, new_location)
        print(f"Moved {source_dir} to {new_location}")
    else:
        print(f"Source directory {source_dir} does not exist.")
