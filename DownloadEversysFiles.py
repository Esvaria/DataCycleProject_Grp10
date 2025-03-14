import os
import uuid
import time
from concurrent.futures import ThreadPoolExecutor
from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open, FilePipePrinterAccessMask
from smbprotocol.file_info import FileInformationClass
from smbprotocol.open import ImpersonationLevel, CreateDisposition, CreateOptions, ShareAccess, FileAttributes

# Configuration SMB
SMB_SERVER = "10.130.25.152"
SMB_SHARE = "Eversys"
SMB_USER = "Student"
SMB_PASSWORD = "3uw.AQ!SWxsDBm2zi3"

# Local folder to save DAT files
DAT_FILES_FOLDER = "EversysDatFiles"
os.makedirs(DAT_FILES_FOLDER, exist_ok=True)

# Connect to SMB
def connect_smb():
    try:
        conn = Connection(uuid.uuid4(), SMB_SERVER, 445)
        conn.connect(timeout=30)

        session = Session(conn, SMB_USER, SMB_PASSWORD)
        session.connect()

        tree = TreeConnect(session, f"\\\\{SMB_SERVER}\\{SMB_SHARE}")
        tree.connect()

        print(f"Successfully connected to SMB: {SMB_SERVER}\\{SMB_SHARE}")
        return tree, conn, session
    except Exception as e:
        print(f"SMB Connection failed: {e}")
        return None, None, None

# Retrieve SMB file list
def list_files(tree):
    """Retrieve list of DAT files from the SMB share."""
    try:
        dir_open = Open(tree, "")
        dir_open.create(
            impersonation_level=ImpersonationLevel.Impersonation,
            desired_access=FilePipePrinterAccessMask.GENERIC_READ,
            file_attributes=FileAttributes.FILE_ATTRIBUTE_NORMAL,
            share_access=ShareAccess.FILE_SHARE_READ,
            create_disposition=CreateDisposition.FILE_OPEN,
            create_options=CreateOptions.FILE_DIRECTORY_FILE
        )

        entries = []
        while True:
            try:
                batch = dir_open.query_directory(pattern="*", file_information_class=FileInformationClass.FILE_NAMES_INFORMATION)
                batch_files = [entry["file_name"].get_value().decode("utf-16-le") for entry in batch]

                # Filter only .DAT files and remove "." and ".."
                batch_files = [f for f in batch_files if f.endswith(".dat") and f not in (".", "..")]

                if not batch_files:
                    break

                entries.extend(batch_files)
            except Exception as e:
                if "STATUS_NO_MORE_FILES" in str(e):
                    print("No more files to retrieve from the server.")
                    break
                else:
                    print(f"Error retrieving batch: {e}")
                    break

        dir_open.close()
        print(f"Total DAT files found: {len(entries)}")
        return entries
    except Exception as e:
        print(f"Error listing files: {e}")
        return []

# Check if file already exists locally
def local_file_exists(filename):
    return os.path.exists(os.path.join(DAT_FILES_FOLDER, filename))

# Download SMB file
def download_file(tree, filename):
    local_path = os.path.join(DAT_FILES_FOLDER, filename)

    if local_file_exists(filename):
        print(f"Skipping {filename} - Already exists locally.")
        return None

    try:
        file_open = Open(tree, filename)
        file_open.create(
            impersonation_level=ImpersonationLevel.Impersonation,
            desired_access=FilePipePrinterAccessMask.GENERIC_READ,
            file_attributes=FileAttributes.FILE_ATTRIBUTE_NORMAL,
            share_access=ShareAccess.FILE_SHARE_READ,
            create_disposition=CreateDisposition.FILE_OPEN,
            create_options=CreateOptions.FILE_NON_DIRECTORY_FILE
        )

        data = file_open.read(0, file_open.end_of_file)
        file_open.close()

        if data:
            with open(local_path, "wb") as f:
                f.write(data)
            print(f"Downloaded {filename}")
            return local_path
        else:
            print(f"Skipping {filename}: Empty file")
            return None
    except Exception as e:
        print(f"Error opening file: {filename} - {e}")
        return None

# Multithreading for faster downloads
def threaded_download(tree, file_list):
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(lambda f: download_file(tree, f), file_list)

# Main function
def main():
    tree, conn, session = connect_smb()
    if not tree:
        return

    all_files = list_files(tree)
    new_files = [f for f in all_files if not local_file_exists(f)]

    if not new_files:
        print("No new DAT files to download.")
    else:
        print(f"Downloading {len(new_files)} new DAT files...")
        threaded_download(tree, new_files)

    session.disconnect()
    conn.disconnect()
    print("All DAT files have been downloaded successfully.")

if __name__ == "__main__":
    main()
