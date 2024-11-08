# Script to load files into HDFS# import the python subprocess module
import os
import pyhdfs

# HDFS configuration
HDFS_HOST = 'localhost'  # Replace with your HDFS host
HDFS_PORT = 9870         # Default WebHDFS port
HDFS_DEST_PATH = '/user/hadoop/data/raw'  # Destination path in HDFS

# Local directory containing raw data files
LOCAL_RAW_DATA_PATH = '/home/freddy/Documents/Cours_efrei/Data_integration/Projets_data_integration/data_integration_student_loans/data/raw'

def upload_files_to_hdfs(local_path, hdfs_path, hdfs_client):
    # Ensure the HDFS directory exists
    if not hdfs_client.exists(hdfs_path):
        hdfs_client.mkdirs(hdfs_path)
        print(f"Created HDFS directory: {hdfs_path}")
    
    # Iterate through files in the local raw data directory
    for file_name in os.listdir(local_path):
        local_file_path = os.path.join(local_path, file_name)
        
        # Only process files
        if os.path.isfile(local_file_path):
            hdfs_file_path = f"{hdfs_path}/{file_name}"
            
            # Upload file to HDFS
            with open(local_file_path, 'rb') as file_data:
                hdfs_client.create(hdfs_file_path, file_data)
            print(f"Uploaded {file_name} to HDFS at {hdfs_file_path}")
            
            # Verify upload success
            if hdfs_client.exists(hdfs_file_path):
                print(f"Successfully uploaded: {file_name}")
            else:
                print(f"Failed to upload: {file_name}")

def main():
    # Initialize HDFS client
    hdfs_client = pyhdfs.HdfsClient(hosts=f"{HDFS_HOST}:{HDFS_PORT}")
    
    # Start uploading files
    upload_files_to_hdfs(LOCAL_RAW_DATA_PATH, HDFS_DEST_PATH, hdfs_client)

if __name__ == "__main__":
    main()
