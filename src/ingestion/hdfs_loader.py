import os
import pyhdfs
import logging
from config import hdfs_config





# Set up logging
logging.basicConfig(
    filename="logs/hdfs_loader.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def upload_files_to_hdfs(local_path, hdfs_path, hdfs_client):
    """Uploads files from a local directory to HDFS."""
    
    try:
        # Ensure the HDFS directory exists
        if not hdfs_client.exists(hdfs_path):
            hdfs_client.mkdirs(hdfs_path)
            logging.info(f"Created HDFS directory: {hdfs_path}")
        
        # Iterate through files in the local raw data directory
        for file_name in os.listdir(local_path):
            local_file_path = os.path.join(local_path, file_name)
            
            # Only process files
            if os.path.isfile(local_file_path):
                hdfs_file_path = f"{hdfs_path}/{file_name}"
                
                # Upload file to HDFS
                with open(local_file_path, 'rb') as file_data:
                    hdfs_client.create(hdfs_file_path, file_data)
                logging.info(f"Uploaded {file_name} to HDFS at {hdfs_file_path}")
                
                # Verify upload success
                if hdfs_client.exists(hdfs_file_path):
                    logging.info(f"Successfully uploaded: {file_name}")
                else:
                    logging.error(f"Failed to upload: {file_name}")

    except Exception as e:
        logging.error(f"Error uploading files to HDFS: {e}")

def main():
    """Main function to initialize HDFS client and upload files."""
    
    try:
        # Initialize HDFS client
        hdfs_client = pyhdfs.HdfsClient(hosts=f"{HDFS_HOST}:{HDFS_PORT}")
        logging.info("HDFS client initialized")
        
        # Start uploading files
        upload_files_to_hdfs(LOCAL_RAW_DATA_PATH, HDFS_RAW_DEST_PATH, hdfs_client)
        
    except Exception as e:
        logging.error(f"Failed to initialize HDFS client: {e}")

if __name__ == "__main__":
    main()
