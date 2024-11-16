
# # HDFS configuration
# HDFS_HOST = 'localhost'  # Replace with your HDFS host
# HDFS_PORT = 9870         # Default WebHDFS port
# HDFS_RAW_DEST_PATH = '/user/hadoop/data/raw'  # Destination path in HDFS
# HDFS_CLEAN_DEST_PATH = '/user/hadoop/data/cleaned'  # Destination path in HDFS
# LOCAL_RAW_DATA_PATH = '/home/freddy/Documents/Cours_efrei/Data_integration/Projets_data_integration/data_integration_student_loans/data/raw'

# # Additional HDFS settings
# REPLICATION_FACTOR = 1  # Default replication factor (adjust for cluster setup)


# hdfs_config.py

class HDFSConfig:
    """
    Configuration settings for HDFS integration in a data pipeline.
    
    Attributes:
        HOST (str): The hostname or IP address of the HDFS Namenode.
        PORT (int): The WebHDFS port for HTTP connections.
        RAW_DEST_PATH (str): HDFS path for storing raw, unprocessed data.
        CLEAN_DEST_PATH (str): HDFS path for storing cleaned or processed data.
        LOCAL_RAW_DATA_PATH (str): Local path where raw data is initially stored.
        REPLICATION_FACTOR (int): Default replication factor for HDFS files.
    """
    
    # HDFS Connection Settings
    HOST = 'localhost'  # Replace with your HDFS host
    PORT = 9870         # Default WebHDFS port

    # HDFS Destination Paths
    RAW_DEST_PATH = '/user/hadoop/data/raw'      # Destination path in HDFS for raw data
    CLEAN_DEST_PATH = '/user/hadoop/data/cleaned'  # Destination path in HDFS for cleaned data

    # Local Paths
    LOCAL_RAW_DATA_PATH = '/home/freddy/Documents/Cours_efrei/Data_integration/Projets_data_integration/data_integration_student_loans/data/raw'

    # HDFS File Settings
    REPLICATION_FACTOR = 1  # Adjust for cluster setup
