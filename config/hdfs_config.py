# HDFS configuration
HDFS_HOST = 'localhost'  # Replace with your HDFS host
HDFS_PORT = 9870         # Default WebHDFS port
HDFS_RAW_DEST_PATH = '/user/hadoop/data/raw'  # Destination path in HDFS
HDFS_CLEAN_DEST_PATH = '/user/hadoop/data/cleaned'  # Destination path in HDFS
LOCAL_RAW_DATA_PATH = '/home/freddy/Documents/Cours_efrei/Data_integration/Projets_data_integration/data_integration_student_loans/data/raw'

# Additional HDFS settings
REPLICATION_FACTOR = 1  # Default replication factor (adjust for cluster setup)