# HDFS settings and paths

HDFS_URL = "hdfs://localhost:9000"  # HDFS namenode address
RAW_DATA_PATH = "/data/raw"  # Directory for raw input files in HDFS
PROCESSED_DATA_PATH = "/data/processed"  # Directory for processed data
VERSIONED_DATA_PATH = "/data/versions"  # Directory for versioned data

# Additional HDFS settings
REPLICATION_FACTOR = 1  # Default replication factor (adjust for cluster setup)