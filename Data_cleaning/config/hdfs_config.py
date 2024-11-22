class HDFSConfig:
    # HDFS Connection Settings
    HOST = 'localhost'
    PORT = 9870 # Make sure this matches the port you've set in core-site.xml

    # HDFS Destination Paths
    RAW_DEST_PATH = '/user/anthonycormeaux/data/raw'
    CLEAN_DEST_PATH = '/user/anthonycormeaux/data/cleaned'
    COMBINED_DEST_PATH = '/user/anthonycormeaux/data/combined'

    # Local Paths
    LOCAL_RAW_DATA_PATH = '/Users/anthonycormeaux/Documents/Projet_data_integration/Nouvelle version/data_integration_student_loans/data/raw'
    LOCAL_CLEAN_DATA_PATH = '/Users/anthonycormeaux/Documents/Projet_data_integration/Nouvelle version/data_integration_student_loans/data/cleaned'
    LOCAL_COMBINED_DATA_PATH = "/Users/anthonycormeaux/Documents/Projet_data_integration/Nouvelle version/data_integration_student_loans/data/combined"

    # HDFS File Settings
    REPLICATION_FACTOR = 1