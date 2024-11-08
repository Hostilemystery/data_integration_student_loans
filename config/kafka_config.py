# Kafka settings (topic names, servers, etc.)
KAFKA_BROKER_URL = "localhost:9092"  # Update with the actual broker address
TOPIC_NAME_FL_DASHBOARD = "fl_dashboard_topic"  # Topic for FL_Dashboard files
TOPIC_NAME_SCHOOL_CODELIST = "school_codelist_topic"  # Topic for 1617fedschoolcodelist file
BATCH_SIZE = 100  # Number of records per batch
POLL_INTERVAL = 10  # Interval between batches in seconds