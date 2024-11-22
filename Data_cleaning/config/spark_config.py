# Spark setting
from config.kafka_config import TOPIC_NAME_FL_DASHBOARD, TOPIC_NAME_SCHOOL_CODELIST

APP_NAME = "StudentLoanDataIntegration"
MASTER = "local[*]"  # Use "yarn" if on a cluster
BATCH_DURATION = 10  # Duration in seconds for Spark Streaming batch intervals

# Kafka Spark settings
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"  # Kafka brokers for Spark Kafka integration
TOPICS = [TOPIC_NAME_FL_DASHBOARD, TOPIC_NAME_SCHOOL_CODELIST]  # Topics for streaming