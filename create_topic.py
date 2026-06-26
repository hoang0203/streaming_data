from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import time

KAFKA_SERVER = "kafka:29092" # Use your internal docker-compose Kafka name/port
TOPIC_NAME = "pgstream_v3.public.orders"

print(f"⏳ Connecting to Kafka at {KAFKA_SERVER}...")

# Add a simple retry loop to wait for Kafka to boot up
connected = False
while not connected:
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER, client_id='init_script')
        connected = True
    except Exception:
        print("Kafka not ready yet, retrying in 3 seconds...")
        time.sleep(3)

print("🚀 Kafka is up! Attempting to create topic...")

try:
    topic = NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)
    admin_client.create_topics(new_topics=[topic], validate_only=False)
    print(f"✅ Topic '{TOPIC_NAME}' created successfully!")
except TopicAlreadyExistsError:
    print(f"👍 Topic '{TOPIC_NAME}' already exists. Moving on.")
except Exception as e:
    print(f"⚠️ Could not create topic: {e}")