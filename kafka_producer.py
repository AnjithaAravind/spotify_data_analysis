import boto3
from kafka import KafkaProducer

# Initialize S3 client (for quick testing; use IAM roles/env vars for production)
s3 = boto3.client(
    's3',
    aws_access_key_id='<your-access-id>',
    aws_secret_access_key='<your-secret-access-key>',
    region_name='ap-south-1'
)

# Define your files
files = [
    'transformed_data/album_data/album_transformed_2025-04-25_18-05-46.json',
    'transformed_data/artist_data/artist_transformed_2025-04-25_18-05-48.json',
    'transformed_data/songs_data/songs_transformed_2025-04-25_18-05-48.json'
    ]


bucket_name = 'myspotibucket'
kafka_topic = 'my-topic'

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: v.encode('utf-8')
)

# Loop through all files
for object_key in files:
    print(f"Reading from: {object_key}")
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    lines = response['Body'].read().decode('utf-8').splitlines()

    for line in lines[1:]:  # skip header
        producer.send(kafka_topic, value=line)
        print(f"Sent: {line}")

# Flush the producer at the end
producer.flush()
print("All data sent to Kafka.")
