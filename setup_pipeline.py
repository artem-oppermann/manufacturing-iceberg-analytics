import json
from confluent_kafka.admin import AdminClient, NewTopic
from minio import Minio

def setup_minio_bucket():
    client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    
    bucket_name = "inspection-data"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Created MinIO bucket: {bucket_name}")
    else:
        print(f"MinIO bucket already exists: {bucket_name}")
    
    return bucket_name

def create_redpanda_topic():
    admin_client = AdminClient({
        'bootstrap.servers': 'localhost:19092'
    })
    
    topic = NewTopic(
        "inspections",
        num_partitions=3,
        replication_factor=1
    )
    
    try:
        futures = admin_client.create_topics([topic])
        for topic_name, future in futures.items():
            future.result()
            print(f"Created Redpanda topic: {topic_name}")
    except Exception as e:
        print(f"ℹTopic might already exist: {e}")

def verify_pipeline_setup():
    print("\nPIPELINE VERIFICATION:")
    print("=" * 40)
    
    try:
        admin_client = AdminClient({'bootstrap.servers': 'localhost:19092'})
        metadata = admin_client.list_topics(timeout=5)
        print(f"✅ Redpanda: {len(metadata.topics)} topics available")
    except Exception as e:
        print(f"Redpanda connection failed: {e}")
    
    try:
        client = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
        buckets = client.list_buckets()
        print(f"✅ MinIO: {len(buckets)} buckets available")
    except Exception as e:
        print(f"MinIO connection failed: {e}")

if __name__ == "__main__":
    print("🚀 Setting up Redpanda + True Iceberg pipeline...")
    setup_minio_bucket()
    create_redpanda_topic()
    verify_pipeline_setup()
    print("\n🎯 Pipeline setup complete!")