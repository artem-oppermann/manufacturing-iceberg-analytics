from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType, DoubleType, TimestampType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
import os

def create_iceberg_catalog():
    
    os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin'
    os.environ['AWS_ENDPOINT_URL'] = 'http://localhost:9000'
    os.environ['AWS_REGION'] = 'us-east-1'
    os.environ['AWS_S3_ALLOW_UNSAFE_RENAME'] = 'true'
    
    warehouse_path = "/tmp/iceberg-warehouse"
    os.makedirs(warehouse_path, exist_ok=True)
    
    catalog_config = {
        "uri": f"sqlite:///{warehouse_path}/catalog.db",
        "warehouse": f"s3://inspection-data/warehouse",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.region": "us-east-1",
        "s3.path-style-access": "true"
    }
    
    try:
        catalog = SqlCatalog("manufacturing", **catalog_config)
        print("✅ Created true PyIceberg SQL catalog with MinIO backend")
        return catalog
    except Exception as e:
        print(f"❌ Failed to create catalog: {e}")
        raise

def get_inspection_schema():
    
    schema = Schema(
        NestedField(1, "product_id", StringType(), required=True),        
        NestedField(2, "result", StringType(), required=True),           
        NestedField(3, "defect_type", StringType(), required=False),     
        NestedField(4, "confidence", DoubleType(), required=True),       
        NestedField(5, "timestamp", LongType(), required=True),          
        NestedField(6, "inspection_station", StringType(), required=True), 
        NestedField(7, "operator_id", StringType(), required=True),      
        NestedField(8, "shift_id", StringType(), required=True),         
        NestedField(9, "batch_number", StringType(), required=True),     
        NestedField(10, "line_speed", DoubleType(), required=True),      
        NestedField(11, "temperature", DoubleType(), required=True),     
        NestedField(12, "humidity", DoubleType(), required=True),        
        NestedField(13, "event_time", TimestampType(), required=True)    
    )
    
    return schema

def get_partition_spec():
    
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=13, 
            field_id=1000,
            transform=DayTransform(),
            name="event_date"
        )
    )
    
    return partition_spec

def setup_manufacturing_table(catalog):
    
    try:
        table = catalog.load_table("manufacturing.quality_inspections")
        return table
        
    except Exception:
        schema = get_inspection_schema()
        partition_spec = get_partition_spec()
        
        try:
            catalog.create_namespace("manufacturing")
        except Exception:
            pass  
        
        table = catalog.create_table(
            identifier="manufacturing.quality_inspections",
            schema=schema,
            partition_spec=partition_spec,
            properties={
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "gzip",
                "write.metadata.compression-codec": "gzip"
            }
        )
        
        print(" Created new PyIceberg table: manufacturing.quality_inspections")
        print(f" Schema: {len(schema.fields)} fields")
        print(f"  Partitioning: day-based on event_time")
        return table

if __name__ == "__main__":
    catalog = create_iceberg_catalog()
    table = setup_manufacturing_table(catalog)
    print(f" True Iceberg table ready with {len(table.schema().fields)} fields")