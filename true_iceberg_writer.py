import json
import pandas as pd
import pyarrow as pa
from confluent_kafka import Consumer, KafkaError
from datetime import datetime
import time
from iceberg_catalog import create_iceberg_catalog, setup_manufacturing_table

class TrueIcebergWriter:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': 'localhost:19092',
            'group.id': 'true-pyiceberg-writer',
            'auto.offset.reset': 'earliest'
        })
        
        print(" Initializing true PyIceberg catalog...")
        self.catalog = create_iceberg_catalog()
        self.table = setup_manufacturing_table(self.catalog)
        
        self.batch_size = 50  
        self.batch_timeout = 30  
        
        print(" True PyIceberg writer initialized")
        print(f" Table schema: {len(self.table.schema().fields)} fields")
        print(f"  Partitioning: day-based partitioning on event_time")
    
    def consume_and_commit(self):
        self.consumer.subscribe(['inspections'])
        
        messages_batch = []
        last_commit_time = time.time()
        commit_counter = 0
        
        print(" Starting Redpanda → True PyIceberg pipeline...")
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    # Check if we should commit accumulated messages
                    if (messages_batch and 
                        time.time() - last_commit_time > self.batch_timeout):
                        self.atomic_commit_to_iceberg(messages_batch, commit_counter)
                        messages_batch = []
                        commit_counter += 1
                        last_commit_time = time.time()
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Consumer error: {msg.error()}")
                        break
                
                try:
                    message_data = json.loads(msg.value().decode('utf-8'))
                    
                    iceberg_record = self.transform_to_iceberg_format(message_data)
                    messages_batch.append(iceberg_record)
                    
                    print(f"📥 Queued: {iceberg_record['product_id']} - {iceberg_record['result']}")
                    
                    if len(messages_batch) >= self.batch_size:
                        self.atomic_commit_to_iceberg(messages_batch, commit_counter)
                        messages_batch = []
                        commit_counter += 1
                        last_commit_time = time.time()
                        
                except Exception as e:
                    print(f"Error processing message: {e}")
                    
        except KeyboardInterrupt:
            print("🛑 Stopping PyIceberg writer...")
            
            if messages_batch:
                self.atomic_commit_to_iceberg(messages_batch, commit_counter)
                
        finally:
            self.consumer.close()
    
    def transform_to_iceberg_format(self, raw_event):
        
        event_time = datetime.fromtimestamp(raw_event['timestamp'] / 1000)
        
        iceberg_record = {
            'product_id': raw_event['product_id'],
            'result': raw_event['result'],
            'defect_type': raw_event.get('defect_type'),
            'confidence': float(raw_event['confidence']),
            'timestamp': int(raw_event['timestamp']),
            'inspection_station': raw_event['inspection_station'],
            'operator_id': raw_event['operator_id'],
            'shift_id': raw_event['shift_id'],
            'batch_number': raw_event['batch_number'],
            'line_speed': float(raw_event['line_speed']),
            'temperature': float(raw_event['temperature']),
            'humidity': float(raw_event['humidity']),
            'event_time': event_time
        }
        
        return iceberg_record
    
    def atomic_commit_to_iceberg(self, records, commit_id):
        if not records:
            return
            
        try:
            current_snapshot = self.table.current_snapshot()
            current_snapshot_id = current_snapshot.snapshot_id if current_snapshot else None
            
            arrow_table = self.create_arrow_table_from_records(records)
            
            print(f" Committing {len(records)} records to PyIceberg...")
            
            self.table.append(arrow_table)
            
            new_snapshot = self.table.current_snapshot()
            
            print(f" PyIceberg atomic commit #{commit_id}: {len(records)} records")
            print(f"    Snapshot: {current_snapshot_id} → {new_snapshot.snapshot_id}")
            print(f"    Commit time: {datetime.fromtimestamp(new_snapshot.timestamp_ms / 1000)}")
            
        except Exception as e:
            print(f"❌ PyIceberg atomic commit failed: {e}")
            raise

    def create_arrow_table_from_records(self, records):
        
        product_ids = [record['product_id'] for record in records]
        results = [record['result'] for record in records]
        defect_types = [record['defect_type'] for record in records]
        confidences = [record['confidence'] for record in records]
        timestamps = [record['timestamp'] for record in records]
        inspection_stations = [record['inspection_station'] for record in records]
        operator_ids = [record['operator_id'] for record in records]
        shift_ids = [record['shift_id'] for record in records]
        batch_numbers = [record['batch_number'] for record in records]
        line_speeds = [record['line_speed'] for record in records]
        temperatures = [record['temperature'] for record in records]
        humidities = [record['humidity'] for record in records]
        event_times = [pd.to_datetime(record['event_time']).tz_localize(None) for record in records]
        
        pyarrow_schema = pa.schema([
            pa.field('product_id', pa.string(), nullable=False),
            pa.field('result', pa.string(), nullable=False),
            pa.field('defect_type', pa.string(), nullable=True),  
            pa.field('confidence', pa.float64(), nullable=False),
            pa.field('timestamp', pa.int64(), nullable=False),
            pa.field('inspection_station', pa.string(), nullable=False),
            pa.field('operator_id', pa.string(), nullable=False),
            pa.field('shift_id', pa.string(), nullable=False),
            pa.field('batch_number', pa.string(), nullable=False),
            pa.field('line_speed', pa.float64(), nullable=False),
            pa.field('temperature', pa.float64(), nullable=False),
            pa.field('humidity', pa.float64(), nullable=False),
            pa.field('event_time', pa.timestamp('us'), nullable=False)
        ])
        
        arrays = [
            pa.array(product_ids),
            pa.array(results),
            pa.array(defect_types),  
            pa.array(confidences),
            pa.array(timestamps),
            pa.array(inspection_stations),
            pa.array(operator_ids),
            pa.array(shift_ids),
            pa.array(batch_numbers),
            pa.array(line_speeds),
            pa.array(temperatures),
            pa.array(humidities),
            pa.array(event_times)
        ]
        
        return pa.table(arrays, schema=pyarrow_schema)

if __name__ == "__main__":
    writer = TrueIcebergWriter()
    writer.consume_and_commit()