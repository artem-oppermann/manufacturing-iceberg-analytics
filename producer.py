import json
import time
from confluent_kafka import Producer
from datetime import datetime, timedelta
import random

def delivery_report(err, msg):
    """Enhanced delivery callback with status reporting"""
    if err is not None:
        print(f'❌ Message delivery failed: {err}')
    else:
        print(f'✅ Delivered: {msg.key().decode()} to partition {msg.partition()}')

def generate_inspection_event(product_id, shift_id="day_shift", base_timestamp=None):
    """Generate realistic inspection data with metadata for analytics"""
    defect_types = ['scratch', 'dent', 'misalignment', 'color_variation', 'dimension_error']
    stations = ['station_A', 'station_B', 'station_C', 'station_D']
    operators = ['operator_001', 'operator_002', 'operator_003']
    
    # Simulate realistic failure patterns (15% defect rate)
    is_defective = random.random() < 0.15
    
    # Use provided timestamp or current time
    if base_timestamp is None:
        event_timestamp = int(time.time() * 1000)
    else:
        event_timestamp = base_timestamp
    
    event = {
        'product_id': product_id,
        'result': 'NG' if is_defective else 'OK',
        'defect_type': random.choice(defect_types) if is_defective else None,
        'confidence': round(random.uniform(0.85, 0.99), 3),
        'timestamp': event_timestamp,
        'inspection_station': random.choice(stations),
        'operator_id': random.choice(operators),
        'shift_id': shift_id,
        'batch_number': f"BATCH_{random.randint(1000, 9999)}",
        'line_speed': round(random.uniform(45.0, 55.0), 1),  # units/min
        'temperature': round(random.uniform(20.0, 25.0), 1),  # Celsius
        'humidity': round(random.uniform(40.0, 60.0), 1)      # %
    }
    
    return event

def produce_historical_data():
    """Produce inspection data with historical timestamps for time-travel demo"""
    config = {
        'bootstrap.servers': 'localhost:19092',
        'acks': 'all',
        'retries': 5,
        'retry.backoff.ms': 100,
        'request.timeout.ms': 30000,
        'delivery.timeout.ms': 60000
    }
    
    producer = Producer(config)
    topic = 'inspections'
    
    print("🚀 Producing historical inspection data for PyIceberg time-travel...")
    print("📊 Generating 3 hours of inspection events...")
    
    # Generate data for the last 3 hours
    current_time = time.time()
    start_time = current_time - (3 * 60 * 60)  # 3 hours ago
    
    try:
        product_counter = 1
        
        # Generate data in 15-minute intervals
        for minutes_offset in range(0, 180, 15):  # Every 15 minutes for 3 hours
            batch_timestamp = int((start_time + minutes_offset * 60) * 1000)
            batch_time_str = datetime.fromtimestamp(batch_timestamp / 1000).strftime("%H:%M")
            
            print(f"📅 Generating batch for {batch_time_str} (15 products)")
            
            # Generate 15 products per 15-minute interval
            for i in range(15):
                # Add some random variance within the 15-minute window
                variance = random.randint(0, 900)  # 0-15 minutes in seconds
                event_timestamp = batch_timestamp + (variance * 1000)
                
                event = generate_inspection_event(
                    f'prod_{product_counter:04d}', 
                    base_timestamp=event_timestamp
                )
                
                producer.produce(
                    topic,
                    key=event['product_id'].encode(),
                    value=json.dumps(event).encode(),
                    callback=delivery_report
                )
                
                product_counter += 1
                time.sleep(0.02)  # Small delay to avoid overwhelming
            
            producer.poll(0)
        
        print("⏳ Waiting for final message delivery...")
        producer.flush(30)
        
        print("✅ Historical inspection data sent to Redpanda!")
        print(f"📈 Generated {product_counter-1} inspection events spanning 3 hours")
        print("💡 Now run true_iceberg_writer.py to commit to true PyIceberg tables")
        
    except KeyboardInterrupt:
        print("🛑 Interrupted by user")
    except Exception as e:
        print(f"❌ Producer error: {e}")
    finally:
        producer.flush()

def produce_realtime_data():
    """Produce real-time inspection data with current timestamps"""
    config = {
        'bootstrap.servers': 'localhost:19092',
        'acks': 'all',
        'retries': 5,
        'retry.backoff.ms': 100,
        'request.timeout.ms': 30000,
        'delivery.timeout.ms': 60000
    }
    
    producer = Producer(config)
    topic = 'inspections'
    
    print("🚀 Producing real-time inspection data for PyIceberg...")
    print("📊 Simulating active production line...")
    
    try:
        for i in range(1, 201):  # Produce 200 inspection events
            event = generate_inspection_event(f'prod_{i:04d}')
            
            producer.produce(
                topic,
                key=event['product_id'].encode(),
                value=json.dumps(event).encode(),
                callback=delivery_report
            )
            
            # Progress indicator
            if i % 25 == 0:
                print(f"📈 Produced {i} inspection events...")
                time.sleep(1.0)  # Brief pause every 25 messages
            else:
                time.sleep(0.1)  # Simulate real production timing
            
            producer.poll(0)
        
        print("⏳ Waiting for final message delivery...")
        producer.flush(30)
        
        print("✅ Real-time inspection data sent to Redpanda!")
        print("💡 Now run true_iceberg_writer.py to commit to PyIceberg tables")
        
    except KeyboardInterrupt:
        print("🛑 Interrupted by user")
    except Exception as e:
        print(f"❌ Producer error: {e}")
    finally:
        producer.flush()

def main():
    """Interactive producer with data type selection"""
    print("🤔 Choose inspection data generation mode:")
    print("1. Real-time data (current timestamps)")
    print("2. Historical data (3-hour span for PyIceberg time-travel)")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "2":
        produce_historical_data()
    else:
        produce_realtime_data()

if __name__ == "__main__":
    main()