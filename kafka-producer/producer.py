import os
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = 'user-events'

# Sample data for generating realistic events
USERS = [f'user_{i}' for i in range(1, 101)]
EVENTS = ['page_view', 'click', 'purchase', 'signup', 'logout']
PAGES = ['/home', '/products', '/cart', '/checkout', '/profile', '/search']
PRODUCTS = [f'product_{i}' for i in range(1, 51)]

def create_producer():
    """Create Kafka producer with retry logic"""
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(2, 8, 0)
            )
            print(f" Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except KafkaError as e:
            retry_count += 1
            print(f"  Kafka connection attempt {retry_count}/{max_retries} failed: {e}")
            time.sleep(5)
    
    raise Exception("Failed to connect to Kafka after maximum retries")

def generate_event():
    """Generate a realistic user event"""
    event_type = random.choice(EVENTS)
    
    event = {
        'event_id': f'evt_{int(time.time() * 1000)}_{random.randint(1000, 9999)}',
        'user_id': random.choice(USERS),
        'event_type': event_type,
        'timestamp': datetime.utcnow().isoformat(),
        'session_id': f'session_{random.randint(10000, 99999)}',
    }
    
    # Add event-specific data
    if event_type == 'page_view':
        event['page'] = random.choice(PAGES)
        event['duration_seconds'] = random.randint(5, 300)
    
    elif event_type == 'click':
        event['element'] = random.choice(['button', 'link', 'image'])
        event['page'] = random.choice(PAGES)
    
    elif event_type == 'purchase':
        event['product_id'] = random.choice(PRODUCTS)
        event['amount'] = round(random.uniform(10.0, 500.0), 2)
        event['currency'] = 'USD'
    
    elif event_type == 'signup':
        event['source'] = random.choice(['google', 'facebook', 'direct', 'email'])
        event['country'] = random.choice(['US', 'UK', 'FR', 'DE', 'CA'])
    
    return event

def main():
    print("🚀 Starting Kafka Producer...")
    
    # Wait for Kafka to be ready
    time.sleep(10)
    
    producer = create_producer()
    
    events_sent = 0
    
    try:
        print(f"📊 Producing events to topic '{TOPIC_NAME}'...")
        
        while True:
            event = generate_event()
            
            # Send to Kafka
            future = producer.send(TOPIC_NAME, value=event)
            
            try:
                # Wait for confirmation
                record_metadata = future.get(timeout=10)
                events_sent += 1
                
                if events_sent % 10 == 0:
                    print(f" Sent {events_sent} events | Last: {event['event_type']} from {event['user_id']}")
            
            except KafkaError as e:
                print(f" Failed to send event: {e}")
            
            # Random delay between events (0.5 to 2 seconds)
            time.sleep(random.uniform(0.5, 2.0))
    
    except KeyboardInterrupt:
        print(f"\n Stopping producer. Total events sent: {events_sent}")
    
    finally:
        producer.close()

if __name__ == '__main__':
    main()