import json, time, random
from faker import Faker
from confluent_kafka import Producer

fake = Faker()
kafka_config = {'bootstrap.servers': 'kafka:29092', 'client.id': 'ecommerce_simulator'}
producer = Producer(kafka_config)
topic_name = 'ecommerce_events'

def delivery_report(err, msg):
    if err is not None: print(f"Message delivery failed: {err}")

def generate_ecommerce_event():
    actions = ['view', 'add_to_cart', 'purchase']
    return {
        'user_id': fake.uuid4(),
        'product_id': fake.ean8(),
        'timestamp': time.time(),
        'action_type': random.choices(actions, weights=[0.7, 0.2, 0.1])[0],
        'price': round(random.uniform(10.0, 500.0), 2)
    }

print("Starting E-commerce Data Simulator...")
try:
    while True:
        event = generate_ecommerce_event()
        producer.produce(topic=topic_name, key=event['user_id'], value=json.dumps(event).encode('utf-8'), callback=delivery_report)
        producer.poll(0)
        time.sleep(0.5)
except KeyboardInterrupt:
    print("\nSimulator stopped.")
finally:
    producer.flush()