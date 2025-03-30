import time
import random
import json
import logging
from datetime import datetime
from kafka import KafkaProducer
from prometheus_client import Counter, Gauge, start_http_server

start_http_server(8000)

traffic_counter = Counter(
    "traffic_events_total", "Total number of traffic events", ["sensor_id"]
)

vehicle_count_gauge = Gauge(
    "vehicle_count", "The number of vehicles at a sensor", ["sensor_id"]
)

average_speed_gauge = Gauge(
    "average_speed", "The average speed of vehicles at a sensor", ["sensor_id"]
)

congestion_level_gauge = Gauge(
    "congestion_level", "Congestion level at a sensor", ["sensor_id", "level"]
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

TOPIC = "traffic_data"

try:
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    logging.info("Kafka producer initialized successfully.")
except Exception as e:
    logging.error(f"Error initializing Kafka producer: {e}")
    exit()

SENSORS = ["S123", "S124", "S125", "S126", "S127"]

def get_congestion_level(vehicle_count):
    """Determines congestion level based on vehicle count."""
    if vehicle_count < 10:
        return "LOW"
    elif 10 <= vehicle_count < 30:
        return "MEDIUM"
    else:
        return "HIGH"

def generate_sensor_data():
    sensor_id = random.choice(SENSORS)
    timestamp = datetime.now().isoformat()
    vehicle_count = random.randint(5, 50)
    average_speed = round(random.uniform(20.0, 80.0), 1)
    congestion_level = get_congestion_level(vehicle_count)

    return {
        "sensor_id": sensor_id,
        "timestamp": timestamp,
        "vehicle_count": vehicle_count,
        "average_speed": average_speed,
        "congestion_level": congestion_level,
    }

try:
    while True:
        data = generate_sensor_data()
        try:
            producer.send(TOPIC, value=data)
            logging.info(f"Sent: {data}")
            
            traffic_counter.labels(sensor_id=data["sensor_id"]).inc()
            
            vehicle_count_gauge.labels(sensor_id=data["sensor_id"]).set(data["vehicle_count"])
            average_speed_gauge.labels(sensor_id=data["sensor_id"]).set(data["average_speed"])
            congestion_level_gauge.labels(sensor_id=data["sensor_id"], level=data["congestion_level"]).set(1)  # Assuming 1 means 'active'

        except Exception as e:
            logging.error(f"Error sending data to Kafka: {e}")
        
        time.sleep(random.uniform(0.5, 2.0))

except KeyboardInterrupt:
    logging.info("Stopping producer...")
finally:
    try:
        producer.close()
        logging.info("Kafka producer closed.")
    except Exception as e:
        logging.error(f"Error closing Kafka producer: {e}")

