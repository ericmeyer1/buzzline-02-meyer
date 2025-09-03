# buzzline-02-eric

This project demonstrates **real-time streaming analytics** using Apache Kafka.  
I created a **custom Kafka producer** and **custom Kafka consumer** to simulate streaming sensor data from an extrusion processing system.

---

## Custom Kafka Producer: `kafka_producer_eric.py`

This producer generates fun day to day messages about stocks, the weather, and the system health.

### Features

- Generates fun daily messages
- Logs each generated message
- Sends messages to the Kafka topic defined in `.env` (`KAFKA_TOPIC`)

### Run Producer (Windows)

```powershell```
.venv\Scripts\activate
py -m producers.kafka_producer_eric

---

## Custom Kafka Consumer: `kafka_consumer_eric.py`

This consumer reads fake extrusion temperatures and sends messages baed on the temps beeing to hot or low.

### Features

- Generates extrusion sensor messages
- Logs each response message
  
### Run Consumer (Windows)

```powershell
.venv\Scripts\activate
py -m consumers.kafka_consumer_eric
