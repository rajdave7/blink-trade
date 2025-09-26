import socket
import struct
import json
import time
import os
from collections import defaultdict, deque
from confluent_kafka import Producer
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Kafka configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = "market.ticks"

# Prometheus metrics
messages_received_total = Counter(
    "market_data_messages_received_total",
    "Total number of market data messages received",
    ["symbol", "msg_type"],
)

messages_produced_total = Counter(
    "market_data_messages_produced_total",
    "Total number of messages produced to Kafka",
    ["symbol", "msg_type"],
)

message_processing_duration = Histogram(
    "market_data_processing_duration_seconds",
    "Time spent processing market data messages",
)

kafka_queue_depth = Gauge(
    "kafka_producer_queue_depth", "Current depth of Kafka producer queue"
)

data_quality_errors = Counter(
    "market_data_quality_errors_total",
    "Total number of data quality errors",
    ["error_type"],
)

sequence_gaps = Counter(
    "market_data_sequence_gaps_total", "Total number of sequence gaps detected"
)

latency_histogram = Histogram(
    "market_data_end_to_end_latency_seconds",
    "End-to-end latency from generation to processing",
)


class DataQualityValidator:
    def __init__(self):
        self.last_sequences = {}  # symbol -> last sequence number
        self.price_history = defaultdict(
            lambda: deque(maxlen=100)
        )  # symbol -> recent prices

    def validate_message(self, msg_data):
        """Validate market data quality and detect anomalies"""
        errors = []

        try:
            # Check required fields
            required_fields = ["symbol", "price", "timestamp", "sequence"]
            for field in required_fields:
                if field not in msg_data:
                    errors.append(f"missing_field_{field}")

            if not errors:  # Only continue if basic fields are present
                symbol = msg_data["symbol"]
                price = float(msg_data["price"])
                timestamp = int(msg_data["timestamp"])
                sequence = int(msg_data["sequence"])

                # Check price reasonableness
                if price <= 0:
                    errors.append("negative_price")
                elif price > 10000:  # Arbitrary high price threshold
                    errors.append("suspicious_high_price")

                # Check sequence numbers for gaps
                if symbol in self.last_sequences:
                    expected_seq = self.last_sequences[symbol] + 1
                    if sequence != expected_seq:
                        if sequence > expected_seq:
                            gaps = sequence - expected_seq
                            sequence_gaps.inc(gaps)
                            errors.append(f"sequence_gap_{gaps}")
                        else:
                            errors.append("sequence_regression")

                self.last_sequences[symbol] = sequence

                # Check for price spikes (more than 5% move)
                price_hist = self.price_history[symbol]
                if len(price_hist) > 0:
                    last_price = price_hist[-1]
                    change_pct = abs(price - last_price) / last_price
                    if change_pct > 0.05:  # 5% threshold
                        errors.append("price_spike")

                price_hist.append(price)

                # Check timestamp reasonableness (not too old or in future)
                current_time = time.time() * 1000000  # microseconds
                if abs(timestamp - current_time) > 60 * 1000000:  # 60 seconds
                    errors.append("timestamp_skew")

        except (ValueError, TypeError) as e:
            errors.append(f"parse_error_{type(e).__name__}")

        # Record all errors
        for error in errors:
            data_quality_errors.labels(error_type=error).inc()

        return errors


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        # Extract symbol from message for metrics
        try:
            data = json.loads(msg.value().decode("utf-8"))
            symbol = data.get("symbol", "unknown")
            msg_type = data.get("msgType", "unknown")
            messages_produced_total.labels(symbol=symbol, msg_type=msg_type).inc()
        except:
            messages_produced_total.labels(symbol="unknown", msg_type="unknown").inc()


def wait_for_kafka():
    """Wait for Kafka to be available"""
    max_retries = 30
    retry_count = 0

    while retry_count < max_retries:
        try:
            producer = Producer(
                {
                    "bootstrap.servers": KAFKA_BROKER,
                    "queue.buffering.max.messages": 100000,
                    "queue.buffering.max.kbytes": 1048576,  # 1GB
                    "batch.num.messages": 1000,
                    "statistics.interval.ms": 5000,  # Enable statistics
                }
            )
            # Try to get metadata to test connection
            metadata = producer.list_topics(timeout=5.0)
            print("Connected to Kafka successfully!")
            return producer
        except Exception as e:
            retry_count += 1
            print(
                f"Waiting for Kafka... Attempt {retry_count}/{max_retries}. Error: {e}"
            )
            time.sleep(2)

    raise Exception("Failed to connect to Kafka after maximum retries")


def stats_cb(stats_json):
    """Callback for Kafka producer statistics"""
    try:
        stats = json.loads(stats_json)
        # Update queue depth metric
        queue_depth = stats.get("msg_cnt", 0)
        kafka_queue_depth.set(queue_depth)
    except:
        pass


def main():
    # Start Prometheus metrics server
    start_http_server(8000)
    print("Prometheus metrics server started on port 8000")

    # Wait for Kafka to be ready
    p = wait_for_kafka()

    # Set statistics callback
    # p.set_config_callback("stats_cb", stats_cb)

    producer_config = {
        'bootstrap.servers': 'your-kafka-servers',
        'statistics.interval.ms': 10000,  # Enable stats every 10 seconds
        'stats_cb': stats_cb,  # Set callback in config
        # ... other config options
    }

    p = Producer(producer_config)
    # Initialize data quality validator
    validator = DataQualityValidator()

    # Set up multicast socket
    MCAST_GRP = "239.255.0.1"
    MCAST_PORT = 30001

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", MCAST_PORT))

    mreq = struct.pack("4sl", socket.inet_aton(MCAST_GRP), socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    print(f"Listening on {MCAST_GRP} {MCAST_PORT} -> producing to {TOPIC}")
    print("Data quality validation enabled")

    message_count = 0
    start_time = time.time()

    while True:
        try:
            with message_processing_duration.time():
                data, addr = sock.recvfrom(1024)
                msg = data.decode("utf-8")

                # Parse and validate message
                try:
                    parsed_msg = json.loads(msg)
                    symbol = parsed_msg.get("symbol", "unknown")
                    msg_type = parsed_msg.get("msgType", "unknown")

                    # Record message received
                    messages_received_total.labels(
                        symbol=symbol, msg_type=msg_type
                    ).inc()

                    # Calculate latency if timestamp is present
                    if "timestamp" in parsed_msg:
                        try:
                            msg_timestamp = (
                                int(parsed_msg["timestamp"]) / 1000000
                            )  # Convert to seconds
                            current_time = time.time()
                            latency = current_time - msg_timestamp
                            if (
                                latency > 0 and latency < 60
                            ):  # Reasonable latency bounds
                                latency_histogram.observe(latency)
                        except:
                            pass

                    # Validate data quality
                    quality_errors = validator.validate_message(parsed_msg)

                    # Produce to Kafka
                    p.produce(
                        TOPIC,
                        json.dumps(parsed_msg).encode("utf-8"),
                        callback=delivery_report,
                    )
                    p.poll(0)  # Process delivery reports

                    message_count += 1

                    # Print stats every 5000 messages
                    if message_count % 5000 == 0:
                        elapsed = time.time() - start_time
                        rate = message_count / elapsed
                        error_count = sum(data_quality_errors._value._value.values())

                        print(
                            f"Processed {message_count} messages, "
                            f"Rate: {rate:.1f} msg/s, "
                            f"Quality errors: {error_count}, "
                            f"Last symbol: {symbol}"
                        )

                except json.JSONDecodeError:
                    data_quality_errors.labels(error_type="json_decode_error").inc()
                    print(f"Invalid JSON received: {msg}")
                except BufferError:
                    print("Producer queue full, flushing...")
                    p.flush()
                    # Retry the message
                    p.produce(
                        TOPIC,
                        json.dumps(parsed_msg).encode("utf-8"),
                        callback=delivery_report,
                    )
                    p.poll(0)

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error processing message: {e}")

    # Clean shutdown
    print("Shutting down...")
    p.flush()
    sock.close()


if __name__ == "__main__":
    main()
