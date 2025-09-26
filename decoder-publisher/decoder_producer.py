import socket
import struct
import json
import time
from confluent_kafka import Producer

# Kafka configuration
KAFKA_BROKER = "kafka:9092"
TOPIC = "market.ticks"


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


def wait_for_kafka():
    """Wait for Kafka to be available"""
    max_retries = 30
    retry_count = 0

    while retry_count < max_retries:
        try:
            producer = Producer({"bootstrap.servers": KAFKA_BROKER})
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


def main():
    # Wait for Kafka to be ready
    p = wait_for_kafka()

    # Set up multicast socket
    MCAST_GRP = "239.255.0.1"
    MCAST_PORT = 30001

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", MCAST_PORT))

    mreq = struct.pack("4sl", socket.inet_aton(MCAST_GRP), socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    print(f"Listening on {MCAST_GRP} {MCAST_PORT} -> producing to {TOPIC}")

    while True:
        try:
            data, addr = sock.recvfrom(1024)
            msg = data.decode("utf-8")

            # Parse and forward to Kafka
            try:
                parsed_msg = json.loads(msg)
                p.produce(
                    TOPIC,
                    json.dumps(parsed_msg).encode("utf-8"),
                    callback=delivery_report,
                )
                p.poll(0)  # Process delivery reports

            except json.JSONDecodeError:
                print(f"Invalid JSON received: {msg}")
            except BufferError:
                print("Producer queue full, waiting...")
                p.flush()  # Wait for all messages to be delivered
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
    p.flush()
    sock.close()


if __name__ == "__main__":
    main()
