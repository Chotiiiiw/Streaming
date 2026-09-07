import argparse
import json
import os
import time

from kafka import KafkaProducer
from transaction_generator import StatisticalTransactionGenerator


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate transactions and publish them to Kafka."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of transactions to generate.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds between transactions.",
    )
    parser.add_argument(
        "--users",
        type=int,
        default=5,
        help="Number of unique users to generate.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional random seed for reproducible transactions.",
    )
    parser.add_argument(
        "--mean-gap-seconds",
        type=float,
        default=90.0,
        help="Average simulated event-time gap between normal orders.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.count <= 0:
        raise ValueError("--count must be greater than zero")

    if args.interval < 0:
        raise ValueError("--interval cannot be negative")

    if args.users <= 0:
        raise ValueError("--users must be greater than zero")

    if args.mean_gap_seconds <= 0:
        raise ValueError("--mean-gap-seconds must be greater than zero")

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    generator = StatisticalTransactionGenerator(
        user_count=args.users,
        seed=args.seed,
        mean_gap_seconds=args.mean_gap_seconds,
    )
    transactions = generator.generate(args.count)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=5,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    try:
        for transaction in transactions:
            message_key = transaction["user_id"]
            metadata = producer.send(
                topic="transactions_raw",
                key=message_key,
                value=transaction,
            ).get(timeout=10)

            print(
                f"acknowledged: "
                f"topic={metadata.topic}, "
                f"partition={metadata.partition}, "
                f"offset={metadata.offset}, "
                f"transaction={transaction}, "
                f"key={message_key}"
            )

            time.sleep(args.interval)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
