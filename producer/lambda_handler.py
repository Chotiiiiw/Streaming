import json
import logging
import os
import socket
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone

from transaction_generator import StatisticalTransactionGenerator


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def lambda_handler(event, context):
    event = event or {}
    initialize_only = bool(event.get("initialize_only", False))
    inspect_only = bool(event.get("inspect_only", False))

    kafka_config = _kafka_config()
    topics = _topics()
    created_topics = _ensure_topics(kafka_config, topics)
    LOGGER.info(
        "Successfully connected to Kafka; topics ready: %s; created=%s",
        ",".join(topics.values()),
        ",".join(created_topics) if created_topics else "none",
    )

    if initialize_only:
        return {
            "statusCode": 200,
            "initialized": True,
            "topics": list(topics.values()),
            "created_topics": created_topics,
            "published": 0,
        }

    if inspect_only:
        sample_limit = _positive_int(event, "sample_limit", 3)
        samples = _inspect_topics(
            kafka_config,
            [
                topics["clean_transactions"],
                topics["fraud_alerts"],
                topics["transactions_dlq"],
            ],
            sample_limit,
        )
        LOGGER.info(
            "Kafka output inspection complete: %s",
            ",".join(
                f"{topic}={len(records)}"
                for topic, records in samples.items()
            ),
        )
        return {
            "statusCode": 200,
            "initialized": True,
            "topics": list(topics.values()),
            "samples": samples,
        }

    scenario = event.get("scenario")
    scenario_id = None

    if scenario == "evidence":
        scenario_id, transactions = _evidence_transactions()
    elif scenario is not None:
        raise ValueError(f"Unknown scenario: {scenario}")
    elif "transactions" in event:
        transactions = _explicit_transactions(event["transactions"])
    else:
        count = _positive_int(event, "count", 10)
        users = _positive_int(event, "users", 5)
        mean_gap_seconds = _positive_float(
            event,
            "mean_gap_seconds",
            90.0,
        )
        seed = event.get("seed")

        if seed is not None:
            seed = int(seed)

        generator = StatisticalTransactionGenerator(
            user_count=users,
            seed=seed,
            mean_gap_seconds=mean_gap_seconds,
        )
        transactions = generator.generate(count)
    published = _publish_transactions(
        kafka_config,
        topics["transactions_raw"],
        transactions,
    )
    LOGGER.info(
        "Published %d transactions to %s",
        published,
        topics["transactions_raw"],
    )

    return {
        "statusCode": 200,
        "initialized": True,
        "topics": list(topics.values()),
        "created_topics": created_topics,
        "published": published,
        "scenario": scenario,
        "scenario_id": scenario_id,
    }


def _kafka_config():
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    from kafka.net.sasl.oauth import AbstractTokenProvider

    region = _required_environment("AWS_REGION")

    class LambdaMskTokenProvider(AbstractTokenProvider):
        def token(self):
            token, _ = MSKAuthTokenProvider.generate_auth_token(region)
            return token

    return {
        "bootstrap_servers": _required_environment(
            "KAFKA_BOOTSTRAP_SERVERS"
        ).split(","),
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "OAUTHBEARER",
        "sasl_oauth_token_provider": LambdaMskTokenProvider(),
        "client_id": f"lambda-producer-{socket.gethostname()}",
        "request_timeout_ms": 30000,
    }


def _topics():
    return {
        "transactions_raw": _required_environment(
            "TRANSACTIONS_RAW_TOPIC"
        ),
        "clean_transactions": _required_environment(
            "CLEAN_TRANSACTIONS_TOPIC"
        ),
        "fraud_alerts": _required_environment("FRAUD_ALERTS_TOPIC"),
        "transactions_dlq": _required_environment(
            "TRANSACTIONS_DLQ_TOPIC"
        ),
    }


def _ensure_topics(kafka_config, topics):
    from kafka.admin import KafkaAdminClient, NewTopic

    admin_client = KafkaAdminClient(**kafka_config)

    try:
        existing_topics = set(admin_client.list_topics())
        topic_partitions = {
            topics["transactions_raw"]: 3,
            topics["clean_transactions"]: 1,
            topics["fraud_alerts"]: 1,
            topics["transactions_dlq"]: 1,
        }
        missing_topics = [
            NewTopic(
                name=topic_name,
                num_partitions=partition_count,
                replication_factor=-1,
            )
            for topic_name, partition_count in topic_partitions.items()
            if topic_name not in existing_topics
        ]

        if missing_topics:
            admin_client.create_topics(
                new_topics=missing_topics,
                validate_only=False,
            )

        return [topic.name for topic in missing_topics]
    finally:
        admin_client.close()


def _publish_transactions(kafka_config, topic, transactions):
    from kafka import KafkaProducer

    producer = KafkaProducer(
        **kafka_config,
        acks="all",
        retries=5,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    try:
        acknowledgements = [
            producer.send(
                topic=topic,
                key=transaction["user_id"],
                value=transaction,
            )
            for transaction in transactions
        ]

        for acknowledgement in acknowledgements:
            acknowledgement.get(timeout=30)

        producer.flush()
        return len(acknowledgements)
    finally:
        producer.close()


def _inspect_topics(kafka_config, topic_names, sample_limit):
    from kafka import KafkaConsumer

    samples = {}

    for topic_name in topic_names:
        consumer = KafkaConsumer(
            topic_name,
            **{
                **kafka_config,
                "client_id": f"lambda-inspector-{socket.gethostname()}",
                "group_id": f"lambda-inspector-{uuid.uuid4().hex}",
                "auto_offset_reset": "earliest",
                "enable_auto_commit": False,
                "consumer_timeout_ms": 4000,
                "value_deserializer": lambda value: json.loads(
                    value.decode("utf-8")
                ),
            },
        )

        recent_records = deque(maxlen=sample_limit)
        try:
            for record in consumer:
                recent_records.append(record.value)
        finally:
            consumer.close()

        samples[topic_name] = list(recent_records)

    return samples


def _explicit_transactions(transactions):
    if not isinstance(transactions, list) or not transactions:
        raise ValueError("transactions must be a non-empty list")
    if len(transactions) > 500:
        raise ValueError("transactions cannot contain more than 500 records")

    required_fields = {
        "transaction_id",
        "user_id",
        "amount",
        "country",
        "event_time",
    }
    for transaction in transactions:
        if not isinstance(transaction, dict):
            raise ValueError("each transaction must be an object")
        missing_fields = required_fields.difference(transaction)
        if missing_fields:
            raise ValueError(
                "transaction is missing fields: "
                + ", ".join(sorted(missing_fields))
            )
        if not str(transaction["user_id"]).strip():
            raise ValueError("transaction user_id must not be blank")

    return transactions


def _evidence_transactions():
    scenario_id = uuid.uuid4().hex[:8]
    user_id = f"evidence_user_{scenario_id}"
    end_time = datetime.now(timezone.utc)

    transactions = []
    for index, minutes_before in enumerate((20, 16, 12, 8, 4), start=1):
        transactions.append(
            _scenario_transaction(
                scenario_id,
                f"clean_{index}",
                user_id,
                100.0,
                "TH",
                end_time - timedelta(minutes=minutes_before),
            )
        )

    fraud_inputs = (
        ("fraud_1", 1000.0, "SG", 50),
        ("fraud_2", 1200.0, "US", 40),
        ("fraud_3", 1500.0, "JP", 30),
        ("fraud_4", 1800.0, "SG", 20),
        ("fraud_5", 2200.0, "US", 10),
    )
    for transaction_id, amount, country, seconds_before in fraud_inputs:
        transactions.append(
            _scenario_transaction(
                scenario_id,
                transaction_id,
                user_id,
                amount,
                country,
                end_time - timedelta(seconds=seconds_before),
            )
        )

    transactions.append(
        _scenario_transaction(
            scenario_id,
            "invalid_1",
            user_id,
            -10.0,
            "TH",
            end_time,
        )
    )
    return scenario_id, transactions


def _scenario_transaction(
    scenario_id,
    transaction_id,
    user_id,
    amount,
    country,
    event_time,
):
    return {
        "transaction_id": f"evidence_{scenario_id}_{transaction_id}",
        "user_id": user_id,
        "amount": amount,
        "country": country,
        "event_time": event_time.isoformat(timespec="milliseconds").replace(
            "+00:00",
            "Z",
        ),
    }


def _required_environment(name):
    value = os.getenv(name)

    if value is None or not value.strip():
        raise ValueError(f"Missing required environment variable: {name}")

    return value.strip()


def _positive_int(event, name, default):
    value = int(event.get(name, default))

    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")

    return value


def _positive_float(event, name, default):
    value = float(event.get(name, default))

    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")

    return value
