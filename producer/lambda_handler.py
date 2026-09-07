import json
import os
import socket

from transaction_generator import StatisticalTransactionGenerator


def lambda_handler(event, context):
    event = event or {}
    initialize_only = bool(event.get("initialize_only", False))

    kafka_config = _kafka_config()
    topics = _topics()
    created_topics = _ensure_topics(kafka_config, topics)

    if initialize_only:
        return {
            "statusCode": 200,
            "initialized": True,
            "created_topics": created_topics,
            "published": 0,
        }

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

    return {
        "statusCode": 200,
        "initialized": True,
        "created_topics": created_topics,
        "published": published,
    }


def _kafka_config():
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    from kafka.sasl.oauth import AbstractTokenProvider

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
        "api_version_auto_timeout_ms": 10000,
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
