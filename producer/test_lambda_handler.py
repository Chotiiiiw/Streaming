import os
import unittest
from unittest.mock import patch

import lambda_handler


TOPIC_ENVIRONMENT = {
    "TRANSACTIONS_RAW_TOPIC": "transactions_raw",
    "CLEAN_TRANSACTIONS_TOPIC": "clean_transactions",
    "FRAUD_ALERTS_TOPIC": "fraud_alerts",
    "TRANSACTIONS_DLQ_TOPIC": "transactions_dlq",
}


class LambdaHandlerTest(unittest.TestCase):
    @patch.dict(os.environ, TOPIC_ENVIRONMENT, clear=True)
    @patch.object(lambda_handler, "_kafka_config", return_value={})
    @patch.object(
        lambda_handler,
        "_ensure_topics",
        return_value=["transactions_raw"],
    )
    @patch.object(lambda_handler, "_publish_transactions")
    def test_initialize_only_does_not_publish(
        self,
        publish_transactions,
        ensure_topics,
        kafka_config,
    ):
        response = lambda_handler.lambda_handler(
            {"initialize_only": True},
            None,
        )

        self.assertEqual(0, response["published"])
        self.assertEqual(["transactions_raw"], response["created_topics"])
        publish_transactions.assert_not_called()

    @patch.dict(os.environ, TOPIC_ENVIRONMENT, clear=True)
    @patch.object(lambda_handler, "_kafka_config", return_value={})
    @patch.object(lambda_handler, "_ensure_topics", return_value=[])
    @patch.object(lambda_handler, "_publish_transactions", return_value=12)
    def test_publish_mode_generates_requested_count(
        self,
        publish_transactions,
        ensure_topics,
        kafka_config,
    ):
        response = lambda_handler.lambda_handler(
            {
                "count": 12,
                "users": 3,
                "seed": 42,
                "mean_gap_seconds": 60,
            },
            None,
        )

        self.assertEqual(12, response["published"])
        generated = publish_transactions.call_args.args[2]
        self.assertEqual(12, len(generated))
        self.assertEqual(
            {
                "transaction_id",
                "user_id",
                "amount",
                "country",
                "event_time",
            },
            set(generated[0]),
        )

    @patch.dict(os.environ, TOPIC_ENVIRONMENT, clear=True)
    @patch.object(lambda_handler, "_kafka_config", return_value={})
    @patch.object(lambda_handler, "_ensure_topics", return_value=[])
    @patch.object(lambda_handler, "_publish_transactions", return_value=1)
    def test_publish_mode_accepts_explicit_transactions(
        self,
        publish_transactions,
        ensure_topics,
        kafka_config,
    ):
        transaction = {
            "transaction_id": "tx_explicit",
            "user_id": "user_explicit",
            "amount": -1,
            "country": "TH",
            "event_time": "2026-09-07T00:00:00.000Z",
        }

        response = lambda_handler.lambda_handler(
            {"transactions": [transaction]},
            None,
        )

        self.assertEqual(1, response["published"])
        self.assertEqual(
            [transaction],
            publish_transactions.call_args.args[2],
        )

    @patch.dict(os.environ, TOPIC_ENVIRONMENT, clear=True)
    @patch.object(lambda_handler, "_kafka_config", return_value={})
    @patch.object(lambda_handler, "_ensure_topics", return_value=[])
    @patch.object(lambda_handler, "_inspect_topics")
    def test_inspect_mode_returns_output_topic_samples(
        self,
        inspect_topics,
        ensure_topics,
        kafka_config,
    ):
        expected = {
            "clean_transactions": [{"risk_level": "LOW"}],
            "fraud_alerts": [{"risk_level": "HIGH"}],
            "transactions_dlq": [{"error_reason": "invalid"}],
        }
        inspect_topics.return_value = expected

        response = lambda_handler.lambda_handler(
            {"inspect_only": True, "sample_limit": 2},
            None,
        )

        self.assertEqual(expected, response["samples"])
        self.assertEqual(2, inspect_topics.call_args.args[2])

    def test_evidence_scenario_contains_each_route_input(self):
        scenario_id, transactions = lambda_handler._evidence_transactions()

        self.assertEqual(8, len(scenario_id))
        self.assertEqual(11, len(transactions))
        self.assertEqual(5, len([
            transaction
            for transaction in transactions
            if "_clean_" in transaction["transaction_id"]
        ]))
        self.assertEqual(-10.0, transactions[-1]["amount"])

    def test_rejects_non_positive_count(self):
        with self.assertRaisesRegex(ValueError, "count"):
            lambda_handler._positive_int({"count": 0}, "count", 10)


if __name__ == "__main__":
    unittest.main()
