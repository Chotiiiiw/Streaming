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

    def test_rejects_non_positive_count(self):
        with self.assertRaisesRegex(ValueError, "count"):
            lambda_handler._positive_int({"count": 0}, "count", 10)


if __name__ == "__main__":
    unittest.main()
