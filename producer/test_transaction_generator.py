import unittest
from datetime import datetime, timezone

from transaction_generator import StatisticalTransactionGenerator


class StatisticalTransactionGeneratorTest(unittest.TestCase):
    def test_preserves_transaction_schema(self):
        generator = StatisticalTransactionGenerator(user_count=3, seed=42)

        transactions = generator.generate(
            20,
            end_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        expected_fields = {
            "transaction_id",
            "user_id",
            "amount",
            "country",
            "event_time",
        }

        for transaction in transactions:
            self.assertEqual(expected_fields, set(transaction))
            self.assertGreater(transaction["amount"], 0)

    def test_seed_produces_repeatable_transactions(self):
        end_time = datetime(2026, 1, 1, tzinfo=timezone.utc)

        first = StatisticalTransactionGenerator(5, seed=7).generate(
            50,
            end_time=end_time,
        )
        second = StatisticalTransactionGenerator(5, seed=7).generate(
            50,
            end_time=end_time,
        )

        self.assertEqual(first, second)

    def test_event_times_are_chronological(self):
        generator = StatisticalTransactionGenerator(user_count=5, seed=9)
        transactions = generator.generate(
            100,
            end_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        event_times = [item["event_time"] for item in transactions]

        self.assertEqual(event_times, sorted(event_times))


if __name__ == "__main__":
    unittest.main()
