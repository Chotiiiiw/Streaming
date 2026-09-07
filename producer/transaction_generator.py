import math
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


COUNTRIES = ("TH", "SG", "US", "JP", "GB", "AU")
COUNTRY_WEIGHTS = (55, 10, 12, 8, 7, 8)


@dataclass(frozen=True)
class UserProfile:
    user_id: str
    home_country: str
    typical_amount: float
    amount_variability: float
    activity_weight: float


class StatisticalTransactionGenerator:
    def __init__(
        self,
        user_count,
        seed=None,
        mean_gap_seconds=90.0,
    ):
        if user_count <= 0:
            raise ValueError("user_count must be greater than zero")

        if mean_gap_seconds <= 0:
            raise ValueError("mean_gap_seconds must be greater than zero")

        self.random = random.Random(seed)
        self.mean_gap_seconds = mean_gap_seconds
        self.profiles = self._create_profiles(user_count)

    def generate(self, count, end_time=None):
        if count <= 0:
            raise ValueError("count must be greater than zero")

        if end_time is None:
            end_time = datetime.now(timezone.utc)
        elif end_time.tzinfo is None:
            raise ValueError("end_time must include timezone information")

        generated = []
        simulated_seconds = 0.0
        burst_profile = None
        burst_remaining = 0

        for _ in range(count):
            if burst_remaining > 0:
                profile = burst_profile
                simulated_seconds += self.random.uniform(3.0, 25.0)
                burst_remaining -= 1
            else:
                profile = self.random.choices(
                    self.profiles,
                    weights=[item.activity_weight for item in self.profiles],
                    k=1,
                )[0]
                simulated_seconds += self.random.expovariate(
                    1.0 / self.mean_gap_seconds
                )

                # A small number of sessions become short transaction bursts.
                if self.random.random() < 0.04:
                    burst_profile = profile
                    burst_remaining = self.random.randint(2, 7)

            generated.append(
                {
                    "transaction_id": self._transaction_id(),
                    "user_id": profile.user_id,
                    "amount": self._amount(profile),
                    "country": self._country(profile),
                    "simulated_seconds": simulated_seconds,
                }
            )

        final_offset = generated[-1]["simulated_seconds"]

        for transaction in generated:
            seconds_before_end = (
                final_offset - transaction.pop("simulated_seconds")
            )
            event_time = end_time - timedelta(seconds=seconds_before_end)
            transaction["event_time"] = (
                event_time
                .astimezone(timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z")
            )

        return generated

    def _create_profiles(self, user_count):
        profiles = []

        for index in range(user_count):
            typical_amount = self.random.lognormvariate(
                math.log(600.0),
                0.8,
            )

            profiles.append(
                UserProfile(
                    user_id=f"user_{index}",
                    home_country=self.random.choices(
                        COUNTRIES,
                        weights=COUNTRY_WEIGHTS,
                        k=1,
                    )[0],
                    typical_amount=max(50.0, min(typical_amount, 5000.0)),
                    amount_variability=self.random.uniform(0.25, 0.65),
                    activity_weight=self.random.lognormvariate(0.0, 0.7),
                )
            )

        return profiles

    def _amount(self, profile):
        amount = self.random.lognormvariate(
            math.log(profile.typical_amount),
            profile.amount_variability,
        )

        # Rare heavy-tail purchases create realistic amount anomalies.
        if self.random.random() < 0.025:
            anomaly_multiplier = max(
                2.0,
                self.random.lognormvariate(math.log(5.0), 0.45),
            )
            amount *= anomaly_multiplier

        return round(max(amount, 1.0), 2)

    def _country(self, profile):
        # Most orders occur at home; occasional foreign orders model travel
        # and create imperfect overlap with the country-switching rule.
        if self.random.random() >= 0.02:
            return profile.home_country

        foreign_countries = [
            country
            for country in COUNTRIES
            if country != profile.home_country
        ]
        return self.random.choice(foreign_countries)

    def _transaction_id(self):
        return f"tx_{uuid.UUID(int=self.random.getrandbits(128)).hex}"
