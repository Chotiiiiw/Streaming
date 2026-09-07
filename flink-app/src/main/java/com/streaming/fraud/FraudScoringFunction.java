package com.streaming.fraud;

import com.streaming.fraud.model.ScoredTransaction;
import com.streaming.fraud.model.Transaction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class FraudScoringFunction
        extends KeyedProcessFunction<String, Transaction, ScoredTransaction> {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            FraudScoringFunction.class
    );

    private static final Duration VELOCITY_WINDOW = Duration.ofMinutes(3);
    private static final Duration SPENDING_WINDOW = Duration.ofMinutes(5);
    private static final Duration COUNTRY_WINDOW = Duration.ofMinutes(30);
    private static final int MINIMUM_BASELINE_SIZE = 5;
    private static final int MAXIMUM_BASELINE_SIZE = 10;
    private static final int HIGH_RISK_THRESHOLD = 60;
    private static final int MEDIUM_RISK_THRESHOLD = 30;

    private transient ListState<Transaction> recentTransactions;
    private transient ListState<BigDecimal> baselineAmounts;
    private transient boolean firstTransactionLogged;

    @Override
    public void open(OpenContext openContext) {
        ListStateDescriptor<Transaction> recentDescriptor =
                new ListStateDescriptor<>(
                    "recent-transactions-30m",
                    Transaction.class
                );
        recentDescriptor.enableTimeToLive(
                StateTtlConfig.newBuilder(Duration.ofHours(1)).build()
        );
        recentTransactions = getRuntimeContext().getListState(recentDescriptor);

        ListStateDescriptor<BigDecimal> baselineDescriptor =
                new ListStateDescriptor<>(
                    "clean-baseline-last-10",
                    BigDecimal.class
                );
        baselineDescriptor.enableTimeToLive(
                StateTtlConfig.newBuilder(Duration.ofDays(30)).build()
        );
        baselineAmounts = getRuntimeContext().getListState(baselineDescriptor);
        firstTransactionLogged = false;
    }

    @Override
    public void processElement(
            Transaction current,
            Context context,
            Collector<ScoredTransaction> collector
    ) throws Exception {
        if (!firstTransactionLogged) {
            LOGGER.info(
                    "Received first valid transaction from MSK; "
                            + "Kafka source connection and fraud scoring "
                            + "pipeline are active."
            );
            firstTransactionLogged = true;
        }

        List<Transaction> recent = recentTransactions(current.eventTime);
        List<BigDecimal> baseline = values(baselineAmounts.get());
        BigDecimal baselineAverage = average(baseline);

        int velocityCount = 1;
        BigDecimal spendingTotal = current.amount;
        Set<String> countries = new HashSet<>();
        countries.add(current.country);

        Instant velocityCutoff = current.eventTime.minus(VELOCITY_WINDOW);
        Instant spendingCutoff = current.eventTime.minus(SPENDING_WINDOW);
        Instant countryCutoff = current.eventTime.minus(COUNTRY_WINDOW);

        for (Transaction previous : recent) {
            if (previous.eventTime.isAfter(current.eventTime)) {
                continue;
            }
            if (!previous.eventTime.isBefore(velocityCutoff)) {
                velocityCount++;
            }
            if (!previous.eventTime.isBefore(spendingCutoff)) {
                spendingTotal = spendingTotal.add(previous.amount);
            }
            if (!previous.eventTime.isBefore(countryCutoff)) {
                countries.add(previous.country);
            }
        }

        int velocityScore = velocityScore(velocityCount);
        int amountAnomalyScore = amountAnomalyScore(
                current.amount,
                baselineAverage,
                baseline.size()
        );
        int spendingBurstScore = spendingBurstScore(
                spendingTotal,
                baselineAverage,
                baseline.size()
        );
        int countrySwitchScore = countrySwitchScore(countries.size());
        int totalScore = Math.addExact(
                Math.addExact(velocityScore, amountAnomalyScore),
                Math.addExact(spendingBurstScore, countrySwitchScore)
        );

        List<String> reasons = new ArrayList<>();
        addReason(reasons, velocityScore, "TRANSACTION_VELOCITY");
        addReason(reasons, amountAnomalyScore, "AMOUNT_ANOMALY");
        addReason(reasons, spendingBurstScore, "SPENDING_BURST");
        addReason(reasons, countrySwitchScore, "COUNTRY_SWITCHING");

        String riskLevel = riskLevel(totalScore);
        collector.collect(
                new ScoredTransaction(
                        current,
                        velocityScore,
                        amountAnomalyScore,
                        spendingBurstScore,
                        countrySwitchScore,
                        totalScore,
                        riskLevel,
                        reasons.toArray(String[]::new)
                )
        );

        recent.add(current);
        recentTransactions.update(recent);

        // Do not let high-risk activity teach the user's normal baseline.
        if (totalScore < HIGH_RISK_THRESHOLD) {
            baseline.add(current.amount);
            if (baseline.size() > MAXIMUM_BASELINE_SIZE) {
                baseline.remove(0);
            }
            baselineAmounts.update(baseline);
        }
    }

    static int velocityScore(int transactionCount) {
        if (transactionCount < 3) {
            return 0;
        }
        return doublingScore(transactionCount, 3, 15);
    }

    static int amountAnomalyScore(
            BigDecimal amount,
            BigDecimal baselineAverage,
            int baselineSize
    ) {
        if (baselineSize < MINIMUM_BASELINE_SIZE
                || baselineAverage == null
                || baselineAverage.signum() == 0) {
            return 0;
        }

        double ratio = ratio(amount, baselineAverage);
        if (ratio < 2) {
            return 0;
        }
        if (ratio < 3) {
            return 10;
        }
        if (ratio < 5) {
            return 20;
        }
        if (ratio < 10) {
            return 30;
        }
        return 40 + doublingSteps(ratio / 10.0) * 10;
    }

    static int spendingBurstScore(
            BigDecimal spendingTotal,
            BigDecimal baselineAverage,
            int baselineSize
    ) {
        if (baselineSize < MINIMUM_BASELINE_SIZE
                || baselineAverage == null
                || baselineAverage.signum() == 0) {
            return 0;
        }

        BigDecimal expectedSpending = baselineAverage.multiply(
                BigDecimal.valueOf(5)
        );
        double spendingRatio = ratio(spendingTotal, expectedSpending);
        if (spendingRatio < 1) {
            return 0;
        }
        return (1 + doublingSteps(spendingRatio)) * 15;
    }

    static int countrySwitchScore(int distinctCountryCount) {
        if (distinctCountryCount <= 1) {
            return 0;
        }
        return distinctCountryCount == 2 ? 30 : 60;
    }

    private List<Transaction> recentTransactions(Instant currentEventTime)
            throws Exception {
        Instant cutoff = currentEventTime.minus(COUNTRY_WINDOW);
        List<Transaction> retained = new ArrayList<>();

        for (Transaction transaction : recentTransactions.get()) {
            if (!transaction.eventTime.isBefore(cutoff)) {
                retained.add(transaction);
            }
        }
        return retained;
    }

    private static List<BigDecimal> values(Iterable<BigDecimal> values) {
        List<BigDecimal> result = new ArrayList<>();
        for (BigDecimal value : values) {
            result.add(value);
        }
        return result;
    }

    private static BigDecimal average(List<BigDecimal> amounts) {
        if (amounts.isEmpty()) {
            return null;
        }
        BigDecimal total = BigDecimal.ZERO;
        for (BigDecimal amount : amounts) {
            total = total.add(amount);
        }
        return total.divide(
                BigDecimal.valueOf(amounts.size()),
                8,
                RoundingMode.HALF_UP
        );
    }

    private static int doublingScore(
            int value,
            int initialThreshold,
            int step
    ) {
        int tier = 1;
        long nextThreshold = (long) initialThreshold * 2;
        while (value >= nextThreshold) {
            tier++;
            nextThreshold *= 2;
        }
        return Math.multiplyExact(tier, step);
    }

    private static int doublingSteps(double value) {
        return (int) Math.floor(Math.log(value) / Math.log(2));
    }

    private static double ratio(BigDecimal numerator, BigDecimal denominator) {
        return numerator.divide(denominator, 8, RoundingMode.HALF_UP)
                .doubleValue();
    }

    private static String riskLevel(int score) {
        if (score >= HIGH_RISK_THRESHOLD) {
            return "HIGH";
        }
        if (score >= MEDIUM_RISK_THRESHOLD) {
            return "MEDIUM";
        }
        return "LOW";
    }

    private static void addReason(
            List<String> reasons,
            int score,
            String reason
    ) {
        if (score > 0) {
            reasons.add(reason);
        }
    }
}
