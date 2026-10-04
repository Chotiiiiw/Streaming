package com.streaming.fraud;

import com.streaming.fraud.model.InvalidTransaction;
import com.streaming.fraud.model.Transaction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
// use bigdecimal to avoid floating-point error
import java.math.BigDecimal;

public final class TransactionValidationFunction
        extends ProcessFunction<Transaction, Transaction> {

    public static final OutputTag<InvalidTransaction> INVALID_TRANSACTIONS =
            new OutputTag<>("invalid-transactions") {
            };

    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<Transaction> collector
    ) {
        String errorReason = validationError(transaction);

        if (errorReason == null) {
            collector.collect(transaction);
        } else {
            context.output(
                    INVALID_TRANSACTIONS,
                    new InvalidTransaction(transaction, errorReason)
            );
        }
    }

    private static String validationError(Transaction transaction) {
        if (isBlank(transaction.transactionId)) {
            return "missing transaction_id";
        }
        if (isBlank(transaction.userId)) {
            return "missing user_id";
        }
        if (transaction.amount == null) {
            return "amount is missing or invalid";
        }
        if (transaction.amount.compareTo(BigDecimal.ZERO) <= 0) {
            return "amount must be greater than zero";
        }
        if (isBlank(transaction.country)) {
            return "missing country";
        }
        if (transaction.eventTime == null) {
            return "event_time is missing or invalid";
        }
        return null;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
