package com.streaming.fraud;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ApplicationConfigTest {

    @Test
    void runtimePropertiesOverrideEnvironment() {
        ApplicationConfig config = ApplicationConfig.fromSources(
                Map.of(
                        "KAFKA_BOOTSTRAP_SERVERS",
                        "aws-msk:9098"
                ),
                Map.of(
                        "KAFKA_BOOTSTRAP_SERVERS",
                        "environment-kafka:9092"
                )
        );

        assertEquals("aws-msk:9098", config.bootstrapServers());
    }

    @Test
    void environmentOverridesLocalDefault() {
        ApplicationConfig config = ApplicationConfig.fromSources(
                Map.of(),
                Map.of(
                        "TRANSACTIONS_RAW_TOPIC",
                        "transactions_from_environment"
                )
        );

        assertEquals(
                "transactions_from_environment",
                config.transactionsRawTopic()
        );
    }

    @Test
    void environmentProvidesS3OutputPath() {
        ApplicationConfig config = ApplicationConfig.fromSources(
                Map.of(),
                Map.of(
                        "S3_OUTPUT_PATH",
                        "s3a://example-data/curated/transactions"
                )
        );

        assertEquals(
                "s3a://example-data/curated/transactions",
                config.s3OutputPath()
        );
    }

    @Test
    void missingValuesUseLocalDefaults() {
        ApplicationConfig config = ApplicationConfig.fromSources(
                Map.of(),
                Map.of()
        );

        assertEquals("kafka:29092", config.bootstrapServers());
        assertEquals("transactions_raw", config.transactionsRawTopic());
        assertEquals("clean_transactions", config.cleanTransactionsTopic());
        assertEquals("fraud_alerts", config.fraudAlertsTopic());
        assertEquals("transactions_dlq", config.transactionsDlqTopic());
        assertEquals("transaction-router-v1", config.groupId());
        assertEquals("latest-offset", config.startupMode());
        assertEquals("file:///tmp/flink-output/transactions",config.s3OutputPath());
        assertEquals("PLAINTEXT", config.securityProtocol());
        assertEquals("AWS_MSK_IAM", config.saslMechanism());
        assertEquals(
                "software.amazon.msk.auth.iam.IAMLoginModule required;",
                config.saslJaasConfig()
        );
        assertEquals(
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
                config.saslCallbackHandler()
        );
    }
}
