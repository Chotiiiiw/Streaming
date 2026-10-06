package com.streaming.fraud;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public record ApplicationConfig(
        String bootstrapServers,
        String transactionsRawTopic,
        String cleanTransactionsTopic,
        String fraudAlertsTopic,
        String transactionsDlqTopic,
        String groupId,
        String startupMode,
        String s3OutputPath,
        String securityProtocol,
        String saslMechanism,
        String saslJaasConfig,
        String saslCallbackHandler
) {

    private static final String PROPERTY_GROUP_ID = "KafkaConfigProperties";

    public ApplicationConfig {
        bootstrapServers = requireNonBlank(
                bootstrapServers,
                "bootstrapServers"
        );
        transactionsRawTopic = requireNonBlank(
                transactionsRawTopic,
                "transactionsRawTopic"
        );
        cleanTransactionsTopic = requireNonBlank(
                cleanTransactionsTopic,
                "cleanTransactionsTopic"
        );
        fraudAlertsTopic = requireNonBlank(
                fraudAlertsTopic,
                "fraudAlertsTopic"
        );
        transactionsDlqTopic = requireNonBlank(
                transactionsDlqTopic,
                "transactionsDlqTopic"
        );
        groupId = requireNonBlank(groupId, "groupId");
        startupMode = requireNonBlank(startupMode, "startupMode");
        s3OutputPath = requireNonBlank(s3OutputPath, "s3OutputPath");
        securityProtocol = requireNonBlank(
                securityProtocol,
                "securityProtocol"
        );
        saslMechanism = requireNonBlank(
                saslMechanism,
                "saslMechanism"
        );
        saslJaasConfig = requireNonBlank(
                saslJaasConfig,
                "saslJaasConfig"
        );
        saslCallbackHandler = requireNonBlank(
                saslCallbackHandler,
                "saslCallbackHandler"
        );
    }

    public static ApplicationConfig load() throws IOException {
        return fromSources(
                loadRuntimeProperties(),
                System.getenv()
        );
    }

    static ApplicationConfig fromSources(
            Map<String, String> runtimeProperties,
            Map<String, String> environment
    ) {
        return new ApplicationConfig(
                resolve(
                        runtimeProperties,
                        environment,
                        "KAFKA_BOOTSTRAP_SERVERS",
                        "kafka:29092"
                ),
                resolve(
                        runtimeProperties,
                        environment,
                        "TRANSACTIONS_RAW_TOPIC",
                        "transactions_raw"
                ),
                resolve(
                        runtimeProperties,
                        environment,
                        "CLEAN_TRANSACTIONS_TOPIC",
                        "clean_transactions"
                ),
                resolve(
                        runtimeProperties,
                        environment,
                        "FRAUD_ALERTS_TOPIC",
                        "fraud_alerts"
                ),
                resolve(
                        runtimeProperties,
                        environment,
                        "TRANSACTIONS_DLQ_TOPIC",
                        "transactions_dlq"
                ),
                resolve(
                        runtimeProperties,
                        environment,
                        "KAFKA_GROUP_ID",
                        "transaction-router-v1"
                ),
                resolve(
                        runtimeProperties,
                        environment,
                        "KAFKA_STARTUP_MODE",
                        "latest-offset"
                ),
                resolve(
                        runtimeProperties,
                        environment,
                        "S3_OUTPUT_PATH",
                        "file:///tmp/flink-output/transactions"
                ),
                resolve(
                        runtimeProperties,
                        environment,
                        "KAFKA_SECURITY_PROTOCOL",
                        "PLAINTEXT"
                ),
                resolve(
                        runtimeProperties,
                        environment,
                        "KAFKA_SASL_MECHANISM",
                        "AWS_MSK_IAM"
                ),
                resolve(
                        runtimeProperties,
                        environment,
                        "KAFKA_SASL_JAAS_CONFIG",
                        "software.amazon.msk.auth.iam."
                                + "IAMLoginModule required;"
                ),
                resolve(
                        runtimeProperties,
                        environment,
                        "KAFKA_SASL_CALLBACK_HANDLER",
                        "software.amazon.msk.auth.iam."
                                + "IAMClientCallbackHandler"
                )
        );
    }

    private static Map<String, String> loadRuntimeProperties()
            throws IOException {
        Map<String, Properties> propertyGroups;

        try {
            propertyGroups =
                    KinesisAnalyticsRuntime.getApplicationProperties();
        } catch (FileNotFoundException exception) {
            // Managed Flink supplies this file; local execution does not.
            return Map.of();
        }

        Properties properties = propertyGroups.get(PROPERTY_GROUP_ID);

        if (properties == null) {
            return Map.of();
        }

        Map<String, String> runtimeProperties = new HashMap<>();
        for (String propertyName : properties.stringPropertyNames()) {
            runtimeProperties.put(
                    propertyName,
                    properties.getProperty(propertyName)
            );
        }

        return Map.copyOf(runtimeProperties);
    }

    private static String resolve(
            Map<String, String> runtimeProperties,
            Map<String, String> environment,
            String propertyName,
            String defaultValue
    ) {
        String runtimeValue = runtimeProperties.get(propertyName);

        if (runtimeValue != null && !runtimeValue.isBlank()) {
            return runtimeValue.trim();
        }

        String environmentValue = environment.get(propertyName);

        if (environmentValue != null && !environmentValue.isBlank()) {
            return environmentValue.trim();
        }

        return defaultValue;
    }

    private static String requireNonBlank(
            String value,
            String fieldName
    ) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    fieldName + " must not be blank"
            );
        }

        return value.trim();
    }
}
