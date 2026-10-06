CREATE TABLE transactions_s3_sink (
    `transaction_id` STRING,
    `user_id` STRING,
    `amount` DECIMAL(18, 2),
    `country` STRING,
    `event_time` TIMESTAMP_LTZ(3),
    `velocity_score` INT,
    `amount_anomaly_score` INT,
    `spending_burst_score` INT,
    `country_switch_score` INT,
    `risk_score` INT,
    `risk_level` STRING,
    `risk_reasons` ARRAY<STRING>
) WITH (
    'connector' = 'filesystem',
    'path' = '{{S3_OUTPUT_PATH}}',
    'format' = 'json',
    'sink.rolling-policy.file-size' = '128MB',
    'sink.rolling-policy.rollover-interval' = '15 min',
    'sink.rolling-policy.check-interval' = '1 min'
);
