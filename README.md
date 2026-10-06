# Real-Time Streaming Fraud Detection

A learning project that processes transaction events in real time and identifies potentially fraudulent activity.

## How it works

```text
AWS Lambda producer
        |
        v
Amazon MSK (transactions_raw)
        |
        v
Apache Flink
        |
        +--> clean_transactions
        +--> fraud_alerts
        +--> transactions_dlq
        +--> Amazon S3
```

The Lambda function publishes simulated transactions to Amazon MSK. Apache Flink validates and scores each transaction, then routes it to the appropriate output.

## Technology

- Python and AWS Lambda
- Amazon MSK Serverless
- Apache Flink and Java 17
- Amazon S3
- Terraform

## Project structure

```text
producer/          Lambda producer and Python tests
flink-app/         Flink application and Java tests
infra/terraform/   AWS infrastructure
docs/              Deployment steps and results
```

## Test and build

Run the Python tests:

```bash
cd producer
python3 -m unittest discover -p 'test_*.py'
```

Build the Lambda package:

```bash
cd producer
./build_lambda_package.sh
```

Test and build the Flink application:

```bash
cd flink-app
mvn clean verify
```

## AWS deployment

See [Deployment and results](docs/results.md) for the AWS setup, Terraform deployment, test scenario, screenshots, and cleanup steps.