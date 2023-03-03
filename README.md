# Aiven's HTTP Sink Connector for Apache Kafka®

![HTTP logo](docs/IETF-Badge-HTTP.png)

This is a sink [Apache Kafka Connect](https://kafka.apache.org/documentation/#connect) connector that sends Kafka records over HTTP.

## Status

We consider the connector **stable** despite the major version is currently 0.

## How it works

The connector uses the POST HTTP method to deliver records.

The connector supports:
- authorization (static, OAuth2, Apikey);
- setting HTTP headers;
- batching;
- delivery retries.

An example of configuration:

```json
{
  "name": "http-sink-test",
  "config": {
    "connector.class": "io.aiven.kafka.connect.http.HttpSinkConnector",
    "topics.regex": "domain_a.*",
    "http.authorization.type": "none",
    "http.url": "http://httpmockserver:1080",
    "batching.enabled": true,
    "batch.max.size": 2,
    "tasks.max": "1",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter"
  }
}
```

See the [configuration options](docs/sink-connector-config-options.rst) for details.

## Development

The connector supports Java 11 or later. It uses [Gradle](https://gradle.org/) for building and automating tasks.

To build a distribution:
```bash
./gradlew clean check distTar distZip
```
(check `build/distributions/` then).

Integration tests can be run with
```bash
./gradlew integrationTest
```
and require [Docker](https://www.docker.com/) installed.

# Formatting

Checkstyle is used for verifying code formatting.
For misc files Spotless is used.

Both are run on Gradle `check` target. Spotless formatting can be done with
```bash
./gradlew spotlessApply
```

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE).

## Trademark

Apache Kafka, Apache Kafka Connect are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.
