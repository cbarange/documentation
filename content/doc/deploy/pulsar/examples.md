---
type: docs
linkTitle: Examples
title: Pulsar examples
description: Produce and consume messages on a Pulsar add-on of Clever Cloud from Python, Java, and Rust, with the environment variables of the add-on
keywords:
- pulsar
- examples
- python
- java
- rust
- producer
- consumer
aliases:
- /doc/pulsar/examples
---

Each example below subscribes to a topic, publishes a message to it, then receives and acknowledges this message. It reads the [environment variables](/doc/deploy/pulsar/#connect-to-pulsar) of the add-on, which your application gets when you link the add-on to it. To run an example on your computer, export them first, with the ID of the add-on or its real ID, such as `pulsar_xxx`:

```bash
eval "$(clever addon env <addon-id> -F shell)"
```

A topic keeps the [schema](https://pulsar.apache.org/docs/schema-overview/) of the first client that uses it. Use a different topic for each example, or the same schema in each client.

## Python

Install the official [Python client](https://pulsar.apache.org/docs/client-libraries-python/):

```text {filename="requirements.txt"}
pulsar-client>=3.13
```

```python {filename="main.py"}
import os
import pulsar

client = pulsar.Client(
    os.environ["ADDON_PULSAR_BINARY_URL"],
    authentication=pulsar.AuthenticationToken(os.environ["ADDON_PULSAR_TOKEN"]),
)
topic = f"persistent://{os.environ['ADDON_PULSAR_TENANT']}/{os.environ['ADDON_PULSAR_NAMESPACE']}/python-topic"

consumer = client.subscribe(topic, "my-subscription", initial_position=pulsar.InitialPosition.Earliest)
producer = client.create_producer(topic)
producer.send(b"Hello from Python")

message = consumer.receive()
print("Received:", message.data().decode())
consumer.acknowledge(message)
client.close()
```

## Java

Add the official [Java client](https://pulsar.apache.org/docs/client-libraries-java/) to your `pom.xml`:

```xml {filename="pom.xml"}
<dependency>
  <groupId>org.apache.pulsar</groupId>
  <artifactId>pulsar-client</artifactId>
  <version>4.2.0</version>
</dependency>
```

```java {filename="Hello.java"}
import org.apache.pulsar.client.api.*;

public class Hello {
    public static void main(String[] args) throws PulsarClientException {
        String topic = "persistent://" + System.getenv("ADDON_PULSAR_TENANT") + "/"
            + System.getenv("ADDON_PULSAR_NAMESPACE") + "/java-topic";

        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(System.getenv("ADDON_PULSAR_BINARY_URL"))
                .authentication(AuthenticationFactory.token(System.getenv("ADDON_PULSAR_TOKEN")))
                .build();
             Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
             Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create()) {
            producer.send("Hello from Java");
            Message<String> message = consumer.receive();
            System.out.println("Received: " + message.getValue());
            consumer.acknowledge(message);
        }
    }
}
```

## Rust

Add [Magnetar](https://github.com/CleverCloud/magnetar), the Rust client that Clever Cloud maintains as an alternative to [pulsar-rs](https://github.com/streamnative/pulsar-rs). Its crates.io package is `magnetar-driver`, and your code imports it as `magnetar`, as in the example below:

```toml {filename="Cargo.toml"}
[dependencies]
magnetar-driver = "1.8"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust {filename="src/main.rs"}
use std::sync::Arc;

use magnetar::proto::auth::TokenAuth;
use magnetar::{OutgoingMessage, PulsarClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env = |name: &str| std::env::var(name).unwrap();
    let topic = format!(
        "persistent://{}/{}/rust-topic",
        env("ADDON_PULSAR_TENANT"),
        env("ADDON_PULSAR_NAMESPACE")
    );

    let client = PulsarClient::builder()
        .service_url(env("ADDON_PULSAR_BINARY_URL"))
        .auth(Arc::new(TokenAuth::from_env("ADDON_PULSAR_TOKEN")?))
        .build()
        .await?;

    let consumer = client.consumer(&topic).subscription("my-subscription").subscribe().await?;
    let producer = client.producer(&topic).create().await?;
    producer
        .send(OutgoingMessage::with_payload(b"Hello from Rust".as_slice()).into())
        .await?;

    let message = consumer.receive().await?;
    println!("Received: {}", String::from_utf8_lossy(&message.payload));
    consumer.ack(message.message_id).await?;
    Ok(())
}
```
