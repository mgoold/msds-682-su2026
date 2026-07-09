# Demo 01: Create a Kafka Topic with Python

**This demo creates a real Kafka topic in Confluent Cloud using Python.**

Important: this demo creates the topic only. It does not produce Kafka messages yet.

Messages/events are created later by producer demos. Demo 01 only creates the destination that future producers will write to.

> Key classroom point: get Kafka API keys from Confluent, keep them in `.env`, load them with `python-dotenv`, then use `AdminClient` to create the topic. After this demo, Confluent should show one topic row, but production and consumption may still be empty.

## Demo Goal

| Question | Answer |
|---|---|
| What are we creating? | A Kafka topic named `msds682.demo01.trip-events.v1` |
| Are we creating messages? | No. This demo creates no messages. |
| Why create a topic first? | A producer needs a topic destination before it can send events. |
| What should Confluent show? | One topic row with 3 partitions. Production/consumption can be empty. |
| What comes later? | Producer code that writes event messages into this topic. |

## What You Should See at the End

In Confluent Cloud, open your cluster and go to Topics. You should see a row like:

| Topic name | Partitions | Production | Consumption |
|---|---:|---|---|
| `msds682.demo01.trip-events.v1` | `3` | empty or `--` | empty or `--` |

That is the expected screenshot result. Empty production/consumption is correct because no producer has sent messages yet.

Run:

```bash
python demo01_create_topic.py --run-id lec2
```

Expected terminal output:

```json
{
  "status": "created",
  "topic": "msds682.demo01.trip-events.v1",
  "partitions": 3,
  "replication_factor": 3,
  "cleanup_policy": "delete",
  "bootstrap_host": "YOUR_CLUSTER_HOST:9092",
  "has_username": true,
  "has_password": true
}
```

If you run it again, `status` should become `already_exists`. That is correct. The script is safe to rerun.

The script also writes:

```text
outputs/runs/lec2/demo01_topic_creation/topic_report.json
```

## The Example Topic

We will create this ridesharing event topic:

```text
msds682.demo01.trip-events.v1
```

Think of the topic as a future event history for trips. Right now the topic is empty; later producer demos will append event messages.

| Topic concept | In this demo |
|---|---|
| Topic name | `msds682.demo01.trip-events.v1` |
| Future event examples | `trip_requested`, `driver_matched`, `trip_started`, `trip_completed` |
| Key idea | Demo 01 creates the destination; later producers send trip events into it |
| Why versioned | `v1` leaves room for a future schema/topic change |

## High-Value Details to Notice

| Detail | Why it matters |
|---|---|
| Topic name is lowercase and versioned | This follows a production-style naming convention and avoids vague names like `test` |
| `partitions=3` | Kafka can split one topic into parallel logs; later this affects ordering and consumer scaling |
| `replication_factor=3` | Confluent Cloud expects replicated data for fault tolerance |
| `cleanup.policy=delete` | Old records expire by retention policy; this is the normal event-log behavior |
| `topic_exists(...)` before `create_topics(...)` | The script is idempotent: running it twice does not break |
| JSON report has no secret | You can show the result without exposing the API key or password |

## Read the Core Code First

This is the important logic before you run anything.

```python
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
import os


def load_config():
    # Reads .env and makes values available through os.getenv(...).
    # This keeps API keys out of the Python source file.
    load_dotenv()
    return {
        # Kafka broker address. This tells the client which cluster to contact.
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),

        # Confluent Cloud requires encrypted SASL authentication.
        "security.protocol": os.getenv("SECURITY_PROTOCOL", "SASL_SSL"),
        "sasl.mechanisms": os.getenv("SASL_MECHANISMS", "PLAIN"),

        # SASL_USERNAME is the Kafka API key.
        # SASL_PASSWORD is the Kafka API secret.
        "sasl.username": os.getenv("SASL_USERNAME"),
        "sasl.password": os.getenv("SASL_PASSWORD"),
    }


def topic_exists(admin_client, topic_name):
    # Ask Kafka for cluster metadata.
    # If the topic is already there, we do not create it again.
    return topic_name in admin_client.list_topics(timeout=10).topics


def create_topic(admin_client, topic_name):
    if topic_exists(admin_client, topic_name):
        return "already_exists"

    # This creates only the topic metadata/log structure.
    # It does not create any Kafka messages.
    topic = NewTopic(
        topic_name,

        # Three partitions means Kafka stores this topic as three ordered logs.
        # Kafka ordering is guaranteed inside one partition, not globally.
        num_partitions=3,

        # Three replicas means Confluent keeps copies for fault tolerance.
        replication_factor=3,

        # "delete" means Kafka eventually deletes old records by retention.
        # It is the normal setting for an event-history topic.
        config={"cleanup.policy": "delete"},
    )
    # create_topics returns a future. result(...) waits for broker confirmation.
    futures = admin_client.create_topics([topic])
    futures[topic_name].result(timeout=30)
    return "created"


conf = load_config()
# AdminClient is for Kafka admin operations: create topic, list topics,
# inspect cluster metadata. It is not a Producer.
admin_client = AdminClient(conf)
status = create_topic(admin_client, "msds682.demo01.trip-events.v1")
print(status)
```

Download the complete runnable script:

[demo01_create_topic.py](handouts/demo01_create_topic.py)

## What Each Piece Does

| Piece | What it does | Why students need it |
|---|---|---|
| Confluent API key | Username/secret pair that lets Python connect to your Kafka cluster | Without it, the Python client cannot authenticate |
| `.env` | Local file that stores secrets outside source code | Keeps credentials out of GitHub, Canvas, screenshots, and AI tools |
| `python-dotenv` | Reads `.env` and loads values into `os.getenv(...)` | Lets code use secrets without hardcoding them |
| `AdminClient` | Kafka admin client from `confluent-kafka` | Creates topics, checks metadata, and manages Kafka resources |
| `NewTopic` | Topic definition object | Specifies topic name, partitions, replication factor, and config |
| `topic_exists` | Metadata check | Makes the script idempotent, so rerunning does not fail |
| JSON report | Secret-free run evidence | TA can inspect result without seeing your API secret |

## What This Demo Does Not Do

| Not included yet | Why |
|---|---|
| Produce messages | Producer is the next concept after topic creation |
| Consume messages | Consumer needs messages to read first |
| Define a schema | Schema/serialization comes later |
| Show nonzero production metrics | No messages are written in Demo 01 |
| Prove topic ordering | Ordering matters after messages are produced into partitions |

## Step 1: Create a Working Folder

```bash
mkdir -p msds682-demos
cd msds682-demos
```

## Step 2: Create and Activate Python

Recommended:

```bash
uv python install 3.11
uv venv --python 3.11 .venv
source .venv/bin/activate
```

Fallback:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

## Step 3: Install Packages

```bash
python -m pip install --upgrade pip
python -m pip install confluent-kafka python-dotenv
```

## Step 4: Get Kafka API Keys from Confluent

In Confluent Cloud:

1. Open your environment and Kafka cluster.
2. Go to API keys.
3. Create a Kafka cluster API key.
4. Copy the API key and API secret immediately.
5. Copy your cluster bootstrap server, usually ending in `:9092`.

> Use a Kafka cluster API key, not your Confluent website password.

You need three values from Confluent:

| `.env` field | Where it comes from |
|---|---|
| `BOOTSTRAP_SERVERS` | Kafka cluster endpoint / bootstrap server |
| `SASL_USERNAME` | Kafka API key |
| `SASL_PASSWORD` | Kafka API secret |

## Step 5: Create `.env`

Create a file named `.env` in the same folder as `demo01_create_topic.py`.

```text
# Kafka cluster bootstrap server.
# Get this from your Confluent cluster settings.
# Keep only host:port here; do not include https://.
BOOTSTRAP_SERVERS=YOUR_CLUSTER_HOST:9092

# Confluent Cloud Kafka connections use SASL over SSL.
# For this course, leave these two values as shown.
SECURITY_PROTOCOL=SASL_SSL
SASL_MECHANISMS=PLAIN

# Kafka API key from Confluent.
# This is the username for the Python Kafka client.
SASL_USERNAME=YOUR_KAFKA_API_KEY

# Kafka API secret from Confluent.
# This is the password for the Python Kafka client.
# Never share, screenshot, commit, or paste this value into AI tools.
SASL_PASSWORD=YOUR_KAFKA_API_SECRET

# Topic name to create.
# You may customize the middle/name part if the instructor asks you to.
DEMO01_TOPIC_NAME=msds682.demo01.trip-events.v1
```

Example shape, with fake values:

```text
BOOTSTRAP_SERVERS=pkc-abc12.us-west2.gcp.confluent.cloud:9092
SECURITY_PROTOCOL=SASL_SSL
SASL_MECHANISMS=PLAIN
SASL_USERNAME=ABCD1234EFGH5678
SASL_PASSWORD=fakeSecretDoNotUse_fakeSecretDoNotUse_fakeSecretDoNotUse
DEMO01_TOPIC_NAME=msds682.demo01.trip-events.v1
```

Never submit or screenshot real `.env` values.

Quick meaning of each `.env` field:

| Field | Meaning | Example |
|---|---|---|
| `BOOTSTRAP_SERVERS` | Kafka cluster address | `pkc-abc12.us-west2.gcp.confluent.cloud:9092` |
| `SECURITY_PROTOCOL` | Encrypted auth protocol | `SASL_SSL` |
| `SASL_MECHANISMS` | SASL auth mechanism | `PLAIN` |
| `SASL_USERNAME` | Kafka API key | `ABCD1234EFGH5678` |
| `SASL_PASSWORD` | Kafka API secret | do not display |
| `DEMO01_TOPIC_NAME` | Topic this script creates | `msds682.demo01.trip-events.v1` |

## Step 6: Download the Script

Download:

[demo01_create_topic.py](handouts/demo01_create_topic.py)

Put it in the same folder as `.env`.

Your folder should look like this:

```text
msds682-demos/
  .env
  demo01_create_topic.py
  .venv/
```

## Step 7: Run Demo 01

```bash
python demo01_create_topic.py --run-id lec2
```

If successful, you should see JSON with:

- `status`: `created` or `already_exists`
- `topic`: `msds682.demo01.trip-events.v1`
- `partitions`: `3`
- `replication_factor`: `3`
- `has_username`: `true`
- `has_password`: `true`

The report deliberately does not print your API key or secret.

Then check Confluent Cloud:

1. Open the cluster.
2. Click Topics.
3. Search for `msds682.demo01.trip-events.v1`.
4. Confirm it has 3 partitions.
5. Do not worry if Production and Consumption are blank; that means no messages have been produced yet.

## Optional: Use Your Own Topic Name

If many students share one cluster, add your USF username or initials:

```bash
python demo01_create_topic.py \
  --topic msds682.demo01.trip-events.yourname.v1 \
  --run-id lec2
```

## Optional Reference: Delete a Topic

Do not run this in the normal demo. It is here so you can recognize the common admin operation.

Deleting a Kafka topic removes the topic and its stored messages. In this Demo 01 topic there are no messages yet, but in real projects deletion can destroy data.

```python
from confluent_kafka.admin import AdminClient


def delete_topic(admin_client, topic_name):
    # delete_topics also returns {topic_name: Future}.
    # result(...) waits until Kafka accepts the delete request.
    futures = admin_client.delete_topics([topic_name], operation_timeout=30)
    futures[topic_name].result(timeout=30)
    print(f"Deleted topic: {topic_name}")


# Example only. Do not run unless the instructor asks.
conf = load_config()
admin_client = AdminClient(conf)
delete_topic(admin_client, "msds682.demo01.trip-events.v1")
```

Safer production habit: before deletion, list topics, confirm the exact topic name, and check whether any producers or consumers still depend on it.

## Common Problems

| Error or symptom | Most likely cause | Fix |
|---|---|---|
| `Missing required .env values` | `.env` is missing or in the wrong folder | Put `.env` next to the script or run from the folder containing `.env` |
| Authentication failure | Wrong API key/secret or copied website password | Create a Kafka cluster API key in Confluent |
| Timeout | Bootstrap server is wrong or network is blocked | Recopy the cluster bootstrap server |
| Topic already exists | You already created it | This is fine; continue |
| Secret appears in screenshot | `.env` or hardcoded config is visible | Do not share the screenshot; rotate the API key if exposed |

## Why We Do Not Hardcode Secrets

Older classroom examples sometimes showed credentials directly in code for teaching speed. In this course, use `.env` instead:

```python
# Do not do this:
"sasl.password": "real-secret-in-source-code"

# Do this:
"sasl.password": os.getenv("SASL_PASSWORD")
```

The engineering habit matters more than this small demo: production code should not hardcode secrets.
