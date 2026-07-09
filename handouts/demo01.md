# Demo 01: Create a Kafka Topic with Python

This demo creates a real Kafka topic in Confluent Cloud using Python.

You will use the same pattern as many production projects:

1. keep secrets in `.env`;
2. load config with `python-dotenv`;
3. create an `AdminClient`;
4. check whether the topic already exists;
5. create it only if needed;
6. write a small JSON report for debugging.

## What You Will Create

We will create a ridesharing event topic:

```text
msds682.demo01.trip-events.v1
```

Mental model:

- topic: `trip-events`
- messages: `trip_requested`, `driver_matched`, `trip_started`, `trip_completed`
- key idea: this topic stores the event history for trips

## Step 1: Create a Working Folder

```bash
mkdir -p msds682-demos
cd msds682-demos
```

## Step 2: Create and Activate a Python Environment

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

## Step 4: Create `.env`

Create a file named `.env` in the same folder as the script.

```text
BOOTSTRAP_SERVERS=YOUR_BOOTSTRAP_SERVER:9092
SECURITY_PROTOCOL=SASL_SSL
SASL_MECHANISMS=PLAIN
SASL_USERNAME=YOUR_KAFKA_API_KEY
SASL_PASSWORD=YOUR_KAFKA_API_SECRET
DEMO01_TOPIC_NAME=msds682.demo01.trip-events.v1
```

Important:

- Do not paste `.env` into Canvas, GitHub, Slack, or AI tools.
- Do not commit `.env`.
- `SASL_USERNAME` and `SASL_PASSWORD` must be a Kafka cluster API key and secret, not a general website password.

## Step 5: Download the Script

Download:

[demo01_create_topic.py](handouts/demo01_create_topic.py)

Or create a file named `demo01_create_topic.py` and copy the code from that link.

## Step 6: Run Demo 01

```bash
python demo01_create_topic.py --run-id lec2
```

Expected output:

```json
{
  "status": "created",
  "topic": "msds682.demo01.trip-events.v1",
  "partitions": 3,
  "replication_factor": 3,
  "cleanup_policy": "delete",
  "has_username": true,
  "has_password": true
}
```

If the topic already exists, this is also fine:

```json
{
  "status": "already_exists"
}
```

## Step 7: Check the Report

The script writes:

```text
outputs/runs/lec2/demo01_topic_creation/topic_report.json
```

This report is safe to show to the TA because it does not print your API secret.

## Optional: Use Your Own Topic Name

If many students share one cluster, use your initials or USF username:

```bash
python demo01_create_topic.py \
  --topic msds682.demo01.trip-events.yourname.v1 \
  --run-id lec2
```

## What This Demo Means

This is the first real Kafka admin workflow in the course. A producer cannot send useful events until the team agrees on a topic name and the topic exists. Later demos will write messages into topics; this demo creates the destination first.
