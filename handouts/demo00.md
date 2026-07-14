# Demo 00: Environment Setup and First Local Run

This first demo checks whether your laptop can run the Python environment used in MSDS 682. It does not require Confluent, Kafka, Docker, AWS, GCP, or any private credentials.

The goal is simple: create a Python environment, install the course packages, run one script, and produce a local JSON report that the TA can inspect if something goes wrong.

## What You Will Verify

- Python can run from your terminal.
- Your virtual environment is active.
- The core course packages are installed.
- A local output artifact is created under `outputs/runs/...`.

## Step 1: Create a Working Folder

Open a terminal and create a folder for the course demos.

```bash
mkdir -p msds682-demos
cd msds682-demos
```

## Step 2: Create a Python Environment

Recommended setup with `uv`:

```bash
uv python install 3.11
uv venv --python 3.11 .venv
source .venv/bin/activate
```

Fallback setup with built-in `venv`:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

On Windows PowerShell, activation usually looks like this:

```powershell
.venv\Scripts\Activate.ps1
```

## Step 3: Install the Course Packages

Download the exact Summer 2026 package baseline:

[requirements.txt](handouts/requirements.txt)

The file pins the course environment rather than installing floating latest
versions. Its Kafka line is:

```txt
confluent-kafka[avro,schemaregistry]==2.15.0
```

Install the packages:

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If you use `uv`, this is also fine:

```bash
uv pip install -r requirements.txt
```

## Step 4: Download the Demo Script

Download the script:

[demo00_environment_check.py](handouts/demo00_environment_check.py)

Or create a file named `demo00_environment_check.py` and copy the code from that link.

## Step 5: Run Demo 00

Run:

```bash
python demo00_environment_check.py --run-id lec1
```

Expected behavior:

- The terminal prints your Python version, platform, and package versions.
- A JSON report is written to:

```text
outputs/runs/lec1/demo00_environment/environment_report.json
```

## Step 6: Check the Output

Open the JSON report. It should look similar to this:

```json
{
  "python": "3.11.x",
  "implementation": "CPython",
  "platform": "macOS-...",
  "packages": {
    "fastapi": "0.x",
    "pydantic": "2.x",
    "confluent-kafka": "2.15.0",
    "fastmcp": "3.x"
  }
}
```

If a package says `"not installed"`, rerun the install step and ask for help if it still fails.

## What This Demo Means

This demo is the first reproducibility check for the course. Later assignments will do more interesting work with producers, consumers, schemas, FastAPI, and AI-assisted workflows, but they all depend on this basic setup working first.

For grading and TA review, the minimum runnable path should work locally without private cloud credentials.
