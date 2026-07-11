#!/usr/bin/env bash
# Shared configuration for the msds682 homework cluster scripts.
# Edit these values, then run ./setup.sh (spin up) or ./teardown.sh (delete).
#
# This file is *sourced* by the other scripts; it is not meant to be run directly.

# Name for your homework cluster (must be unique within your Confluent environment).
CLUSTER_NAME="msds682-hw"

# Cloud + region. Defaults match the cluster you already used
# (bootstrap host was pkc-619z3.us-east1.gcp.confluent.cloud).
CLOUD="gcp"
REGION="us-east1"

# Cluster type. Use "basic" for class: no hourly base fee, ~$0 when idle.
# Do NOT use "standard" or "dedicated" for homework — those bill while idle.
CLUSTER_TYPE="basic"

# Topic to create on spin-up (matches Demo 01).
TOPIC_NAME="msds682.demo01.trip-events.v1"
PARTITIONS=3

# Optional: pin a specific Confluent environment id (env-xxxxx).
# Leave empty to auto-detect (uses the active env, or the only env if there is one).
CONFLUENT_ENV_ID=""
