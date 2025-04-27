ecs-s3-manager

A simple command-line interface (CLI) tool to manage Dell EMC ECS (S3-compatible) resources: buckets, prefixes (pseudo-folders), and lifecycle policies.

Prerequisites

Python version 3.7 or higher

Configuration file named .config.yaml at the project root, defining one or more profiles (buckets), for example:

bucket1:
  endpoint: "https://object.ecstestdrive.com"
  access_key: "AKI..."
  secret_key: "xyz..."
  namespace: "my-namespace"
  region: "us-east-1"
  prefix_list:
    - "04-2015"
    - "05-2015"
    # ... additional prefixes ...

Install dependencies:

pip install -r requirements.txt

CLI Overview

All commands require at least the --profile <name> option to select which profile from .config.yaml to use.

Usage: s3cli [OPTIONS] COMMAND [ARGS]...

Options:
  --profile TEXT    Profile name defined in .config.yaml
  --config PATH     Path to configuration file  [default: .config.yaml]
  --help            Show this message and exit

Commands:
  bucket            Bucket management commands
  create-prefixes   Create prefixes defined in profile
  lifecycle         Lifecycle policy management
  batch-lifecycle   Apply lifecycle policies in batch based on prefixes

bucket

bucket create <bucket_name>
Create a new bucket.

bucket list-objects <bucket_name> [--prefix <prefix>]
List objects in a bucket, optionally filtered by prefix.

create-prefixes

create-prefixes
For each prefix in prefix_list, create an empty prefix (folder) by issuing a PUT /bucket/prefix request.

batch-lifecycle

batch-lifecycle <years>
For each prefix MM-YYYY in prefix_list, calculate an expiration date by adding <years> years to the prefix’s date, then apply a lifecycle rule with an <Expiration><Date>…</Date></Expiration>.

lifecycle

Subcommands for manual lifecycle rule operations:

lifecycle apply <bucket> <rule_id> [OPTIONS]
Create and apply a named lifecycle rule to the bucket.

Options:

--days N — expire objects after N days

--years M — expire objects after M years

--filter <prefix> — apply rule only to keys with this prefix

--noncurrent-version-expiration — expire non-current versions

--expired-object-delete-marker — remove expired object delete markers

--status Enabled|Disabled — enable or disable the rule

--tag <key:value> — apply rule only to objects with this tag

lifecycle get <bucket>
Retrieve the raw XML of all lifecycle rules for the bucket.

lifecycle list-rules <bucket>
List all lifecycle rule IDs for the bucket.

lifecycle clean-up <bucket> <years>
Remove the oldest lifecycle rule (by prefix date) and add a new rule for the next month after the most recent one, with expiration offset by <years> years.