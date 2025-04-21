# ecs-s3-v2

A simple and flexible Python CLI tool to manage Dell EMC ECS S3 resources from terminal.

## Features

- **Bucket management**: create, update (versioning), list objects, get info, and delete buckets
- **Lifecycle rules**: apply, list, retrieve, and purge object lifecycle configurations
- **Secure authentication**: AWS Signature V2 (with future support for Signature V4)
- **Flexible output**: JSON, YAML, or human‑readable tables
- **Config file support**: centralize your ECS credentials in `.config.yaml`

## Prerequisites

- Python **3.6** or newer
- Access to a Dell EMC ECS S3 endpoint
- ECS credentials (access key, secret key, namespace)

## Installation

1. Clone the repository

2. (Optional) Create and activate a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. (Optional) Install the CLI globally:
   ```bash
   pip install .
   ```

## Configuration

You can either pass credentials via command‑line options or define a `.config.yaml` file in your working directory:

```yaml
s3:
  endpoint: "https://object.your-ecs-domain.com"
  access_key: "YOUR_ACCESS_KEY"
  secret_key: "YOUR_SECRET_KEY"
  namespace: "YOUR_NAMESPACE"
  region: "YOUR_REGION"   # required for AWS Signature V4
```

## Usage

All commands share the following global options:

```text
  --endpoint      ECS S3 endpoint URL (required)
  --access-key    ECS access key (required)
  --secret-key    ECS secret key (required)
  --namespace     ECS namespace (required)
  --region        AWS region for V4 signing (optional)
  --auth-method   v2 or v4 (default: v2)
```

### Bucket commands

| Command                             | Description                                |
|-------------------------------------|--------------------------------------------|
| `bucket create <name>`              | Create a new bucket                       |
| `bucket get-info <name>`            | Show bucket metadata                      |
| `bucket update <name> --versioning [yes|no]` | Enable or disable versioning       |
| `bucket list-objects <name> [--filter <prefix>]` | List objects with optional prefix  |
| `bucket delete <name>`              | Delete a bucket                           |

**Examples:**
```bash
# Create and get info
s3cli bucket create mybucket
s3cli bucket get-info mybucket

# Enable versioning
s3cli bucket update mybucket --versioning yes

# List objects with prefix
s3cli bucket list-objects mybucket --filter logs/
```  

### Lifecycle commands

| Command                                                  | Description                                |
|----------------------------------------------------------|--------------------------------------------|
| `lifecycle apply <bucket> <rule-name> [options]`         | Create & apply a lifecycle rule            |
| `lifecycle get <bucket>`                                 | Retrieve all lifecycle rules               |
| `lifecycle list-rules <bucket>`                          | List rule names in memory                  |

**Rule options for `lifecycle apply`:**

- `--days <n>`  : expire objects after `<n>` days
- `--years <n>` : expire objects after `<n>` years
- `--noncurrent-version-expiration` : expire non-current object versions
- `--expired-object-delete-marker`  : remove expired delete markers
- `--filter <prefix>` : only apply to keys with given prefix
- `--status [Enabled|Disabled]` : rule state (default: Enabled)

**Example:**
```bash
# Apply a 30-day expiration rule
s3cli lifecycle apply mybucket expire30 --days 30 --filter logs/ --status Enabled

# Get the rules back
s3cli lifecycle get mybucket
```  

## Testing

A simple functional test script is provided in `test.py`. It covers bucket creation, versioning toggle, listing, and lifecycle application