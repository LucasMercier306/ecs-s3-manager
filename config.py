import yaml

def load_config(bucket_name: str, config_file: str = ".config.yaml") -> dict:
    """Load the configuration for a specific bucket from the YAML file."""
    with open(config_file, "r") as f:
        full_config = yaml.safe_load(f)

    if bucket_name not in full_config:
        raise ValueError(f"Bucket '{bucket_name}' not found in {config_file}")

    return full_config[bucket_name]
