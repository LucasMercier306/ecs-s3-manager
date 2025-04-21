import json
import yaml
from tabulate import tabulate

def format_output(data, fmt):
    if fmt=='json':
        click.echo(json.dumps(data, indent=2))
    elif fmt=='yaml':
        click.echo(yaml.safe_dump(data, sort_keys=False))
    elif fmt=='table':
        if isinstance(data, list) and data and isinstance(data[0], dict):
            click.echo(tabulate(data, headers='keys'))
        else:
            click.echo(str(data))
    else:
        click.echo(data)