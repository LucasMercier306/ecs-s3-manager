import sys
import datetime
import xml.etree.ElementTree as ET
from dateutil.relativedelta import relativedelta
import click
import yaml
import requests
from s3manager.auth import Authenticator
from s3manager.bucket import BucketManager
from s3manager.lifecycle import LifecycleManager, build_lifecycle_with_date


@click.group(context_settings=dict(help_option_names=['--help']))
@click.option('--profile', required=True, help='Profile name from .config.yaml')
@click.option('--config', 'config_path', default='.config.yaml',
              help='Path to configuration file')
@click.pass_context
def cli(ctx, profile, config_path):
    """CLI tool for managing Dell EMC ECS S3 resources."""
    try:
        cfg = yaml.safe_load(open(config_path)) or {}
    except Exception as e:
        click.echo(f"Error loading config: {e}", err=True)
        sys.exit(1)

    if profile not in cfg:
        click.echo(f"Profile '{profile}' not found in {config_path}", err=True)
        sys.exit(1)

    conf = cfg[profile]
    for key in ('endpoint', 'access_key', 'secret_key', 'namespace', 'region'):
        if not conf.get(key):
            click.echo(f"Missing '{key}' in config for profile '{profile}'", err=True)
            sys.exit(1)

    auth = Authenticator(
        endpoint=conf['endpoint'],
        access_key=conf['access_key'],
        secret_key=conf['secret_key'],
        namespace=conf['namespace'],
        region=conf['region'],
        method=conf.get('auth_method', 'v2')
    )

    ctx.obj = {
        'profile': profile,
        'conf': conf,
        'auth': auth,
        'bucket_mgr': BucketManager(auth),
        'lifecycle_mgr': LifecycleManager(auth)
    }


@cli.command('create-prefixes')
@click.pass_context
def create_prefixes_cmd(ctx):
    """Create placeholder objects for prefixes defined in config."""
    auth = ctx.obj['auth']
    bucket = ctx.obj['profile']
    prefixes = ctx.obj['conf'].get('prefix_list', [])

    for prefix in prefixes:
        key = f"{prefix}/"
        headers, url = auth.sign('PUT', bucket=bucket,
                                 subresource=f"/{key}")
        resp = requests.put(url, headers=headers,
                            data=b'', verify=False)
        resp.raise_for_status()
        click.echo(f"Created placeholder: {key}")


@cli.command('list-objects')
@click.argument('bucket_name')
@click.option('--prefix', default='', help='Filter prefix')
@click.pass_context
def list_objects_cmd(ctx, bucket_name, prefix):
    """List objects in a bucket, optionally filtered by prefix."""
    bm = ctx.obj['bucket_mgr']
    objects = bm.list_objects(bucket_name, prefix=prefix)

    for obj in objects:
        key = getattr(obj, 'key', obj)
        click.echo(key)


@cli.command('batch-lifecycle')
@click.argument('years', type=int)
@click.pass_context
def batch_lifecycle_cmd(ctx, years):
    """
    Apply lifecycle rules for each prefix with expiration offset by years.
    """
    lm = ctx.obj['lifecycle_mgr']
    bucket = ctx.obj['profile']
    prefixes = ctx.obj['conf'].get('prefix_list', [])

    for prefix in prefixes:
        month, year = prefix.split('-')
        base = datetime.date(int(year), int(month), 1)
        expire = base + relativedelta(years=years)
        date_str = expire.strftime('%Y-%m-%dT00:00:00Z')
        rule_id = f"lifecycle-{prefix}"
        xml_body = build_lifecycle_with_date(
            rule_id, prefix+'/', date_str
        )
        res = lm.apply_lifecycle_with_xml(bucket, xml_body)
        click.echo(
            f"Applied rule {rule_id} expires on {date_str},"
            f" status={res.status_code}"
        )


@cli.group()
@click.pass_context
def lifecycle(ctx):
    """Group commands for lifecycle management."""
    pass


@lifecycle.command('populate-lifecycles')
@click.argument('bucket_name')
@click.argument('years', type=int)
@click.pass_context
def lifecycle_populate_cmd(ctx, bucket_name, years):
    """
    Monthly maintenance: remove oldest lifecycle rule and placeholder
    then add new rule and placeholder for next month.
    """
    lm = ctx.obj['lifecycle_mgr']
    bm = ctx.obj['bucket_mgr']
    auth = ctx.obj['auth']

    rules = [rid for rid in lm.list_rules(bucket_name)
             if rid.startswith('lifecycle-')]

    parsed = []
    for rid in rules:
        prefix_part = rid.split('-', 1)[1]
        try:
            m, y = map(int, prefix_part.split('-'))
            parsed.append((datetime.date(y, m, 1), rid, prefix_part))
        except ValueError:
            continue

    if not parsed:
        click.echo('No lifecycle rules to populate')
        return

    parsed.sort(key=lambda x: x[0])
    oldest_date, old_rule, old_prefix = parsed[0]

    objs = bm.list_objects(bucket_name,
                           prefix=f"{old_prefix}/")
    if not objs:
        if len(parsed) > 1:
            lm.remove_rule(bucket_name, old_rule)
        else:
            headers, url = auth.sign(
                'DELETE', bucket=bucket_name,
                subresource='?lifecycle'
            )
            requests.delete(url, headers=headers,
                            verify=False).raise_for_status()

        headers, url = auth.sign(
            'DELETE', bucket=bucket_name,
            subresource=f"/{old_prefix}/"
        )
        requests.delete(url, headers=headers,
                        verify=False).raise_for_status()
        click.echo(f"Removed rule {old_rule} and placeholder {old_prefix}/")

    latest_date, _, latest_prefix = parsed[-1]
    next_month = latest_date.month % 12 + 1
    next_year = latest_date.year + (1 if
                                     latest_date.month == 12 else 0)
    new_prefix = f"{next_month:02d}-{next_year}"

    base_date = datetime.date(next_year, next_month, 1)
    new_expire = base_date + relativedelta(years=years)
    new_date_str = new_expire.strftime('%Y-%m-%dT00:00:00Z')
    new_rule_id = f"lifecycle-{new_prefix}"
    new_xml = build_lifecycle_with_date(
        new_rule_id, new_prefix+'/', new_date_str
    )
    lm.apply_lifecycle_with_xml(bucket_name, new_xml)

    headers, url = auth.sign(
        'PUT', bucket=bucket_name,
        subresource=f"/{new_prefix}/"
    )
    requests.put(url, headers=headers,
                 data=b'', verify=False)
    click.echo(f"Created rule {new_rule_id} & placeholder {new_prefix}/")


@lifecycle.command('get')
@click.argument('bucket_name')
@click.pass_context
def lifecycle_get_cmd(ctx, bucket_name):
    """Display all lifecycle rules for a bucket."""
    lm = ctx.obj['lifecycle_mgr']
    xml = lm.get_lifecycle(bucket_name)

    try:
        root = ET.fromstring(xml)
        ns = '{http://s3.amazonaws.com/doc/2006-03-01/}'
        for rule in root.findall(f'.//{ns}Rule'):
            rid = rule.find(f'{ns}ID').text
            prefix = rule.find(f'{ns}Filter/{ns}Prefix').text
            date = rule.find(f'{ns}Expiration/{ns}Date').text
            click.echo(f"Rule: {rid}, Prefix: {prefix}, Expires: {date}")
    except ET.ParseError:
        click.echo(xml)


@lifecycle.command('list-objects')
@click.argument('bucket_name')
@click.option('--prefix', default='', help='Filter prefix')
@click.pass_context
def lifecycle_list_objects_cmd(ctx, bucket_name, prefix):
    """List objects in a bucket (lifecycle group)."""
    bm = ctx.obj['bucket_mgr']
    objs = bm.list_objects(bucket_name, prefix=prefix)
    for obj in objs:
        key = getattr(obj, 'key', obj)
        click.echo(key)


if __name__ == '__main__':
    cli()