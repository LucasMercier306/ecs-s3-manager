import sys
import click
import yaml
import requests
import datetime
from dateutil.relativedelta import relativedelta
from s3manager.auth import Authenticator
from s3manager.bucket import BucketManager
from s3manager.lifecycle import LifecycleManager, build_lifecycle_with_date

@click.group(context_settings=dict(help_option_names=['--help']))
@click.option('--profile', required=True, help='Profile name from .config.yaml')
@click.option('--config', 'config_path', default='.config.yaml', help='Path to configuration file')
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
    for key in ('endpoint','access_key','secret_key','namespace','region'):
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
    """Create prefixes (folders) in the bucket as defined in config."""
    auth = ctx.obj['auth']
    bucket = ctx.obj['profile']
    prefixes = ctx.obj['conf'].get('prefix_list', [])
    for prefix in prefixes:
        headers, url = auth.sign(
            'PUT', bucket=bucket, subresource=f"/{prefix}", headers={'Content-Type':'application/xml'}
        )
        resp = requests.put(url, headers=headers, verify=False)
        resp.raise_for_status()
        click.echo(resp.text)

@cli.command('batch-lifecycle')
@click.argument('years', type=int)
@click.pass_context
def batch_lifecycle_cmd(ctx, years):
    """
    Apply expiration lifecycle based on prefixes from config with given years offset.
    """
    lc_mgr = ctx.obj['lifecycle_mgr']
    bucket = ctx.obj['profile']
    prefixes = ctx.obj['conf'].get('prefix_list', [])
    for prefix in prefixes:
        try:
            month, year = prefix.split('-')
            base = datetime.date(int(year), int(month), 1)
            expire = base + relativedelta(years=years)
            date_str = expire.strftime('%Y-%m-%dT00:00:00Z')
            rule_id = f"lifecycle-{prefix}"
            xml_body = build_lifecycle_with_date(rule_id, prefix, date_str)
            resp = lc_mgr.apply_lifecycle_with_xml(bucket, xml_body)
            click.echo(resp.text)
        except Exception as e:
            click.echo(f"Error for prefix {prefix}: {e}", err=True)

@cli.group()
@click.pass_context
def bucket(ctx):
    """Bucket management."""
    pass

@bucket.command('create')
@click.argument('bucket_name')
@click.pass_context
def bucket_create(ctx, bucket_name):
    """Create a bucket."""
    res = ctx.obj['bucket_mgr'].create_bucket(bucket_name)
    click.echo(res.body)

@bucket.command('list-objects')
@click.argument('bucket_name')
@click.option('--prefix', default='', help='Filter prefix')
@click.pass_context
def bucket_list_objects(ctx, bucket_name, prefix):
    """List objects in a bucket."""
    res = ctx.obj['bucket_mgr'].list_objects(bucket_name, prefix=prefix)
    click.echo(res.body)

@cli.group()
@click.pass_context
def lifecycle(ctx):
    """Object lifecycle management."""
    pass

@lifecycle.command('apply')
@click.argument('bucket_name')
@click.argument('lifecycle_name')
@click.option('--days', type=int, help='Days until expiration')
@click.option('--years', type=int, help='Years until expiration')
@click.option('--noncurrent-version-expiration', is_flag=True)
@click.option('--expired-object-delete-marker', is_flag=True)
@click.option('--filter', 'prefix', default=None)
@click.option('--status', default='Enabled', type=click.Choice(['Enabled','Disabled']))
@click.option('--tag', default=None)
@click.pass_context
def lifecycle_apply(ctx, bucket_name, lifecycle_name, days, years,
                    noncurrent_version_expiration, expired_object_delete_marker,
                    prefix, status, tag):
    """Create and apply a lifecycle rule to a bucket."""
    lm = ctx.obj['lifecycle_mgr']
    lm.create_rule(
        name=lifecycle_name,
        days=days,
        years=years,
        noncurrent=noncurrent_version_expiration,
        delete_marker=expired_object_delete_marker,
        prefix=prefix,
        status=status,
        tag=tag
    )
    res = lm.apply_rule(bucket_name, lifecycle_name)
    click.echo(res)

@lifecycle.command('get')
@click.argument('bucket_name')
@click.pass_context
def lifecycle_get(ctx, bucket_name):
    """Get lifecycle rules of a bucket."""
    lm = ctx.obj['lifecycle_mgr']
    rules_xml = lm.get_lifecycle(bucket_name)
    click.echo(rules_xml)

@lifecycle.command('list-rules')
@click.argument('bucket_name')
@click.pass_context
def lifecycle_list_rules(ctx, bucket_name):
    """List lifecycle rule names of a bucket."""
    lm = ctx.obj['lifecycle_mgr']
    rules = lm.list_rules(bucket_name)
    click.echo(rules)

@lifecycle.command('clean-up')
@click.argument('bucket_name')
@click.argument('years', type=int)
@click.pass_context
def clean_up(ctx, bucket_name, years):
    """
    Remove oldest lifecycle rule and add new for next prefix expanded by years.
    """
    lm = ctx.obj['lifecycle_mgr']
    rules = lm.list_rules(bucket_name)
    if not rules:
        click.echo('No lifecycle rules found', err=True)
        return
    # Extract prefixes and map to dates
    dates = []
    for rid in rules:
        if '-' in rid:
            _, pref = rid.split('-', 1)
            m, y = pref.split('-')
            dates.append((datetime.date(int(y), int(m), 1), rid))
    if not dates:
        click.echo('No parsable lifecycle rule IDs', err=True)
        return
    # Sort by date
    dates.sort(key=lambda x: x[0])
    oldest_date, oldest_rid = dates[0]
    newest_date, newest_rid = dates[-1]
    # Remove oldest
    lm.remove_rule(bucket_name, oldest_rid)
    click.echo(f'Removed oldest rule: {oldest_rid}')
    # Compute next prefix
    ny, nm = newest_date.year, newest_date.month
    if nm == 12:
        nm = 1
        ny += 1
    else:
        nm += 1
    next_pref = f"{nm:02d}-{ny}"
    base = datetime.date(ny, nm, 1)
    expire = base + relativedelta(years=years)
    date_str = expire.strftime('%Y-%m-%dT00:00:00Z')
    new_rid = f"lifecycle-{next_pref}"
    xml_body = build_lifecycle_with_date(new_rid, next_pref, date_str)
    resp = lm.apply_lifecycle_with_xml(bucket_name, xml_body)
    click.echo(f'Created new rule: {new_rid}')
    click.echo(resp.text)


if __name__ == '__main__':
    cli()