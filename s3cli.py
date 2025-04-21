import os
import sys
import click
import yaml
import json
from tabulate import tabulate

from s3manager.auth import Authenticator
from s3manager.bucket import BucketManager
from s3manager.lifecycle import LifecycleManager

def format_output(data, fmt):
    if fmt == 'json':
        click.echo(json.dumps(data, indent=2))
    elif fmt == 'yaml':
        click.echo(yaml.safe_dump(data, sort_keys=False))
    elif fmt == 'table':
        if isinstance(data, list) and data and isinstance(data[0], dict):
            click.echo(tabulate(data, headers="keys"))
        else:
            click.echo(str(data))
    else:
        click.echo(data)

CONFIG_PATH = os.path.join(os.getcwd(), '.config.yaml')

def load_config():
    defaults = {}
    if os.path.isfile(CONFIG_PATH):
        try:
            cfg = yaml.safe_load(open(CONFIG_PATH)) or {}
            defaults = cfg.get('s3', {})
        except Exception:
            pass
    return defaults

@click.group(context_settings=dict(help_option_names=['--help']))
@click.option('--endpoint', default=None, help='S3 endpoint URL')
@click.option('--access-key', default=None, help='ECS access key')
@click.option('--secret-key', default=None, help='ECS secret key')
@click.option('--namespace', default=None, help='ECS namespace')
@click.option('--region', default=None, help='Region for V4 signing')
@click.option('--auth-method', default='v2', type=click.Choice(['v2', 'v4']), help='Authentication method')
@click.option('--format', 'outfmt', default='json', type=click.Choice(['xml', 'json', 'yaml', 'table']), help='Output format')
@click.pass_context
def cli(ctx, endpoint, access_key, secret_key, namespace, region, auth_method, outfmt):
    """CLI tool for managing Dell EMC ECS S3 resources."""
    cfg = load_config()
    endpoint = endpoint or cfg.get('endpoint')
    access_key = access_key or cfg.get('access_key')
    secret_key = secret_key or cfg.get('secret_key')
    namespace = namespace or cfg.get('namespace')
    region = region or cfg.get('region')

    if not endpoint or not access_key or not secret_key:
        click.echo('Error: endpoint, access-key, and secret-key are required.', err=True)
        sys.exit(1)

    auth = Authenticator(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        namespace=namespace,
        region=region,
        method=auth_method
    )

    ctx.obj = {
        'bucket_mgr': BucketManager(auth),
        'lifecycle_mgr': LifecycleManager(auth),
        'outfmt': outfmt
    }

@cli.group()
def bucket():
    """Bucket management."""
    pass

@bucket.command('create')
@click.argument('bucket_name')
@click.option('--namespace', default=None, help='Namespace (optional)')
@click.option('--versioning', type=click.Choice(['yes','no']), default='no', help='Enable/disable versioning')
@click.pass_context
def bucket_create(ctx, bucket_name, namespace, versioning):
    """Create a bucket."""
    bm = ctx.obj['bucket_mgr']
    res = bm.create_bucket(bucket_name, namespace=namespace, versioning=(versioning=='yes'))
    format_output(res, ctx.obj['outfmt'])

@bucket.command('update')
@click.argument('bucket_name')
@click.option('--namespace', default=None, help='Namespace (optional)')
@click.option('--versioning', type=click.Choice(['yes','no']), help='Modify versioning')
@click.pass_context
def bucket_update(ctx, bucket_name, namespace, versioning):
    """Update an existing bucket."""
    bm = ctx.obj['bucket_mgr']
    res = bm.update_bucket(
        bucket_name,
        namespace=namespace,
        versioning=(versioning=='yes' if versioning else None)
    )
    format_output(res, ctx.obj['outfmt'])

@bucket.command('list-objects')
@click.argument('bucket_name')
@click.option('--filter', 'filter_', default=None, help='Filter results')
@click.pass_context
def bucket_list_objects(ctx, bucket_name, filter_):
    """List objects in a bucket."""
    bm = ctx.obj['bucket_mgr']
    objs = bm.list_objects(bucket_name, prefix=filter_)
    format_output(objs, ctx.obj['outfmt'])

@bucket.command('get-object-info')
@click.argument('bucket_name')
@click.argument('object_key')
@click.pass_context
def bucket_get_object_info(ctx, bucket_name, object_key):
    """Get information about an object."""
    bm = ctx.obj['bucket_mgr']
    info = bm.get_object_info(bucket_name, object_key)
    format_output(info, ctx.obj['outfmt'])

@bucket.command('get-info')
@click.argument('bucket_name')
@click.pass_context
def bucket_get_info(ctx, bucket_name):
    """Get bucket metadata."""
    bm = ctx.obj['bucket_mgr']
    info = bm.get_bucket_info(bucket_name)
    format_output(info, ctx.obj['outfmt'])

@cli.group()
def lifecycle():
    """Object lifecycle management."""
    pass

@lifecycle.command('apply')
@click.argument('bucket_name')
@click.argument('lifecycle_name')
@click.option('--days', type=int, help='Days until expiration')
@click.option('--years', type=int, help='Years until expiration')
@click.option('--noncurrent-version-expiration', is_flag=True, help='Expire noncurrent versions')
@click.option('--expired-object-delete-marker', is_flag=True, help='Expired object delete markers')
@click.option('--filter', 'filter_', default=None, help='Filter (prefix)')
@click.option('--status', default='Enabled', type=click.Choice(['Enabled','Disabled']), help='Rule status')
@click.option('--tag', default=None, help='Associated tag')
@click.pass_context
def lifecycle_apply(ctx, bucket_name, lifecycle_name,
                    days, years,
                    noncurrent_version_expiration,
                    expired_object_delete_marker,
                    filter_, status, tag):
    """Create and apply a lifecycle rule to a bucket."""
    lm = ctx.obj['lifecycle_mgr']
    lm.create_rule(
        name=lifecycle_name,
        days=days,
        years=years,
        noncurrent=noncurrent_version_expiration,
        delete_marker=expired_object_delete_marker,
        prefix=filter_,
        status=status,
        tag=tag
    )
    res = lm.apply_rule(bucket_name, lifecycle_name)
    format_output(res, ctx.obj['outfmt'])

@lifecycle.command('get')
@click.argument('bucket_name')
@click.pass_context
def lifecycle_get(ctx, bucket_name):
    """Get lifecycle rules of a bucket."""
    lm = ctx.obj['lifecycle_mgr']
    rules = lm.get_lifecycle(bucket_name)
    format_output(rules, ctx.obj['outfmt'])

@lifecycle.command('list-rules')
@click.argument('bucket_name')
@click.pass_context
def lifecycle_list_rules(ctx, bucket_name):
    """List all lifecycle rules of a bucket."""
    lm = ctx.obj['lifecycle_mgr']
    rules = lm.list_rules(bucket_name)
    format_output(rules, ctx.obj['outfmt'])

@lifecycle.command('purge')
@click.argument('bucket_name')
@click.pass_context
def lifecycle_purge(ctx, bucket_name):
    """Permanently delete objects marked for deletion."""
    lm = ctx.obj['lifecycle_mgr']
    res = lm.purge_expired(bucket_name)
    format_output(res, ctx.obj['outfmt'])

if __name__ == '__main__':
    cli()
