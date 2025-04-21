import click
from s3client.auth import Authenticator
from s3client.bucket import BucketManager
from s3client.lifecycle import LifecycleManager
from ecs_s3_cli.printer import format_output

@click.group()
@click.option('--endpoint', required=True)
@click.option('--access-key', required=True)
@click.option('--secret-key', required=True)
@click.option('--namespace', required=True)
@click.option('--auth-method', default='v2', type=click.Choice(['v2','v4']))
@click.option('--format', 'outfmt', default='json', type=click.Choice(['json','yaml','table']))
@click.pass_context
def cli(ctx, endpoint, access_key, secret_key, namespace, auth_method, outfmt):
    auth = Authenticator(access_key, secret_key, namespace, endpoint, method=auth_method)
    ctx.obj = {
        'bucket_mgr': BucketManager(auth),
        'lifecycle_mgr': LifecycleManager(auth),
        'outfmt': outfmt
    }

# Bucket commands
@cli.group()
def bucket():
    pass

@bucket.command('create')
@click.argument('name')
@click.option('--versioning/--no-versioning', default=False)
@click.pass_context
def bucket_create(ctx, name, versioning):
    ctx.obj['bucket_mgr'].create_bucket(name, versioning=versioning)
    format_output({'success': True}, ctx.obj['outfmt'])

@bucket.command('get')
@click.argument('name')
@click.pass_context
def bucket_get(ctx, name):
    m = ctx.obj['bucket_mgr'].get_bucket(name)
    format_output(m.__dict__, ctx.obj['outfmt'])

@bucket.command('delete')
@click.argument('name')
@click.pass_context
def bucket_delete(ctx, name):
    ctx.obj['bucket_mgr'].delete_bucket(name)
    format_output({'success': True}, ctx.obj['outfmt'])

# Lifecycle commands
@cli.group()
def lifecycle():
    pass

@lifecycle.command('create-rule')
@click.argument('rule_id')
@click.option('--days', type=int)
@click.option('--expired-marker', is_flag=True)
@click.option('--prefix')
@click.option('--status', default='Enabled', type=click.Choice(['Enabled','Disabled']))
@click.pass_context
def lifecycle_create_rule(ctx, rule_id, days, expired_marker, prefix, status):
    rule = ctx.obj['lifecycle_mgr'].create_rule(rule_id, days=days, expired_object_delete_marker=expired_marker, prefix=prefix, status=status)
    format_output(rule.__dict__, ctx.obj['outfmt'])

@lifecycle.command('apply')
@click.argument('bucket')
@click.argument('rule_id')
@click.pass_context
def lifecycle_apply(ctx, bucket, rule_id):
    rule = ctx.obj['lifecycle_mgr']._rules.get(rule_id)
    ctx.obj['lifecycle_mgr'].apply_rule(bucket, rule)
    format_output({'success': True}, ctx.obj['outfmt'])

@lifecycle.command('list')
@click.argument('bucket')
@click.pass_context
def lifecycle_list(ctx, bucket):
    rules = ctx.obj['lifecycle_mgr'].get_rules(bucket)
    data = [r.__dict__ for r in rules]
    format_output(data, ctx.obj['outfmt'])