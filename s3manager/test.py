#!/usr/bin/env python3
import subprocess
import time
import os

ENDPOINT = os.getenv('ECS_ENDPOINT', 'https://object.ecstestdrive.com')
ACCESS_KEY = os.getenv('ECS_ACCESS_KEY', '')
SECRET_KEY = os.getenv('ECS_SECRET_KEY', '')
NAMESPACE = os.getenv('ECS_NAMESPACE', '')

BASE_CMD = [
    's3cli',
    '--endpoint', ENDPOINT,
    '--access-key', ACCESS_KEY,
    '--secret-key', SECRET_KEY,
    '--namespace', NAMESPACE,
]

FORMATS = ['json', 'yaml', 'table']

def run_command(cmd):
    print(f"\n>>> Running: {' '.join(cmd)}")
    proc = subprocess.run(cmd, text=True, capture_output=True)
    print(proc.stdout)
    if proc.stderr:
        print("[stderr]", proc.stderr)
    print("--- Completed ---\n")

if __name__ == '__main__':
    run_command(BASE_CMD + ['bucket', 'create', 'testbucket'])
    time.sleep(2)

    for fmt in FORMATS:
        run_command(BASE_CMD + ['--format', fmt, 'bucket', 'get-info', 'testbucket'])
        time.sleep(1)

    run_command(BASE_CMD + ['bucket', 'update', 'testbucket', '--versioning', 'yes'])
    time.sleep(1)
    run_command(BASE_CMD + ['bucket', 'update', 'testbucket', '--versioning', 'no'])
    time.sleep(1)

    run_command(BASE_CMD + ['bucket', 'list-objects', 'testbucket'])
    time.sleep(1)

    run_command(BASE_CMD + [
        'lifecycle', 'apply', 'testbucket', 'expire30',
        '--days', '30'
    ])
    time.sleep(2)
    run_command(BASE_CMD + ['lifecycle', 'get', 'testbucket'])
    time.sleep(1)
