import requests

class BucketManager:
    def __init__(self, auth):
        self.auth = auth

    def create_bucket(self, bucket_name: str, namespace: str = None, versioning: bool = False):
        if namespace:
            self.auth.namespace = namespace
        headers, url = self.auth.sign('PUT', bucket=bucket_name)
        resp = requests.put(url, headers=headers)
        resp.raise_for_status()

        if versioning:
            xml = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                '<Status>Enabled</Status>'
                '</VersioningConfiguration>'
            )
            headers_v, url_v = self.auth.sign(
                'PUT',
                bucket=bucket_name,
                subresource='?versioning',
                headers={'Content-Type': 'application/xml'},
                payload=xml.encode()
            )
            resp_v = requests.put(url_v, headers=headers_v, data=xml)
            resp_v.raise_for_status()

        return {'success': True}

    def update_bucket(self, bucket_name: str, namespace: str = None, versioning: bool = None):
        if namespace:
            self.auth.namespace = namespace
        if versioning is not None:
            status = 'Enabled' if versioning else 'Suspended'
            xml = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                f'<Status>{status}</Status>'
                '</VersioningConfiguration>'
            )
            headers, url = self.auth.sign(
                'PUT',
                bucket=bucket_name,
                subresource='?versioning',
                headers={'Content-Type': 'application/xml'},
                payload=xml.encode()
            )
            resp = requests.put(url, headers=headers, data=xml)
            resp.raise_for_status()
            return {'success': True, 'versioning': status}
        return {'success': True, 'message': 'No changes applied'}

    def get_bucket_info(self, bucket_name: str) -> dict:
        headers, url = self.auth.sign('HEAD', bucket=bucket_name)
        resp = requests.head(url, headers=headers)
        if resp.status_code == 404:
            return {'success': False, 'message': f"Bucket {bucket_name} not found", 'status_code': 404}
        resp.raise_for_status()
        return {
            'success': True,
            'CreationDate': resp.headers.get('Date'),
            'ObjectCount': resp.headers.get('x-emc-meta-object-count'),
            'StorageSize': resp.headers.get('x-emc-meta-storage-size')
        }

    def list_objects(self, bucket_name: str, prefix: str = None) -> list:
        return []

    def apply_bucket_tag(self, bucket_name: str, tag_name: str) -> dict:
        return {'success': True, 'bucket': bucket_name, 'tag_applied': tag_name}

    def delete_bucket(self, bucket_name: str, namespace: str = None) -> dict:
        if namespace:
            self.auth.namespace = namespace
        headers, url = self.auth.sign('DELETE', bucket=bucket_name)
        resp = requests.delete(url, headers=headers)
        resp.raise_for_status()
        return {'success': True}
