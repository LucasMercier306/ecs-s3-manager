from dataclasses import dataclass
import requests

@dataclass
class BucketModel:
    name: str
    namespace: str
    versioning_enabled: bool
    creation_date: str
    object_count: int
    storage_size: int

class BucketManager:
    def __init__(self, auth):
        self.auth = auth

    def create_bucket(self, bucket_name: str, namespace: str=None, versioning: bool=False) -> None:
        if namespace:
            self.auth.namespace = namespace
        hdrs, url = self.auth.sign('PUT', bucket=bucket_name)
        r = requests.put(url, headers=hdrs); r.raise_for_status()
        if versioning:
            xml = ('<?xml version="1.0"?>'
                   '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                   '<Status>Enabled</Status></VersioningConfiguration>')
            hdrs2, url2 = self.auth.sign('PUT', bucket=bucket_name, subresource='?versioning', headers={'Content-Type':'application/xml'}, payload=xml.encode())
            r2 = requests.put(url2, headers=hdrs2, data=xml); r2.raise_for_status()

    def get_bucket(self, bucket_name: str, namespace: str=None) -> BucketModel:
        if namespace:
            self.auth.namespace = namespace
        # HEAD
        hdrs, url = self.auth.sign('HEAD', bucket=bucket_name)
        r = requests.head(url, headers=hdrs); r.raise_for_status()
        # Versioning
        hdrv, urlv = self.auth.sign('GET', bucket=bucket_name, subresource='?versioning')
        rv = requests.get(urlv, headers=hdrv); rv.raise_for_status()
        ver = 'Enabled' in rv.text
        return BucketModel(
            name=bucket_name,
            namespace=self.auth.namespace,
            versioning_enabled=ver,
            creation_date=r.headers.get('Date'),
            object_count=int(r.headers.get('x-emc-meta-object-count',0)),
            storage_size=int(r.headers.get('x-emc-meta-storage-size',0))
        )

    def delete_bucket(self, bucket_name: str, namespace: str=None) -> None:
        if namespace:
            self.auth.namespace = namespace
        hdrs, url = self.auth.sign('DELETE', bucket=bucket_name)
        r = requests.delete(url, headers=hdrs); r.raise_for_status()
