from urllib.parse import quote_plus
from .utils import S3Signer

class Authenticator:
    def __init__(
        self,
        access_key: str,
        secret_key: str,
        namespace: str,
        endpoint: str,
        region: str = None,
        method: str = 'v2'
    ):
        """
        :param access_key: S3 access key (ECS username)
        :param secret_key: S3 secret key
        :param namespace: ECS namespace
        :param endpoint: ECS S3 endpoint URL
        :param region: AWS region (for future v4 signature)
        :param method: Authentication method, 'v2' or 'v4'
        """
        self.access_key = access_key
        self.secret_key = secret_key
        self.namespace = namespace
        self.endpoint = endpoint.rstrip('/')
        self.region = region
        self.auth_method = method.lower()

    def sign(
        self,
        method: str,
        bucket: str = '',
        object_name: str = '',
        subresource: str = '',
        headers: dict = None,
        payload: bytes = b''
    ) -> (dict, str):
        """
        Returns (signed_headers, full_url)
        """
        headers = headers.copy() if headers else {}
        headers['x-emc-namespace'] = self.namespace

        path = f"/{bucket}" if bucket else '/'
        if object_name:
            path += f"/{quote_plus(object_name)}"
        url = self.endpoint + path
        if subresource:
            url += subresource

        if self.auth_method == 'v2':
            signed_headers = S3Signer.sign_request_v2(
                method, url, headers, self.access_key, self.secret_key, payload
            )
            return signed_headers, url
        else:
            raise NotImplementedError("Only v2 authentication is supported at this time.")
