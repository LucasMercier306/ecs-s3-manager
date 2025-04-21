import hashlib
import hmac
import base64
import datetime
from urllib.parse import urlparse

class S3Signer:
    @staticmethod
    def sign_request_v2(method: str, url: str, headers: dict, access_key: str, secret_key: str, payload: bytes = b'') -> dict:
        """
        AWS Signature Version 2 adapted pour ECS S3 + en-têtes x-emc-*
        - method: HTTP method
        - url: full URL including ?subresource if any
        - headers: dict of headers (will be mutated to add Date, Authorization)
        - payload: raw body (for Content-MD5 if needed)
        """
        parsed = urlparse(url)
        path = parsed.path or '/'
        # Construire canonical resource avec subresource query
        canonical_resource = path
        if parsed.query:
            canonical_resource += '?' + parsed.query

        # 1) Date
        now = datetime.datetime.utcnow()
        date_str = now.strftime('%a, %d %b %Y %H:%M:%S GMT')
        headers['Date'] = headers.get('Date', date_str)

        # 2) Canonicalized Amz headers (x-amz- or x-emc-), lowercase keys, sorted
        amz_headers = {}
        for k, v in headers.items():
            lk = k.lower()
            if lk.startswith('x-amz-') or lk.startswith('x-emc-'):
                # collapse whitespace
                amz_headers[lk] = ' '.join(v.split())
        canonical_amz = ''.join(f"{k}:{amz_headers[k]}\n" for k in sorted(amz_headers))

        # 3) Content-MD5 (optionnel), Content-Type (optionnel)
        content_md5 = headers.get('Content-MD5', '')
        content_type = headers.get('Content-Type', '')

        # 4) String to sign
        string_to_sign = "\n".join([
            method,
            content_md5,
            content_type,
            headers['Date'],
            canonical_amz + canonical_resource
        ])

        # 5) HMAC-SHA1 + Base64
        sig = hmac.new(secret_key.encode('utf-8'),
                       string_to_sign.encode('utf-8'),
                       hashlib.sha1).digest()
        signature_b64 = base64.b64encode(sig).decode('utf-8')

        # 6) Authorization header
        headers['Authorization'] = f"AWS {access_key}:{signature_b64}"
        return headers
