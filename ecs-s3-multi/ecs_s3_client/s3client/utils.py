import hashlib
import hmac
import base64
import datetime
from urllib.parse import urlparse

class S3Signer:
    @staticmethod
    def sign_request_v2(method: str, url: str, headers: dict, access_key: str, secret_key: str, payload: bytes=b'') -> dict:
        parsed = urlparse(url)
        path = parsed.path or '/'
        canonical_resource = path + ('?' + parsed.query if parsed.query else '')
        date_str = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        headers['Date'] = headers.get('Date', date_str)
        # Canonicalize x-amz- and x-emc- headers
        amz = {k.lower(): ' '.join(v.split()) for k,v in headers.items() if k.lower().startswith(('x-amz-','x-emc-'))}
        canonical_amz = ''.join(f"{k}:{amz[k]}\n" for k in sorted(amz))
        content_md5 = headers.get('Content-MD5','')
        content_type = headers.get('Content-Type','')
        string_to_sign = "\n".join([method, content_md5, content_type, headers['Date'], canonical_amz + canonical_resource])
        sig = hmac.new(secret_key.encode(), string_to_sign.encode(), hashlib.sha1).digest()
        signature = base64.b64encode(sig).decode()
        headers['Authorization'] = f"AWS {access_key}:{signature}"
        return headers
