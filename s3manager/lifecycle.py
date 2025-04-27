import hashlib
import base64
import requests
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element, SubElement, tostring

NS = "http://s3.amazonaws.com/doc/2006-03-01/"

class LifecycleManager:
    def __init__(self, auth):
        self.auth = auth
        self.rules = {}

    def get_lifecycle(self, bucket_name: str) -> str:
        headers, url = self.auth.sign('GET', bucket=bucket_name, subresource='?lifecycle')
        resp = requests.get(url, headers=headers)
        if resp.status_code == 404:
            return ''
        resp.raise_for_status()
        return resp.text

    def list_rules(self, bucket_name: str) -> list:
        xml = self.get_lifecycle(bucket_name)
        if not xml:
            return []
        root = ET.fromstring(xml)
        rules = []
        for rule_el in root.findall(f'{{{NS}}}Rule'):
            rules.append(rule_el.find(f'{{{NS}}}ID').text)
        return rules

    def remove_rule(self, bucket_name: str, rule_id: str) -> dict:
        xml = self.get_lifecycle(bucket_name)
        root = ET.fromstring(xml) if xml else Element('LifecycleConfiguration', xmlns=NS)
        for rule_el in root.findall(f'{{{NS}}}Rule'):
            rid_el = rule_el.find(f'{{{NS}}}ID')
            if rid_el is not None and rid_el.text == rule_id:
                root.remove(rule_el)
                break
        else:
            return {'error': 'rule not found', 'name': rule_id}
        self._put_lifecycle(bucket_name, root)
        return {'removed': rule_id}

    def create_rule(
        self,
        name: str,
        days: int = None,
        years: int = None,
        noncurrent: bool = False,
        delete_marker: bool = False,
        prefix: str = None,
        status: str = 'Enabled',
        tag: str = None
    ) -> dict:
        self.rules[name] = {
            'name': name,
            'days': days,
            'years': years,
            'noncurrent': noncurrent,
            'delete_marker': delete_marker,
            'prefix': prefix,
            'status': status,
            'tag': tag
        }
        return self.rules[name]

    def apply_rule(self, bucket_name: str, rule_name: str) -> dict:
        rule = self.rules.get(rule_name)
        if not rule:
            return {'error': 'rule not found', 'name': rule_name}
        if rule.get('delete_marker'):
            return self.apply_delete_marker_lifecycle(bucket_name, rule_id=rule_name)
        days = rule.get('days') or 0
        return self.apply_expiration_lifecycle(bucket_name, days=days, rule_id=rule_name)

    def apply_delete_marker_lifecycle(self, bucket_name: str, rule_id: str = None) -> dict:
        xml = self.get_lifecycle(bucket_name)
        root = ET.fromstring(xml) if xml else Element('LifecycleConfiguration', xmlns=NS)
        rule = SubElement(root, 'Rule')
        SubElement(rule, 'ID').text = rule_id or 'remove-expired-markers'
        SubElement(rule, 'Filter')
        SubElement(rule, 'Status').text = 'Enabled'
        exp = SubElement(rule, 'Expiration')
        SubElement(exp, 'ExpiredObjectDeleteMarker').text = 'true'
        return self._put_lifecycle(bucket_name, root)

    def apply_expiration_lifecycle(self, bucket_name: str, days: int, rule_id: str = None) -> dict:
        xml = self.get_lifecycle(bucket_name)
        root = ET.fromstring(xml) if xml else Element('LifecycleConfiguration', xmlns=NS)
        rule = SubElement(root, 'Rule')
        SubElement(rule, 'ID').text = rule_id or f'expire-after-{days}-days'
        SubElement(rule, 'Filter')
        SubElement(rule, 'Status').text = 'Enabled'
        exp = SubElement(rule, 'Expiration')
        SubElement(exp, 'Days').text = str(days)
        return self._put_lifecycle(bucket_name, root)

    def _put_lifecycle(self, bucket_name: str, root: Element) -> dict:
        raw = tostring(root, encoding='utf-8')
        body = b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n' + raw
        digest = hashlib.md5(body).digest()
        md5_b64 = base64.b64encode(digest).decode('utf-8')
        headers = {
            'Content-Type': 'application/xml',
            'Content-MD5': md5_b64,
            'Content-Length': str(len(body))
        }
        signed_headers, url = self.auth.sign(
            'PUT', bucket=bucket_name, subresource='?lifecycle', headers=headers, payload=body
        )
        resp = requests.put(url, headers=signed_headers, data=body)
        resp.raise_for_status()
        return {'success': True}

    def apply_lifecycle_with_xml(self, bucket_name: str, xml_body: bytes) -> requests.Response:
        """Apply a full lifecycle XML body directly to the bucket."""
        digest = hashlib.md5(xml_body).digest()
        md5_b64 = base64.b64encode(digest).decode('utf-8')
        headers = {
            'Content-Type': 'application/xml',
            'Content-MD5': md5_b64,
            'Content-Length': str(len(xml_body))
        }
        signed_headers, url = self.auth.sign(
            'PUT', bucket=bucket_name, subresource='?lifecycle', headers=headers, payload=xml_body
        )
        resp = requests.put(url, headers=signed_headers, data=xml_body)
        resp.raise_for_status()
        return resp


def build_lifecycle_with_date(rule_id: str, prefix: str, date_str: str) -> bytes:
    """
    Build XML for a LifecycleConfiguration with a Date expiration.
    """
    root = Element('LifecycleConfiguration', xmlns=NS)
    rule = SubElement(root, 'Rule')
    SubElement(rule, 'ID').text = rule_id
    flt = SubElement(rule, 'Filter')
    SubElement(flt, 'Prefix').text = prefix
    SubElement(rule, 'Status').text = 'Enabled'
    exp = SubElement(rule, 'Expiration')
    SubElement(exp, 'Date').text = date_str
    raw = tostring(root, encoding='utf-8')
    return b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n' + raw