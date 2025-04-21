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
            rules.append({
                'ID': rule_el.find(f'{{{NS}}}ID').text,
                'Status': rule_el.find(f'{{{NS}}}Status').text
            })
        return rules

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

    def purge_expired(self, bucket_name: str) -> dict:
        return self.apply_delete_marker_lifecycle(bucket_name)

    def apply_delete_marker_lifecycle(self, bucket_name: str, rule_id: str = None) -> dict:
        existing = self.get_lifecycle(bucket_name)
        if existing:
            root = ET.fromstring(existing)
        else:
            root = Element('LifecycleConfiguration', xmlns=NS)
        rule = SubElement(root, 'Rule')
        SubElement(rule, 'ID').text = rule_id or 'remove-expired-markers'
        SubElement(rule, 'Filter')
        SubElement(rule, 'Status').text = 'Enabled'
        exp = SubElement(rule, 'Expiration')
        SubElement(exp, 'ExpiredObjectDeleteMarker').text = 'true'
        raw_xml = tostring(root, encoding='utf-8')
        xml_decl = b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        body = xml_decl + raw_xml
        digest = hashlib.md5(body).digest()
        md5_b64 = base64.b64encode(digest).decode('utf-8')
        headers = {
            'Content-Type': 'application/xml',
            'Content-MD5': md5_b64,
            'Content-Length': str(len(body))
        }
        signed_headers, url = self.auth.sign(
            'PUT',
            bucket=bucket_name,
            subresource='?lifecycle',
            headers=headers,
            payload=body
        )
        resp = requests.put(url, headers=signed_headers, data=body)
        resp.raise_for_status()
        return {'success': True}

    def apply_expiration_lifecycle(self, bucket_name: str, days: int, rule_id: str = None) -> dict:
        existing = self.get_lifecycle(bucket_name)
        if existing:
            root = ET.fromstring(existing)
        else:
            root = Element('LifecycleConfiguration', xmlns=NS)
        rule = SubElement(root, 'Rule')
        SubElement(rule, 'ID').text = rule_id or f'expire-after-{days}-days'
        SubElement(rule, 'Filter')
        SubElement(rule, 'Status').text = 'Enabled'
        exp = SubElement(rule, 'Expiration')
        SubElement(exp, 'Days').text = str(days)
        raw_xml = tostring(root, encoding='utf-8')
        xml_decl = b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        body = xml_decl + raw_xml
        digest = hashlib.md5(body).digest()
        md5_b64 = base64.b64encode(digest).decode('utf-8')
        headers = {
            'Content-Type': 'application/xml',
            'Content-MD5': md5_b64,
            'Content-Length': str(len(body))
        }
        signed_headers, url = self.auth.sign(
            'PUT',
            bucket=bucket_name,
            subresource='?lifecycle',
            headers=headers,
            payload=body
        )
        resp = requests.put(url, headers=signed_headers, data=body)
        resp.raise_for_status()
        return {'success': True}
