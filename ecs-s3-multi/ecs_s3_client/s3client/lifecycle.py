from dataclasses import dataclass
from xml.etree.ElementTree import Element, SubElement, tostring
import xml.etree.ElementTree as ET
import requests, hashlib, base64

NS = "http://s3.amazonaws.com/doc/2006-03-01/"

@dataclass
class LifecycleRuleModel:
    id: str
    status: str
    days: int = None
    expired_object_delete_marker: bool = False
    prefix: str = None

class LifecycleManager:
    def __init__(self, auth):
        self.auth = auth
        self._rules = {}

    def create_rule(self, name: str, days: int=None, expired_object_delete_marker: bool=False, prefix: str=None, status: str='Enabled') -> LifecycleRuleModel:
        rule = LifecycleRuleModel(id=name, status=status, days=days, expired_object_delete_marker=expired_object_delete_marker, prefix=prefix)
        self._rules[name] = rule
        return rule

    def get_rules(self, bucket_name: str) -> list:
        hdrs, url = self.auth.sign('GET', bucket=bucket_name, subresource='?lifecycle')
        r = requests.get(url, headers=hdrs)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        root = ET.fromstring(r.text)
        out = []
        for el in root.findall(f'{{{NS}}}Rule'):
            rid = el.find(f'{{{NS}}}ID').text
            st  = el.find(f'{{{NS}}}Status').text
            d_el = el.find(f'{{{NS}}}Expiration/{{{NS}}}Days')
            days = int(d_el.text) if d_el is not None else None
            m_el = el.find(f'{{{NS}}}Expiration/{{{NS}}}ExpiredObjectDeleteMarker')
            em  = (m_el.text.lower()=='true') if m_el is not None else False
            p_el = el.find(f'{{{NS}}}Filter/{{{NS}}}Prefix')
            pf  = p_el.text if p_el is not None else None
            out.append(LifecycleRuleModel(id=rid, status=st, days=days, expired_object_delete_marker=em, prefix=pf))
        return out

    def apply_rule(self, bucket_name: str, rule: LifecycleRuleModel) -> None:
        # Récupère config existante
        hdrs, url = self.auth.sign('GET', bucket=bucket_name, subresource='?lifecycle')
        r = requests.get(url, headers=hdrs)
        root = ET.fromstring(r.text) if r.status_code==200 else Element('LifecycleConfiguration', xmlns=NS)
        # Ajout de la règle
        el = SubElement(root,'Rule')
        SubElement(el,'ID').text = rule.id
        SubElement(el,'Filter')
        SubElement(el,'Status').text = rule.status
        exp = SubElement(el,'Expiration')
        if rule.expired_object_delete_marker:
            SubElement(exp,'ExpiredObjectDeleteMarker').text='true'
        elif rule.days is not None:
            SubElement(exp,'Days').text = str(rule.days)
        body = b'<?xml version="1.0"?>' + tostring(root)
        md5 = base64.b64encode(hashlib.md5(body).digest()).decode()
        hdr2 = {'Content-Type':'application/xml','Content-MD5':md5,'Content-Length':str(len(body))}
        sh, url2 = self.auth.sign('PUT', bucket=bucket_name, subresource='?lifecycle', headers=hdr2, payload=body)
        r2 = requests.put(url2, headers=sh, data=body); r2.raise_for_status()
