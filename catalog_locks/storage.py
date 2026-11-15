"""分段行文件及完整性校验。"""
from pathlib import Path
import hashlib
import json
import os
import tempfile

def canonical(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')

class SegmentStore:
    def __init__(self,root):
        self.root=Path(root)
        self.bytes_read=0
        self.pages_read=[]

    def write_segment(self,name,rows,page_rows=2):
        if not name or Path(name).name!=name or page_rows<1:
            raise ValueError('段名或页大小非法')
        self.root.mkdir(parents=True,exist_ok=True)
        destination=self.root/(name+'.json')
        if destination.exists():raise ValueError('段身份不可覆盖')
        seen=set();pages=[];pending=[]
        for row in rows:
            if not isinstance(row.get('row_id'),str) or row['row_id'] in seen:
                raise ValueError('行身份缺失或重复')
            seen.add(row['row_id']);pending.append(row)
            if len(pending)==page_rows:
                pages.append(pending);pending=[]
        if pending:pages.append(pending)
        manifest={'version':1,'name':name,'pages':[]}
        for index,values in enumerate(pages):
            body=canonical(values);digest=hashlib.sha256(body).hexdigest()
            filename=f'{name}-{index}-{digest[:16]}.page'
            target=self.root/filename
            if not target.exists():
                with target.open('xb') as stream:
                    stream.write(body);stream.flush();os.fsync(stream.fileno())
            manifest['pages'].append({'file':filename,'sha256':digest,'length':len(body),'rows':len(values)})
        descriptor=canonical(manifest)
        fd,path=tempfile.mkstemp(prefix='.segment-',dir=self.root)
        try:
            with os.fdopen(fd,'wb') as stream:
                stream.write(descriptor);stream.flush();os.fsync(stream.fileno())
            os.replace(path,destination)
        finally:
            if os.path.exists(path):os.unlink(path)
        return hashlib.sha256(descriptor).hexdigest()

    def manifest(self,name):
        if Path(name).name!=name:raise ValueError('段身份非法')
        data=json.loads((self.root/(name+'.json')).read_text(encoding='utf-8'))
        if data['version']!=1 or data['name']!=name:raise ValueError('段描述不兼容')
        return data

    def read_page(self,name,index):
        meta=self.manifest(name)['pages'][index]
        if Path(meta['file']).name!=meta['file']:raise ValueError('页路径非法')
        body=(self.root/meta['file']).read_bytes()
        self.bytes_read+=len(body);self.pages_read.append((name,index))
        if len(body)!=meta['length'] or hashlib.sha256(body).hexdigest()!=meta['sha256']:
            raise ValueError('行页校验失败')
        result=json.loads(body)
        if len(result)!=meta['rows']:raise ValueError('页行数不一致')
        return result

    def scan(self,name,columns=None):
        count=len(self.manifest(name)['pages'])
        for index in range(count):
            for row in self.read_page(name,index):
                if columns is None:yield row
                else:yield {key:row[key] for key in columns if key in row}

    def fingerprint(self,name):
        return hashlib.sha256(canonical(self.manifest(name))).hexdigest()
