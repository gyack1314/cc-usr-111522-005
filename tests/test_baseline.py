import tempfile,unittest,json
from pathlib import Path
from catalog_locks.storage import SegmentStore

class Base(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name);self.store=SegmentStore(self.path/'store')
        sample=json.loads((Path(__file__).resolve().parents[1]/'data/sample.json').read_text(encoding='utf-8'))
        for name,rows in sample.items():self.store.write_segment(name,rows)
    def tearDown(self):self.tmp.cleanup()

class StorageTests(Base):
    def test_roundtrip(self):
        rows=[{'row_id':str(i),'v':i} for i in range(5)];self.store.write_segment('roundtrip',rows)
        self.assertEqual(list(self.store.scan('roundtrip')),rows);self.assertEqual(len(self.store.pages_read),3)
    def test_projection_keeps_missing(self):
        self.store.write_segment('project',[{'row_id':'a','v':None},{'row_id':'b'}]);self.assertEqual(list(self.store.scan('project',['v'])),[{'v':None},{}])
    def test_segment_immutable(self):
        self.store.write_segment('fixed',[{'row_id':'a'}]);self.assertRaises(ValueError,self.store.write_segment,'fixed',[])
    def test_page_corruption(self):
        self.store.write_segment('damaged',[{'row_id':'a'}]);meta=self.store.manifest('damaged')['pages'][0];(self.store.root/meta['file']).write_bytes(b'[]')
        self.assertRaises(ValueError,list,self.store.scan('damaged'))
    def test_duplicate_row_identity(self):self.assertRaises(ValueError,self.store.write_segment,'duplicate',[{'row_id':'a'}]*2)
    def test_fingerprint_and_no_projection_scan_skip(self):
        self.store.write_segment('identity',[{'row_id':'a','v':2}]);first=self.store.fingerprint('identity');self.assertEqual(len(first),64)
        list(self.store.scan('identity',['row_id']));self.assertGreater(self.store.bytes_read,0)

from catalog_locks.operations import Catalog
from catalog_locks.model import resource
class DomainTests(Base):
    def test_private_write(self):
        c=Catalog();c.begin('a');c.begin('b');c.stage('a',['d','t'],3);self.assertEqual(c.read('a',['d','t']),3)
        # 另一个事务读不同资源看不到 a 的私有写；同一资源上 a 的未提交 IX
        # 会阻塞 b 的 S，该阻塞语义在 test_locks 中专门验证。
        self.assertIsNone(c.read('b',['d','other']))
    def test_commit(self):
        c=Catalog();c.begin('a');c.stage('a',['d','t'],3);c.commit('a');c.begin('b');self.assertEqual(c.read('b',['d','t']),3)
    def test_rollback_has_real_steps(self):
        c=Catalog();c.begin('a');c.stage('a',['d','t'],3);it=c.rollback_steps('a');next(it);self.assertEqual(c.transactions['a'].state,'rolling_back');list(it);self.assertEqual(c.transactions['a'].state,'aborted')
    def test_resource_shape(self):self.assertRaises(ValueError,resource,['d','','p'])
