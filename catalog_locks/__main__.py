"""临时建段并执行现有诊断。"""
import json,tempfile,argparse
from decimal import Decimal
from .storage import SegmentStore
from .operations import describe

def main():
    parser=argparse.ArgumentParser(description='教育仓湖资料诊断');parser.add_argument('sample');args=parser.parse_args()
    with open(args.sample,encoding='utf-8') as stream:data=json.load(stream)
    with tempfile.TemporaryDirectory() as directory:
        store=SegmentStore(directory)
        for name,rows in data.items():store.write_segment(name,rows)
        result=describe(store)
        print(json.dumps(result,ensure_ascii=False,indent=2,default=lambda x:str(x) if isinstance(x,Decimal) else list(x),allow_nan=False))

if __name__=='__main__':main()
