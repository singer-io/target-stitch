import argparse
import io
import sys
import time
import json

from stitchclient.client import Client
from transit.reader import Reader
from transit.writer import Writer

# https://pymotw.com/2/codecs/
class EOFDetectorWrapper(object):
    def __init__(self, wrapped):
        self.wrapped = wrapped

    def write(self, data):
        return self.wrapped.write(data)

    def read(self, size=-1):
        data = self.wrapped.read(size)
        if data == '':
            raise EOFError()
        return data

    def flush(self):
        return self.wrapped.flush()

    def close(self):
        return self.wrapped.close()

def parse_headers():
    headers = {'version': sys.stdin.readline().strip().lower()}

    for line in sys.stdin:
        if line.strip() == '--':
            break
        (k,v) = line.split(':', 1)
        headers[k.strip().lower()] = v.strip().lower()

    return headers

def from_transit(args):
    last_bookmark = None
    def persist_bookmark(vals):
        if last_bookmark is not None:
            s = io.StringIO()
            writer = Writer(s, "json")
            writer.write(last_bookmark)
            print(s.getvalue().strip(), flush=True)

    with Client(args.cid, args.token, callback_function=persist_bookmark) as stitch:
        reader = Reader()
        try:
            for o in reader.readeach(EOFDetectorWrapper(sys.stdin)):
                if o['type'] == 'RECORD':
                    stitch.push({'action': 'upsert',
                                 'table_name': o['stream'],
                                 'key_names': o['key_fields'],
                                 'sequence': int(time.time() * 1000),
                                 'data': o['record']}, last_bookmark)
                elif o['type'] == 'BOOKMARK':
                    last_bookmark = o['value']
                else:
                    pass
        except EOFError as e:
            pass

def from_jsonline(args):
    last_bookmark = None
    def persist_bookmark(vals):
        if last_bookmark is not None:
            print(json.dumps(last_bookmark), flush=True)

    with Client(args.cid, args.token, callback_function=persist_bookmark) as stitch:
        while True:
            line = sys.stdin.readline() # `for line in stdin` won't play nice in all situations
            if line == '':
                break
            o = json.loads(line)

            if o['type'] == 'RECORD':
                stitch.push({'action': 'upsert',
                             'table_name': o['stream'],
                             'key_names': o['key_fields'],
                             'sequence': int(time.time() * 1000),
                             'data': o['record']}, last_bookmark)
            elif o['type'] == 'BOOKMARK':
                last_bookmark = o['value']
            else:
                pass

def main(args):
    parser = argparse.ArgumentParser(prog="Stitch Persister")
    parser.add_argument('-T', '--token', required=True, help='Stitch API token')
    parser.add_argument('-C', '--cid', required=True, type=int, help='Stitch Client ID')
    args = parser.parse_args(args)

    headers = parse_headers()

    if headers['content-type'] == "transit":
        from_transit(args)
    elif headers['content-type'] == "jsonline":
        from_jsonline(args)
