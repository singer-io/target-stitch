import argparse
import io
import sys
import time

from stitchclient.client import Client
from transit.reader import Reader
from transit.writer import Writer

_writer = Writer(sys.stdout, "json")

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
    for line in sys.stdin:
        if line == '--\n':
            break

def main(args):
    parser = argparse.ArgumentParser(prog="Stitch Persister")
    parser.add_argument('-T', '--token', required=True, help='Stitch API token')
    parser.add_argument('-C', '--cid', required=True, type=int, help='Stitch Client ID')
    args = parser.parse_args(args)

    last_bookmark = None
    def persist_bookmark(vals):
        _writer.write(last_bookmark)

    with Client(args.cid, args.token, callback_function=persist_bookmark) as stitch:
        parse_headers()
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
                    last_bookmark = o
                else:
                    pass
        except EOFError as e:
            pass


if __name__=="__main__":
    print("Running")
