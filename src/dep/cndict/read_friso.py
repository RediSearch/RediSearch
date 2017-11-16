#!/usr/bin/env python
import zlib
import struct
from argparse import ArgumentParser
from cStringIO import StringIO

ap = ArgumentParser()
ap.add_argument('-f', '--file', default='CNDICT.out')

opts = ap.parse_args()
fp = open(opts.file)

# Read the header/version
version = struct.unpack('!I', fp.read(4))[0]
print "VERSION", version

TYPE_MASK = 0x1F
F_SYNS = 0x01 << 5
F_FREQS = 0x02 << 5


def print_header(hdrbyte):
    print "Type: {0}. Has Syns={1}, Has Freqs={2}".format(
        hdrbyte & TYPE_MASK,
        bool(hdrbyte & F_SYNS),
        bool(hdrbyte & F_FREQS)
    )


def read_zstr(fp):
    ret = bytearray()
    while True:
        s = fp.read(1)
        if len(s) == 0 or ord(s) == 0:
            return ret.decode('utf-8')
        ret += s


def read_entry(fp):
    firstbyte = fp.read(1)
    if len(firstbyte) == 0:
        raise EOFError()

    hdrinfo = ord(firstbyte)
    print_header(hdrinfo)
    # Read up to the first buf
    term = read_zstr(fp)
    syns = []
    freqs = 0
    if hdrinfo & F_SYNS:
        # Check the number of syns we're to read
        syncount = struct.unpack("!h", fp.read(2))[0]
        for _ in range(syncount):
            syns.append(read_zstr(fp))
    if hdrinfo & F_FREQS:
        freqs = struct.unpack("!I", fp.read(4))[0]

    return term, syns, freqs

sio = StringIO(zlib.decompress(fp.read()))
while True:
    term, syns, freqs = read_entry(sio)
    print term, freqs