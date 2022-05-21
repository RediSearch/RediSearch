#!/usr/bin/env python

"""
This script gathers settings and dictionaries from friso (a chinese
tokenization library) and generates a C source file that can later be
compiled into RediSearch, allowing the module to have a built-in chinese
dictionary. By default this script will generate a C source file of
compressed data but there are other options to control output (mainly for
debugging).

The `read_friso` script can be used to analyze the dumped data for debugging
purposes
"""

import zlib
import errno
import os
import re
import struct
import sys
import time
import string
from argparse import ArgumentParser

# Load the ini file
ap = ArgumentParser()
ap.add_argument('-i', '--ini', default='friso/friso.ini',
                help='ini file to use for initialization')
ap.add_argument('-m', '--mode', default='c', help='output mode',
                choices=['c', 'raw_z', 'raw_u'])
ap.add_argument('-d', '--dir', default='.',
                help='Override directory of lex files')
ap.add_argument('-o', '--out', help='Name of destination directory',
                default='cndict_generated')

opts = ap.parse_args()

lexdir = opts.dir

DICT_VARNAME = 'ChineseDict'
SIZE_COMP_VARNAME = 'ChineseDictCompressedLength'
SIZE_FULL_VARNME = 'ChineseDictFullLength'


class ConfigEntry(object):
    def __init__(self, srcname, dstname, pytype):
        self.srcname = srcname
        self.dstname = dstname
        self.pytype = pytype
        self.value = None

configs = [
    ConfigEntry('max_len', 'max_len', int),
    ConfigEntry('r_name', 'r_name', int),
    ConfigEntry('mix_len', 'mix_len', int),
    ConfigEntry('lna_len', 'lna_len', int),
    ConfigEntry('add_syn', 'add_syn', int),
    ConfigEntry('clr_stw', 'clr_stw', int),
    ConfigEntry('keep_urec', 'keep_urec', int),
    ConfigEntry('spx_out', 'spx_out', int),
    ConfigEntry('nthreshold', 'nthreshold', int),
    ConfigEntry('mode', 'mode', int),
    ConfigEntry('charset', 'charset', int),
    ConfigEntry('en_sseg', 'en_sseg', int),
    ConfigEntry('st_minl', 'st_minl', int),
    ConfigEntry('kpuncs', 'kpuncs', str)
]


def write_config_init(varname, configs):
    ret = []
    for config in configs:
        if config.value is None:
            continue
        if config.srcname == 'mode':
            ret.append('friso_set_mode({},{});'.format(varname, config.value))
        elif config.dstname == 'kpuncs':
            ret.append('strcpy({}->kpuncs, "{}");'.format(varname, config.value))
        elif config.dstname == 'charset':
            pass
            # Skip
        elif config.pytype == int:
            ret.append('{}->{} = {};'.format(varname, config.dstname, config.value))
        else:
            raise ValueError("Don't understand config!", config)

    return ret


def set_key_value(name, value):
    for config in configs:
        name = name.lower().replace("friso.", "").strip()
        # print name, config.srcname
        if config.srcname == name:
            config.value = config.pytype(value)
            return
    raise ValueError('Bad config key', name)


with open(opts.ini, 'r') as fp:
    for line in fp:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        key, value = line.split('=')
        key = key.strip()
        value = value.strip()
        if key == 'friso.lex_dir':
            if not lexdir:
                lexdir = value
        else:
            set_key_value(key, value)


# Parse the header snippet in order to emit the correct constant.
_LEXTYPE_MAP_STRS = \
r'''
    __LEX_CJK_WORDS__ = 0,
    __LEX_CJK_UNITS__ = 1,
    __LEX_ECM_WORDS__ = 2,    //english and chinese mixed words.
    __LEX_CEM_WORDS__ = 3,    //chinese and english mixed words.
    __LEX_CN_LNAME__ = 4,
    __LEX_CN_SNAME__ = 5,
    __LEX_CN_DNAME1__ = 6,
    __LEX_CN_DNAME2__ = 7,
    __LEX_CN_LNA__ = 8,
    __LEX_STOPWORDS__ = 9,
    __LEX_ENPUN_WORDS__ = 10,
    __LEX_EN_WORDS__ = 11,
    __LEX_OTHER_WORDS__ = 15,
    __LEX_NCSYN_WORDS__ = 16,
    __LEX_PUNC_WORDS__ = 17,        //punctuations
    __LEX_UNKNOW_WORDS__ = 18        //unrecognized words.
'''
LEXTYPE_MAP = {}
for m in re.findall('\s*(__[^=]*__)\s*=\s*([\d]*)', _LEXTYPE_MAP_STRS):
    LEXTYPE_MAP[m[0]] = int(m[1])

# Lex type currently occupies
TYPE_MASK = 0x1F
F_SYNS = 0x01 << 5
F_FREQS = 0x02 << 5


class LexBuffer(object):
    # Size of input buffer before flushing to a zlib block
    CHUNK_SIZE = 65536
    VERSION = 0

    def __init__(self, fp, use_compression=True):
        self._buf = bytearray()
        self._fp = fp
        self._compressor = zlib.compressobj(-1)
        self._use_compression = use_compression

        # Write the file header
        self._fp.write(struct.pack("!I", self.VERSION))
        self._fp.flush()
        self.compressed_size = 0
        self.full_size = 4 # For the 'version' byte

    def _write_data(self, data):
        self._fp.write(data)
        self.compressed_size += len(data)

    def flush(self, is_final=False):
        if not self._use_compression:
            self._write_data(self._buf)
        else:
            # Flush any outstanding data in the buffer
            self._write_data(self._compressor.compress(bytes(self._buf)))

            if is_final:
                self._write_data(self._compressor.flush(zlib.Z_FINISH))

        self._fp.flush()
        self.full_size += len(self._buf)
        self._buf = bytearray()

    def _maybe_flush(self):
        if len(self._buf) > self.CHUNK_SIZE:
            self.flush()

    def add_entry(self, lextype, term, syns, freq):
        # Perform the encoding...
        header = LEXTYPE_MAP[lextype]

        if syns:
            header |= F_SYNS
        if freq:
            header |= F_FREQS

        self._buf.append(header)
        self._buf += term
        self._buf.append(0) # NUL terminator

        if syns:
            self._buf += struct.pack("!h", len(syns))
            for syn in syns:
                self._buf += syn
                self._buf.append(0)
        if freq:
            self._buf += struct.pack("!I", freq)

        self._maybe_flush()


def encode_pair(c):
    if c in string.hexdigits:
        return '\\x{0:x}'.format(ord(c))
    elif c in ('"', '\\', '?'):
        return '\\' + c
    else:
        return repr('%c' % (c,))[1:-1]
    # return '\\x{0:x}'.format(ord(c)) if _needs_escape(c) else c


class SourceEncoder(object):
    LINE_LEN = 40

    def __init__(self, fp):
        self._fp = fp
        self._curlen = 0

    def write(self, blob):
        blob = buffer(blob)
        while len(blob):
            chunk = buffer(blob, 0, self.LINE_LEN)
            blob = buffer(blob, len(chunk), len(blob)-len(chunk))
            encoded = ''.join([encode_pair(c) for c in chunk])
            self._fp.write('"' + encoded + '"\n')

        return len(blob)

    def flush(self):
        self._fp.flush()

    def close(self):
        pass


def process_lex_entry(type, file, buf):
    print type, file
    fp = open(file, 'r')
    for line in fp:
        line = line.strip()
        comps = line.split('/')
        # print comps
        term = comps[0]
        syns = comps[1].split(',') if len(comps) > 1 else []
        if len(syns) == 1 and syns[0].lower() == 'null':
            syns = []
        freq = int(comps[2]) if len(comps) > 2 else 0

        buf.add_entry(type, term, syns, freq)
        # print "Term:", term, "Syns:", syns, "Freq", freq
        # Now dump it, somehow


def strip_comment_lines(blob):
    lines = [line.strip() for line in blob.split('\n')]
    lines = [line for line in lines if line and not line.startswith('#')]
    return lines


def sanitize_file_entry(typestr, filestr):
    typestr = strip_comment_lines(typestr)[0]
    filestr = strip_comment_lines(filestr)
    filestr = [f.rstrip(';') for f in filestr]
    return typestr, filestr


lexre = re.compile(r'([^:]+)\w*:\w*\[([^\]]*)\]', re.MULTILINE)
lexindex = os.path.join(lexdir, 'friso.lex.ini')
lexinfo = open(lexindex, 'r').read()
matches = lexre.findall(lexinfo)
# print matches

dstdir = opts.out
if opts.mode == 'c':
    dstfile = 'cndict_data.c'
else:
    dstfile = 'cndict_data.out'

try:
    os.makedirs(dstdir)
except OSError as e:
    if e.errno != errno.EEXIST:
        raise

dstfile = os.path.join(dstdir, dstfile)
ofp = open(dstfile, 'w')

if opts.mode == 'c':
    ofp.write(r'''
// Compressed chinese dictionary
// Generated by {}
// at {}
#include "dep/friso/friso.h"
#include <stdlib.h>
#include <string.h>
const char {}[] =
'''.format(' '.join(sys.argv), time.ctime(), DICT_VARNAME))
    ofp.flush()

lexout = SourceEncoder(ofp)
lexbuf = LexBuffer(lexout)
for m in matches:
    typestr, filestr = sanitize_file_entry(m[0], m[1])
    # print typestr
    # print filestr
    for filename in filestr:
        filename = os.path.join(os.path.dirname(lexindex), filename)
        process_lex_entry(typestr, filename, lexbuf)

lexbuf.flush(is_final=True)

ofp.write(';\n')
ofp.write('const size_t {} = {};\n'.format(SIZE_COMP_VARNAME, lexbuf.compressed_size))
ofp.write('const size_t {} = {};\n'.format(SIZE_FULL_VARNME, lexbuf.full_size))

config_lines = write_config_init('frisoConfig', configs)
config_fn = '\n'.join(config_lines)
friso_config_txt = '''
void ChineseDictConfigure(friso_t friso, friso_config_t frisoConfig) {
'''
friso_config_txt += config_fn
friso_config_txt += '\n}\n'
ofp.write(friso_config_txt)
ofp.flush()
ofp.close()

# hdrfile = os.path.join(dstdir, 'cndict_data.h')
# hdrfp = open(hdrfile, 'w')
# hdrfp.write(r'''
#ifndef CNDICT_DATA_H
#define CNDICT_DATA_H
# extern const char {data_var}[];
# extern const size_t {uncomp_len_var};
# extern const size_t {comp_len_var};
# {config_fn_txt}
# #endif
# '''.format(
#     data_var=DICT_VARNAME,
#     uncomp_len_var=SIZE_FULL_VARNME,
#     comp_len_var=SIZE_COMP_VARNAME,
#     config_fn_txt=friso_config_txt
# ))
# hdrfp.flush()
