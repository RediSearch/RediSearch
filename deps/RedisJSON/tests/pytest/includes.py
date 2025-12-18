
import sys
import os
from RLTest import Defaults

Defaults.decode_responses = True

try:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../deps/readies"))
    import paella
except:
    pass

ONLY_STABLE = os.getenv('ONLY_STABLE', '0') == '1'
SANITIZER = os.getenv('SANITIZER', '')
VALGRIND = os.getenv('VALGRIND', '0') == '1'
CODE_COVERAGE = os.getenv('CODE_COVERAGE', '0') == '1'

OSNICK = paella.Platform().osnick
