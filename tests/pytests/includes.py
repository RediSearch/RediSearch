
import sys
import os
from RLTest import Defaults

if sys.version_info > (3, 0):
    Defaults.decode_responses = True

try:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../deps/readies"))
    import paella
except:
    pass

UNSTABLE_TESTS = os.getenv('UNSTABLE_TESTS', '0') == '1'
IS_SANITIZER = os.getenv('SANITIZER', '0') == '1'
IS_CODE_COVERAGE = os.getenv('CODE_COVERAGE', '0') == '1'
