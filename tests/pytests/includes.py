
import sys
import os
from RLTest import Defaults
import platform

if sys.version_info > (3, 0):
    Defaults.decode_responses = True


UNSTABLE = os.getenv('UNSTABLE', '0') == '1'
SANITIZER = os.getenv('SANITIZER', '')
CLUSTER = os.getenv('REDIS_STANDALONE', '1') == '0'
VALGRIND = os.getenv('VALGRIND', '0') == '1'
CODE_COVERAGE = os.getenv('CODE_COVERAGE', '0') == '1'
NO_LIBEXT = os.getenv('NO_LIBEXT', '0') == '1'
CI = os.getenv('CI', '') != ''
GHA = os.getenv('GITHUB_ACTIONS', '') != ''
TEST_DEBUG = os.getenv('TEST_DEBUG', '0') == '1'
REJSON = os.getenv('REJSON', '0') == '1'

system=platform.system()
OS =  'macos' if system == 'Darwin' else system

