# Creates count of keys from a document
import argparse
from urlparse import urlparse
import os
import redis

# http://code.activestate.com/recipes/577081-humanized-representation-of-a-number-of-bytes/#c7
def GetHumanReadable(size, precision=2):
    suffixes = ['B ', 'KB', 'MB', 'GB', 'TB', 'PB', 'ZB']
    suffixIndex = 0
    while size > 1024:
        suffixIndex += 1  # increment the index of the suffix
        size = size / 1024.0  # apply the division
    fmt = '{{:4.{}f}} {{}}'.format(precision)
    return fmt.format(size, suffixes[suffixIndex])

if __name__ == '__main__':
    # handle arguments
    parser = argparse.ArgumentParser(description='ReJSON memory profiler', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('file', type=str, default=None, help='JSON filename')
    parser.add_argument('-u', '--uri', type=str, default=None, help='Redis server URI')
    parser.add_argument('-s', '--steps', type=int, default=5, help='number of steps')
    parser.add_argument('-c', '--count', type=int, default=1, help='initial count of documents')
    args = parser.parse_args()
    if args.uri is not None:
        uri = urlparse(args.uri)
    else:
        uri = None

    # initialize
    port = None
    serverpath = os.path.abspath(os.path.join(os.getcwd(), '../../redis/src/redis-server'))
    serverargs = {
        'loadmodule': os.path.abspath(os.path.join(os.getcwd(), '../lib/rejson.so')),
        'save': '',
    }

    with open(args.file) as f:
        json = f.read()
    
    count = args.count
    info = {
        'before': [],
        'after': [],
    }

    # TODO make client connect to an existing instance
    # client = DisposableRedis(port=port, path=serverpath, **serverargs)
    r = redis.StrictRedis()

    # Print file and ReJSON sizes
    r.execute_command('JSON.SET', 'json', '.', json)
    print 'File size: {}'.format(GetHumanReadable(len(json)))
    print 'As ReJSON: {}'.format(GetHumanReadable(r.execute_command('JSON.MEMORY', 'json')))
    print

    # do the steps
    print '| Step | Documents | Dataset memory | Server memory |'
    print '| ---- | --------- | -------------- | ------------- |'
    for i in range(0, args.steps):
        # with client as r:
            # create documents
            info['before'].append(r.info(section='memory'))
            for j in range(0, count):
                r.execute_command('JSON.SET', 'json:{}:{}'.format(i, j), '.', json)
            info['after'].append(r.info(section='memory'))

            # print report
            row = [i + 1, count,
                GetHumanReadable(info['after'][-1]['used_memory_dataset'] - info['before'][-1]['used_memory_dataset']),
                GetHumanReadable(info['after'][-1]['used_memory'] - info['before'][-1]['used_memory'])]
            fmt = '| {{:>4}} | {{:>{}}} | {{:>14}} | {{:>13}} |'.format(max(args.steps, 9))
            print fmt.format(*row)

            # wrap up
            count = count * 10
    print
