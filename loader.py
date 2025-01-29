import pandas as pd
import redis
import subprocess
import time
import random

raw_data = pd.read_csv('data.csv')

r = redis.Redis()

disk = subprocess.Popen(['/home/guy/Code/PoC-Disk/build/main'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)

r.flushall()
r.execute_command('FT.CREATE', 'idx', 'ON', 'HASH',
                  'NOHL', 'NOOFFSETS', 'NOFREQS', 'NOFIELDS',
                  'STOPWORDS', '0',
                  'SCHEMA',
                  'abstract', 'TEXT', 'NOSTEM',
                  'KEY', 'TEXT', 'SORTABLE', 'UNF', 'NOINDEX')

total_redis_time = 0
total_disk_time = 0
global_start = time.time()
for index, row in raw_data.iterrows():
    if index > 10000:
        break

    s = time.time()
    r.hset(row['KEY'], mapping={'abstract': row['abstract'], 'KEY': row['KEY']})
    redis_time = time.time() - s

    s = time.time()
    disk.stdin.write(f"SET {row['KEY']} {row['abstract']}\n".encode())
    disk.stdin.flush()
    disk.stdout.readline().decode().strip()
    disk_time = time.time() - s

    # summing up the times in seconds
    total_redis_time += redis_time
    total_disk_time += disk_time
    cur_redis_avg = total_redis_time/(index+1)
    cur_disk_avg = total_disk_time/(index+1)
    # printing the times in milliseconds
    redis_time *= 1000
    disk_time *= 1000
    cur_disk_avg *= 1000
    cur_redis_avg *= 1000
    print('Redis', f'{redis_time:#7.7f}', f'({cur_redis_avg:#7.5f})', 'Disk', f'{disk_time:#7.7f}', f'({cur_disk_avg:#7.5f})', end='\r', sep='\t')

global_time = time.time() - global_start
print()
print('Total time:', global_time, f'(overhead: {global_time - total_redis_time - total_disk_time})')
print('Total Redis time:', total_redis_time)
print('Total Disk time:', total_disk_time)

for index, row in raw_data.iterrows():
    if index > 10000:
        break
    options = row['abstract'].split()
    random.shuffle(options)
    term = None
    for option in options:
        if option.isalpha():
            term = option
            break
    if term is None:
        continue
    redis_query = ('FT.AGGREGATE', 'idx', term, 'LOAD', '1', '@KEY')
    disk_query = f"SEARCH {term}\n"

    s = time.time()
    redis_res = r.execute_command(*redis_query)
    redis_time = time.time() - s

    s = time.time()
    disk.stdin.write(disk_query.encode())
    disk.stdin.flush()
    disk_res = disk.stdout.readline()
    disk_time = time.time() - s

    # parsing the results
    disk_res = disk_res.decode().strip().split()[3:]
    redis_res = [res[1].decode() for res in redis_res[1:]]

    # print('Redis')
    # print(f"result ({len(redis_res)}):", redis_res)
    # print('time:', redis_time)
    # print('Disk')
    # print("result:", disk_res)
    # print(f"result ({len(disk_res)}):", disk_res)
    # print('time:', disk_time)

    print('Redis', f'{redis_time:#7.7f}', 'Disk', f'{disk_time:#7.7f}', 'same:', set(redis_res) == set(disk_res), end=' ')
    print() if set(redis_res) == set(disk_res) else print('redis:', len(redis_res), 'disk:', len(disk_res))

    # if set(redis_res) != set(disk_res):
    #     print('Redis')
    #     print(f"result ({len(redis_res)}):")
    #     print('Disk')
    #     print(f"result ({len(disk_res)}):")
    #     print('diff:', set(redis_res) - set(disk_res))
    #     print('term:', term)
    #     break
