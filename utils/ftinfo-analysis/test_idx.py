import os
import sys
import time
import json
import redis
import random
import datetime
import itertools
# import subprocess


################################################################################
# Auxiliary functions
################################################################################
def PrintTimeStamp():
    now = datetime.datetime.now()
    print ("Current date and time : ")
    print (now.strftime("%Y-%m-%d %H:%M:%S"))

def JsonPretty(json_data):
    json_formatted_str = json.dumps(json_data, indent=2)
    print(json_formatted_str)

def waitForIndex(r, idx):
    i = 0
    while True:
        res = r.execute_command("ft.info", idx)
        if int(res[res.index(b'indexing') + 1]) == 0:
            break
        time.sleep(1)
        i += 1
    print("Indexing time (s):", i)

def print_ps_memory_usage():
    ps_cmd = "ps -eo rss,comm | grep redis-server"
    returned_value = os.system(ps_cmd)  # returns the exit code in unix
    # ps_cmd = ["ps", "-eo", "rss", ",", "comm", "|", "grep", "redis-server"]
    # res = subprocess.run(ps_cmd, capture_output=True)
    # print(res)
    # print(res.stdout)

def print_info_memory_usage(info, title):
    print("-------------------------------------")
    print(title)
    print("-------------------------------------")
    used_memory_mb = float(info['used_memory']) / (1024*1024)
    used_memory_rss_b = float(info['used_memory_rss'])
    used_memory_rss_kb = used_memory_rss_b / (1024)
    used_memory_rss_mb = float(info['used_memory_rss']) / (1024*1024)
    mem_fragmentation_mb = float(info['mem_fragmentation_bytes']) / (1024*1024)
    used_memory_dataset = float(info['used_memory_dataset']) / (1024*1024)
    mem_fragmentation_ratio = float(info['mem_fragmentation_ratio']) / (1024*1024)
    lazyfree_pending_objects = int(info['lazyfree_pending_objects'])
    allocator_frag_ratio = float(info['allocator_frag_ratio'])
    allocator_allocated = float(info['allocator_allocated'])
    allocator_frag_bytes = float(info['allocator_frag_bytes'])
    allocator_rss_ratio = float(info['allocator_rss_ratio'])

    if(used_memory_rss_b < 0x100000):
        print('used_memory_rss b:      ', '{:12.3f}'.format(used_memory_rss_b))

    print('used_memory_rss mb:     ', '{:12.3f}'.format(used_memory_rss_mb))
    print('used_memory mb:         ', '{:12.3f}'.format(used_memory_mb))
    print('mem_fragmentation_mb:   ', '{:12.3f}'.format(mem_fragmentation_mb))
    # print('used_memory_peak mb:    ', '{:12.3f}'.format(used_memory_peak_mb))
    print('used_memory_dataset mb: ', '{:12.3f}'.format(used_memory_dataset))
    print('mem_fragmentation_ratio:', '{:6.3e}'.format(mem_fragmentation_ratio))
    print('allocator_frag_ratio:   ', '{:6.3e}'.format(allocator_frag_ratio))
    print('lazyfree_pending_objects:', '{:6}'.format(lazyfree_pending_objects))
    print("-------------------------------------")
    return info

def generate_random_hset_lat_long(id):
    # Latitude: -90 to 90
    lat = random.uniform(-90, 90)
    # Longitude: -180 to 180
    long = random.uniform(-180, 180)
    point = str(lat) + "," + str(long)
    return ("n:" + id, "n", point)

def generate_random_hset_geoshape(id):
    x = random.uniform(1, 10)
    y = random.uniform(1, 10)
    w = random.uniform(1, 100)
    return ("n:" + id, "n", "POLYGON(({x} {y}, 1 100, {w} {w}, 100 1, {x} {y}))")

def generate_hset_num(id):
    return ("n:" + id, "n", id)

# def add_hashes(r, num_strings, pipeline, datatype):
#     print("Start Adding Hashes ...")

#     for i in range(num_strings):
#         pipe = r.pipeline()
#         for j in range(pipeline):
#             id = '' . join([str(random.randint(0,9)) for _ in range(11)])
#             match datatype:
#                 case "geo":
#                     hset_cmd = generate_random_hset_lat_long(id)
#                 case "geoshape":
#                     hset_cmd = generate_random_hset_geoshape(id)
#                 case _:
#                     hset_cmd = generate_hset_num(id)
#             pipe.hset(*hset_cmd)
#         pipe.execute()

def add_hashes(r, num_strings, pipeline, datatype):
    print("Start Adding Hashes ...")

    for i in range(int(num_strings/pipeline)):
        pipe = r.pipeline()
        for j in range(pipeline):
            id = '' . join([str(random.randint(0,9)) for _ in range(11)])
            if datatype == "geo":
                hset_cmd = generate_random_hset_lat_long(id)
            elif datatype == "geoshape":
                hset_cmd = generate_random_hset_geoshape(id)
            else:
                hset_cmd = generate_hset_num(id)
            pipe.hset(*hset_cmd)
        pipe.execute()

    print("Done Adding Hashes. Total hashes:", num_strings * pipeline)

# def datatype_to_idxtype(datatype):
#     match datatype:
#         case "tag":
#             return "TAG"
#         case "txt":
#             return "TEXT"
#         case "num":
#             return "NUMERIC"
#         case "geo":
#             return "GEO"
#         case "geoshape":
#             return "GEOSHAPE FLAT"
#         case _:
#             return "TAG"

def datatype_to_idxtype(datatype):
    if datatype == "tag":
        return "TAG"
    elif datatype == "tagwithsuffixtrie":
        return "TAG WITHSUFFIXTRIE"
    elif datatype == "txt":
        return "TEXT"
    elif datatype == "txtwithsuffixtrie":
        return "TEXT WITHSUFFIXTRIE"
    elif datatype == "num":
        return "NUMERIC"
    elif datatype == "geo":
        return "GEO"
    elif datatype == "geoshape":
        return "GEOSHAPE FLAT"
    else:
        return "TAG"

################################################################################
# Main
# arg0 : Type of index (tag, txt, num, geo)
# arg1 : Number of strings
# arg2 : 1 = sortable, 0 = not sortable
#
# The script will create a number of strings with a random number of digits
# and will add them to the index.
# The number of hashes created will be arg1 * 1000
################################################################################

def main(args):
    if(len(args) < 2):
        print("Usage: python3 test_idx.py <type> <num_strings> <sortable>")
        print("       type: tag, txt, num, geo, geoshape")
        print("       num_strings: number of strings to add to the index")
        print("       1 = sortable, 0 = not sortable")
              
        sys.exit(1)

    #
    # Connnect to Redis
    #
    hostname = 'localhost'
    port=6379
    #password = 'applejack'

    r = redis.Redis(
        host=hostname,
        port=port)

    r.ping()
    # PrintTimeStamp()
    info = r.execute_command("info")
    print_info_memory_usage(info, "Initial status")

    # Adding Hashes
    datatype = args[1]
    num_strings = int(args[2])
    pipeline = 1000
    add_hashes(r, num_strings, pipeline, datatype)

    info = r.execute_command("info")
    print_info_memory_usage(info, "Before Indexing")
    beforeidx_used_memory_b = float(info['used_memory'])
    beforeidx_used_memory_mb = float(info['used_memory']) / (1024*1024)
    beforeidx_used_memory_rss_b = float(info['used_memory_rss'])
    beforeidx_used_memory_rss_kb = beforeidx_used_memory_rss_b / (1024)
    beforeidx_used_memory_rss_mb = beforeidx_used_memory_rss_kb / 1024


    print("Generate Index ...")
    cmd = 'FT.CREATE idx PREFIX 1 n: SCHEMA n ' + datatype_to_idxtype(datatype)
    if(len(args) > 3):
        sortable = args[3]
        if(sortable == '1'):
            cmd += ' SORTABLE'
    print(cmd)
    ok = r.execute_command(cmd)
    print(ok)
    # PrintTimeStamp()
    waitForIndex(r, 'idx')


    # r.execute_command('FT.DEBUG GC_FORCEINVOKE idx')
    # time.sleep(60)

    info = r.execute_command("info")
    print_info_memory_usage(info, "After Indexing")
    afteridx_used_memory_b = float(info['used_memory'])
    afteridx_used_memory_mb = float(info['used_memory']) / (1024*1024)
    afteridx_used_memory_rss_b = float(info['used_memory_rss'])
    afteridx_used_memory_rss_kb = float(info['used_memory_rss']) / 1024
    afteridx_used_memory_rss_mb = afteridx_used_memory_rss_kb / 1024
    # print('afteridx_used_memory_rss kb: ', afteridx_used_memory_rss_kb)
    # print_ps_memory_usage()

    mem_fragmentation_mb = float(info['mem_fragmentation_bytes']) / (1024*1024)


    # Printo Index Info
    ft_info = r.execute_command("ft.info idx")
    d = dict(itertools.zip_longest(*[iter(ft_info)] * 2, fillvalue=""))
    inverted_sz_mb = float(d[b'inverted_sz_mb'])
    vector_index_sz_mb = float(d[b'vector_index_sz_mb'])
    offset_vectors_sz_mb = float(d[b'offset_vectors_sz_mb'])
    doc_table_size_mb = float(d[b'doc_table_size_mb'])
    sortable_values_size_mb = float(d[b'sortable_values_size_mb'])
    key_table_size_mb = float(d[b'key_table_size_mb'])
    index_total_mb = (inverted_sz_mb + vector_index_sz_mb + offset_vectors_sz_mb +
        doc_table_size_mb + sortable_values_size_mb + key_table_size_mb)
    # print(f'(Raz) Old memory: {index_total_mb}')
    index_total_mb = float(d[b'total_index_memory_sz_mb'])
    # print(f'(Raz) New memory: {index_total_mb}')
    

    # if(index_total_mb > 1):
    print('inverted_sz_mb         :', '{:12.3f}'.format(inverted_sz_mb))
    print('vector_index_sz_mb     :', '{:12.3f}'.format(vector_index_sz_mb))
    print('offset_vectors_sz_mb   :', '{:12.3f}'.format(offset_vectors_sz_mb))
    print('doc_table_size_mb      :', '{:12.3f}'.format(doc_table_size_mb))
    print('sortable_values_size_mb:', '{:12.3f}'.format(sortable_values_size_mb))
    print('key_table_size_mb      :', '{:12.3f}'.format(key_table_size_mb))
    print('index_total_mb         :', '{:12.3f}'.format(index_total_mb))
    # else:
    #     print('inverted_sz bytes         :', '{:12.3f}'.format(inverted_sz_mb * 1024 * 1024))
    #     print('vector_index_sz bytes     :', '{:12.3f}'.format(vector_index_sz_mb * 1024 * 1024))
    #     print('offset_vectors_sz bytes   :', '{:12.3f}'.format(offset_vectors_sz_mb * 1024 * 1024))
    #     print('doc_table_size bytes      :', '{:12.3f}'.format(doc_table_size_mb * 1024 * 1024))
    #     print('sortable_values_size bytes:', '{:12.3f}'.format(sortable_values_size_mb * 1024 * 1024))
    #     print('key_table_size bytes      :', '{:12.3f}'.format(key_table_size_mb * 1024 * 1024))
    #     print('index_total bytes         :', '{:12.3f}'.format(index_total_mb  * 1024 * 1024))

    print("-------------------------------------")
    # delta_rss_b = afteridx_used_memory_rss_b - beforeidx_used_memory_rss_b
    # if(delta_rss_b < 0x100000):
    #     print('delta_rss_b         :', '{:12.3f}'.format(delta_rss_b))

    delta_rss_mb = afteridx_used_memory_rss_mb - beforeidx_used_memory_rss_mb
    print('delta_rss_mb        :', '{:12.3f}'.format(delta_rss_mb))

    # delta_used_memory_b = afteridx_used_memory_b - beforeidx_used_memory_b
    # if(delta_used_memory_b < 0x100000):
    #     print('delta_used_memory_b :', '{:12.3f}'.format(delta_used_memory_b))

    delta_used_memory_mb = afteridx_used_memory_mb - beforeidx_used_memory_mb
    print('delta_used_memory_mb:', '{:12.3f}'.format(delta_used_memory_mb))

    # print difference between delta_rss_mb and total_mb
    print('(delta_rss_mb - index_total_mb) : ', '{:12.3f}'.format(delta_rss_mb - index_total_mb))

    # print percentage difference between delta_rss_mb and total_mb
    print("abs(delta_rss_mb):", abs(delta_rss_mb))
    # if(abs(delta_rss_b) < 0x100000):
    #     print("abs(delta_rss_b):", abs(delta_rss_b))
    if(abs(delta_rss_mb) > 0):
        delta_rss_percentual = ((delta_rss_mb - index_total_mb) / delta_rss_mb) * 100
        print('((delta_rss_mb - index_total_mb)/delta_rss_mb)*100 : ', '{:6.2f}%'.format(delta_rss_percentual))

        # print difference between delta_rss_mb minus mem_fragmentation_mb and total_mb
        # delta_rss_defrag_mb = delta_rss_mb - mem_fragmentation_mb
        # delta_rss_defrag_percentual = (delta_rss_defrag_mb - index_total_mb)/delta_rss_defrag_mb * 100

    print("abs(delta_used_memory_mb): ", abs(delta_used_memory_mb))
    # if(abs(delta_used_memory_b) < 0x100000):
    #     print("abs(delta_used_memory_b): ", abs(delta_used_memory_b))
    if(abs(delta_used_memory_mb) > 0):
        delta_used_memory_percentual = ((delta_used_memory_mb - index_total_mb) / delta_used_memory_mb) * 100
        print('((delta_used_memory_mb - index_total_mb)/delta_used_memory_mb)*100 : ', '{:6.2f}%'.format(delta_used_memory_percentual))

        # print difference between delta_rss_mb minus mem_fragmentation_mb and total_mb
        # delta_rss_defrag_mb = delta_rss_mb - mem_fragmentation_mb
        # delta_rss_defrag_percentual = (delta_rss_defrag_mb - index_total_mb)/delta_rss_defrag_mb * 100

    # print("-------------------------------------")
    # print(d.keys()) 
    # print(d.values())
    # print(ftinfo)
    # print("-------------------------------------")

    print("Hashes added:", num_strings * pipeline)

    print("FLUSHALL SYNC ...")
    r.execute_command('FLUSHALL SYNC')

if __name__ == "__main__":
    main(sys.argv)
