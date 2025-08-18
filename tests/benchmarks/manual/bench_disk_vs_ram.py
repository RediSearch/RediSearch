import redis
import time
import tqdm
import os
import argparse
import csv
import shutil
import statistics
import threading

IDX_NAME = 'idx'
IDX_NAME_DISK = 'idx-disk'

class BenchDiskVsRam(object):
    def __init__(self, num_docs=1000000, data_path='diskBenchPopulationData.csv', result_file='results.txt', iters=100, disk_files_path=None):
        self.num_docs = num_docs
        self.data_path = data_path + f'_{self.num_docs}.csv'
        self.result_file = result_file
        # Append to the result file name the number of docs used in the benchmark
        if not self.result_file.endswith('.txt'):
            self.result_file += '.txt'
        self.result_file = self.result_file.replace('.txt', f'_{self.num_docs}.txt')
        self.redis = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.redis.execute_command('hello', '3') # Set RESP3, such that we avoid incorrect result count in ft.aggregate.
        self.redis.execute_command('debug', 'mark-internal-client')
        
        # Set configurations
        self.redis.execute_command('_FT.CONFIG', 'SET', 'on_timeout', 'fail')
        self.redis.execute_command('_FT.CONFIG', 'SET', 'default_dialect', '2')
        self.redis.execute_command('_FT.CONFIG', 'SET', 'timeout', '0')
        self.redis.execute_command('_FT.CONFIG', 'SET', 'workers', '0')
        # self.redis.execute_command('CONFIG', 'SET', 'search-on-timeout', 'fail')
        # self.redis.execute_command('CONFIG', 'SET', 'search-default-dialect', '2')
        # self.redis.execute_command('CONFIG', 'SET', 'search-timeout', '10000000')
        # self.redis.execute_command('CONFIG', 'SET', 'search-workers', '0')
        self.iters = iters
        self.disk_files_path = disk_files_path

    def gen_data(self):
        """
        Generates data for the benchmark.
        In order to benchmark flows using both the regular read path, and the
        skip-to path (used in intersection), we have all documents contain a
        specific value for the field `title`, while the `description` field
        will contain a specific value that will repeat every 300 documents.
        Then, we can run regular read queries by searching on `title`, and
        skip-to queries by searching on an intersection of `title` and
        `description`.
        """
        print(f'Creating data file of {self.num_docs} documents')
        skipto_interval = 300
        start_time = time.time()
        with open(self.data_path, 'w') as f:
            for i in tqdm.tqdm(range(self.num_docs), desc='Generating data'):
                f.write(f'doc_{i},hello,description_{i % skipto_interval}\n')
        print(f'Data file created (creation time: {time.time() - start_time:.6f} seconds).')

    def create_indexes(self, index_type: str=None):
        """
        Creates a RediSearch index
        If index_type is None, creates both indexes.
        """
        try:
            if not index_type or index_type == 'disk':
                print('Creating disk index')
                # 
                self.redis.execute_command('FT.CREATE', IDX_NAME_DISK, 'NOHL', 'NOOFFSETS', 'NOFREQS', 'STOPWORDS', '0', 'SCHEMA', 'title', 'TEXT', 'NOSTEM', 'description', 'TEXT', 'NOSTEM')
            if not index_type or index_type == 'ram':
                print('Creating ram index')
                # Add the 'RAM' argument (temporarily supported for benchmarking).
                self.redis.execute_command('FT.CREATE', IDX_NAME, 'RAM', 'NOHL', 'NOOFFSETS', 'NOFREQS', 'STOPWORDS', '0', 'SCHEMA', 'title', 'TEXT', 'NOSTEM', 'description', 'TEXT', 'NOSTEM')
        except redis.exceptions.ResponseError as e:
            if str(e) != 'Index already exists':
                raise e

    def _load_from_file(self):
        """
        Loads the data from the data-file into the database.
        """
        with open(self.data_path, 'r') as f:
            batch_size = 1000
            pipe = self.redis.pipeline()
            for i, line in enumerate(tqdm.tqdm(f, total=self.num_docs, desc='Ingesting data')):
                doc_name, title, description = line.strip().split(',')
                pipe.hset(name=doc_name, mapping={
                    'title': title,
                    'description': description
                })
                if (i + 1) % batch_size == 0:
                    pipe.execute()
                    pipe = self.redis.pipeline()
            if (i + 1) % batch_size != 0:
                pipe.execute()
        assert self.redis.dbsize() == self.num_docs, f"Expected {self.num_docs} docs, found {self.redis.dbsize()}"

    def load_data(self):
        """
        Populates the database with data that corresponds to the index defined
        in `create_index`.
        """
        # Create the data, if it doesn't exist.
        if not os.path.exists(self.data_path):
            print('Data file does not exist, creating it')
            self.gen_data()
            print('Data file created.')

        # Ingest the data
        print(f'Ingesting {self.num_docs} documents')
        start_time = time.time()
        try:
            with open(self.data_path, 'r') as f:
                batch_size = 1000
                pipe = self.redis.pipeline()
                for i, line in enumerate(tqdm.tqdm(f, total=self.num_docs, desc='Ingesting data')):
                    doc_name, title, description = line.strip().split(',')
                    pipe.hset(name=doc_name, mapping={
                        'title': title,
                        'description': description
                    })
                    if (i + 1) % batch_size == 0:
                        pipe.execute()
                        pipe = self.redis.pipeline()
                if (i + 1) % batch_size != 0:
                    pipe.execute()
            assert self.redis.dbsize() == self.num_docs, f"Expected {self.num_docs} docs, found {self.redis.dbsize()}"
        except Exception as e:
            print(f"Error during data ingestion: {e}")
            raise
        print(f'{self.num_docs} documents ingested (in {time.time() - start_time:.6f} seconds).')

    def basic_read_all_bench(self):
        """
        Benchmarks and compares the regular read path between the disk and the RAM
        """
        basic_query = 'FT.AGGREGATE <idx> hello LIMIT 0 0'
        print("="*80)
        print(f"🚀 Running basic read benchmark: {basic_query}")
        print("="*80)
        print("="*10 + " RAM (In-Memory Index) " + "="*10)

        times = []
        read_query = ['FT.AGGREGATE', IDX_NAME, 'hello', 'LIMIT', '0', '0']
        for i in tqdm.tqdm(range(self.iters), desc='RAM read bench'):
            start_time = time.time()
            res = self.redis.execute_command(*read_query)
            times.append(time.time() - start_time)
            if i == 0:
                print(f'result: {res}')
                assert(res['total_results'] == self.num_docs)
        ram_avg_time = statistics.mean(times)
        ram_stddev_time = statistics.stdev(times) if len(times) > 1 else 0.0
        print(f'Average time taken: {ram_avg_time:.6f} seconds over {self.iters} iterations')
        print(f'Standard deviation: {ram_stddev_time:.6f} seconds')

        print("="*10 + " DISK (On-Disk Index) " + "="*10)
        times = []
        read_query_disk = ['FT.AGGREGATE', IDX_NAME_DISK, 'hello', 'LIMIT', '0', '0']
        for i in tqdm.tqdm(range(self.iters), desc='DISK read bench'):
            start_time = time.time()
            res = self.redis.execute_command(*read_query_disk)
            if i == 0:
                print(f'result: {res}')
                assert(res['total_results'] == self.num_docs)
            times.append(time.time() - start_time)
        disk_avg_time = statistics.mean(times)
        disk_stddev_time = statistics.stdev(times) if len(times) > 1 else 0.0
        print(f'Average time taken: {disk_avg_time:.6f} seconds over {self.iters} iterations')
        print(f'Standard deviation: {disk_stddev_time:.6f} seconds')
        print(f'SUMMARY: Disk_time / Ram_time (averages ratio): {disk_avg_time / ram_avg_time:.6f}')

        # Write times to the result file
        if self.result_file:
            with open(self.result_file, 'a') as f:
                f.write("\n" + "="*50 + "\n")
                f.write("READ BENCHMARK RESULTS\n")
                f.write("="*50 + "\n")
                f.write(f'Query: {basic_query}\n')
                f.write(f'RAM avg time: {ram_avg_time:.6f} seconds\n')
                f.write(f'RAM stddev time: {ram_stddev_time:.6f} seconds\n')
                f.write(f'DISK avg time: {disk_avg_time:.6f} seconds\n')
                f.write(f'DISK stddev time: {disk_stddev_time:.6f} seconds\n')
                f.write(f'SUMMARY: Disk_time / Ram_time (averages ratio): {disk_avg_time / ram_avg_time:.6f}\n')

        print("="*80)

    def basic_skipto_bench(self):
        """
        Benchmarks and compares the skip-to path between the disk and the RAM
        """
        basic_query = 'FT.AGGREGATE <idx> @description:description_0 @title:hello LIMIT 0 0'
        print("="*80)
        print(f"🚀 Running skip-to benchmark: {basic_query}")
        print("="*80)
        print("="*10 + " RAM (In-Memory Index) " + "="*10)

        times = []
        skipto_query = ['FT.AGGREGATE', IDX_NAME, '@description:description_0 @title:hello', 'LIMIT', '0', '0']
        for i in tqdm.tqdm(range(self.iters), desc='RAM skip-to bench'):
            start_time = time.time()
            res = self.redis.execute_command(*skipto_query)
            times.append(time.time() - start_time)
            if i == 0:
                print(f'result: {res}')
                # assert(res[0] == (self.num_docs / 300))
        ram_avg_time = statistics.mean(times)
        ram_stddev_time = statistics.stdev(times) if len(times) > 1 else 0.0
        print(f'Average time taken: {ram_avg_time:.6f} seconds over {self.iters} iterations')
        print(f'Standard deviation: {ram_stddev_time:.6f} seconds')

        print("="*10 + " DISK (On-Disk Index) " + "="*10)
        times = []
        skipto_query_disk = ['FT.AGGREGATE', IDX_NAME_DISK, '@description:description_0 @title:hello', 'LIMIT', '0', '0']
        for i in tqdm.tqdm(range(self.iters), desc='DISK skip-to bench'):
            start_time = time.time()
            res = self.redis.execute_command(*skipto_query_disk)
            times.append(time.time() - start_time)
            if i == 0:
                print(f'result: {res}')
                # assert(res[0] == (self.num_docs / 300))
        disk_avg_time = statistics.mean(times)
        disk_stddev_time = statistics.stdev(times) if len(times) > 1 else 0.0
        print(f'Average time taken: {disk_avg_time:.6f} seconds over {self.iters} iterations')
        print(f'Standard deviation: {disk_stddev_time:.6f} seconds')
        print(f'SUMMARY: Disk_time / Ram_time (averages ratio): {disk_avg_time / ram_avg_time:.6f}')

        # Write times to the result file
        if self.result_file:
            with open(self.result_file, 'a') as f:
                f.write("\n" + "="*50 + "\n")
                f.write("SKIP-TO BENCHMARK RESULTS\n")
                f.write("="*50 + "\n")
                f.write(f'Query: {basic_query}\n')
                f.write(f'RAM avg time: {ram_avg_time:.6f} seconds\n')
                f.write(f'RAM stddev time: {ram_stddev_time:.6f} seconds\n')
                f.write(f'DISK avg time: {disk_avg_time:.6f} seconds\n')
                f.write(f'DISK stddev time: {disk_stddev_time:.6f} seconds\n')
                f.write(f'SUMMARY: Disk_time / Ram_time (averages ratio): {disk_avg_time / ram_avg_time:.6f}\n')
        print("="*80)

    def run_basic_read_skipto_benchmarks(self):
        """
        Benchmarks and compares the read and skip-to path between the disk and the RAM
        """
        # Flush the db
        self.redis.flushdb()

        # Create the indexes
        self.create_indexes()

        # Load the data
        self.load_data()

        # Evict the data from the cache
        os.system(f'sudo vmtouch -e {self.disk_files_path}')

        # Run the read path benchmark
        self.basic_read_all_bench()

        # Run the skip-to path benchmark
        self.basic_skipto_bench()

    def run_basic_write_benchmark(self):
        """
        Benchmarks and compares the write path between the disk and the RAM.
        """
        print("="*80)
        print(f"🚀 Running write benchmark")
        print("="*80)
        print("="*10 + " RAM (In-Memory Index) " + "="*10)
        # RAM index
        times = []
        for _ in tqdm.tqdm(range(self.iters), desc='RAM write bench'):
            # Flush the db
            self.redis.flushdb()
            # Create the RAM index
            self.create_indexes(index_type='ram')
            # Ingest the data
            start_time = time.time()
            self._load_from_file()
            times.append(time.time() - start_time)
        ram_avg_time = statistics.mean(times)
        ram_stddev_time = statistics.stdev(times) if len(times) > 1 else 0.0
        print(f'Average time taken: {ram_avg_time:.6f} seconds over {self.iters} iterations')
        print(f'Standard deviation: {ram_stddev_time:.6f} seconds')

        # Flush the db
        self.redis.flushdb()

        # DISK index
        times = []
        for _ in tqdm.tqdm(range(self.iters), desc='DISK write bench'):
            # Flush the db
            self.redis.flushdb()
            # Delete the existing disk db files
            if os.path.exists(self.disk_files_path):
                shutil.rmtree(self.disk_files_path)
            # Create the DISK index
            self.create_indexes(index_type='disk')
            # Ingest the data
            start_time = time.time()
            self._load_from_file()
            times.append(time.time() - start_time)
        disk_avg_time = statistics.mean(times)
        disk_stddev_time = statistics.stdev(times) if len(times) > 1 else 0.0
        print(f'Average time taken: {disk_avg_time:.6f} seconds over {self.iters} iterations')
        print(f'Standard deviation: {disk_stddev_time:.6f} seconds')
        # Compare the time it took for both (can look at the average as well)
        print(f'SUMMARY: Disk_time / Ram_time (averages ratio): {disk_avg_time / ram_avg_time:.6f}')

        # Write times to the result file
        if self.result_file:
            with open(self.result_file, 'a') as f:
                f.write("\n" + "="*50 + "\n")
                f.write("WRITE BENCHMARK RESULTS\n")
                f.write("="*50 + "\n")
                f.write(f'Operation: Document ingestion ({self.num_docs:,} documents)\n')
                f.write(f'RAM avg time: {ram_avg_time:.6f} seconds\n')
                f.write(f'RAM stddev time: {ram_stddev_time:.6f} seconds\n')
                f.write(f'DISK avg time: {disk_avg_time:.6f} seconds\n')
                f.write(f'DISK stddev time: {disk_stddev_time:.6f} seconds\n')
                f.write(f'SUMMARY: Disk_time / Ram_time (averages ratio): {disk_avg_time / ram_avg_time:.6f}\n')

        print("="*80)

    def write_configuration(self):
        """
        Writes the benchmark configuration to the result file.
        """
        if self.result_file:
            with open(self.result_file, 'w') as f:
                from datetime import datetime
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                f.write("="*50 + "\n")
                f.write("BENCHMARK CONFIGURATION\n")
                f.write("="*50 + "\n")
                f.write(f"Timestamp: {current_time}\n")
                f.write(f"Number of documents: {self.num_docs:,}\n")
                f.write(f"Data path: {self.data_path}\n")
                f.write(f"Disk files path: {self.disk_files_path}\n")
                f.write(f"Number of iterations: {self.iters}\n")
                f.write(f"Redis host: localhost:{6379}\n")
                is_flex = self.redis.config_get('bigredis-enabled') == 'yes'
                f.write(f"RedisFlex?: {is_flex}")
                f.write("="*50 + "\n")

    def clean_env(self):
        """
        Cleans the environment before running the benchmarks.
        """
        if os.path.exists(self.disk_files_path):
            # Delete the existing disk db files
            print(f"Deleting existing disk db files at {self.disk_files_path}")
            shutil.rmtree(self.disk_files_path)
        if os.path.exists(self.result_file):
            # Delete the existing result file
            print(f"Deleting existing result file at {self.result_file}")
            os.remove(self.result_file)
        # if os.path.exists(self.data_path):
        #     # Delete the existing disk db files
        #     print(f"Deleting existing data file at {self.data_path}")
        #     os.remove(self.data_path)

    def run_benchmarks(self):
        self.clean_env()

        # Write configuration to result file
        self.write_configuration()

        self.run_basic_read_skipto_benchmarks()
        # self.run_basic_write_benchmark()
        # self.run_basic_read_write_benchmark_no_mt()    # Careful of RAM explosion since we do not yet clean the deleted-ids set.
        # self.run_basic_read_write_benchmark_mt() # Careful of RAM explosion since we do not yet clean the deleted-ids set.
        # self.run_basic_read_skipto_benchmarks_mt()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Benchmark RediSearch RAM vs DISK')
    parser.add_argument('--num_docs', type=int, default=1_000_000, help='Number of documents to index')
    parser.add_argument('--iters', type=int, default=100, help='Number of iterations for each benchmark')
    parser.add_argument('--data_path', type=str, default='tests/benchmarks/manual/diskBenchPopulationData.csv', help='Path to data file')
    parser.add_argument('--result_file', type=str, default='tests/benchmarks/manual/results/results.txt', help='CSV file to write results to')
    parser.add_argument('--disk_files_path', type=str, default='redisearch', help='Path to disk files')
    args = parser.parse_args()

    if not args.result_file.startswith('tests/benchmarks/manual/results/'):
        args.result_file = 'tests/benchmarks/manual/results/' + args.result_file

    bench = BenchDiskVsRam(num_docs=args.num_docs, data_path=args.data_path,
                           result_file=args.result_file, iters=args.iters,
                           disk_files_path=args.disk_files_path)
    bench.run_benchmarks()