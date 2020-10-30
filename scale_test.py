import argparse
import collections
from concurrent import futures
import json
import logging
import math
import prettytable
import subprocess
import time
import threading
import uuid

LOG = logging.getLogger()

parser = argparse.ArgumentParser(description='rbd-wnbd tests')
parser.add_argument('--iterations',
                    help='Total number of test iterations',
                    default=1, type=int)
parser.add_argument('--concurrency',
                    help='The number of tests to run in parallel',
                    default=4, type=int)
parser.add_argument('--fio-iterations',
                    help='Total number of benchmark iterations per disk.',
                    default=1, type=int)
parser.add_argument('--fio-workers',
                    help='Total number of fio workers per disk.',
                    default=1, type=int)
parser.add_argument('--fio-depth',
                    help='The number of concurrent asynchronous operations '
                         'executed per disk',
                    default=64, type=int)
parser.add_argument('--bs',
                    help='Benchmark block size.',
                    default="2M")
parser.add_argument('--op',
                    help='Benchmark operation.',
                    default="read")
parser.add_argument('--image_prefix',
                    help='The image name prefix.',
                    default="cephTest-")
parser.add_argument('--image_size_mb',
                    help='The image size in megabytes.',
                    default=1024, type=int)
parser.add_argument('--verbose', action='store_true',
                    help='Print info messages.')
parser.add_argument('--debug', action='store_true',
                    help='Print debug messages.')


class CephTestException(Exception):
    msg_fmt = "An exception has been encountered."

    def __init__(self, message=None, **kwargs):
        self.kwargs = kwargs
        if not message:
            message = self.msg_fmt % kwargs
        self.message = message
        super(CephTestException, self).__init__(message)


class CommandFailed(CephTestException):
    msg_fmt = (
        "Command failed: %(command)s. "
        "Return code: %(returncode)s. "
        "Stdout: %(stdout)s. Stderr: %(stderr)s.")


def setup_logging(log_level):
    handler = logging.StreamHandler()
    handler.setLevel(log_level)

    log_fmt = '[%(asctime)s] %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_fmt)
    handler.setFormatter(formatter)

    LOG.addHandler(handler)
    LOG.setLevel(logging.DEBUG)


def execute(*args, **kwargs):
    LOG.debug("Executing: %s", args)
    result = subprocess.run(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        **kwargs)
    LOG.debug("Command %s returned %d.",
              args, result.returncode)
    if result.returncode:
        exc = CommandFailed(
            command=args, returncode=result.returncode,
            stdout=result.stdout, stderr=result.stderr)
        LOG.error(exc)
        raise exc
    return result


def array_stats(array):
    array = list(array)
    mean = sum(array) / len(array)
    variance = sum((i - mean) ** 2 for i in array) / len(array)
    std_dev = math.sqrt(variance)

    return {
        'min': min(array),
        'max': max(array),
        'sum': sum(array),
        'mean': mean,
        'median': sorted(array)[len(array) // 2],
        'variance': variance,
        'std_dev': std_dev,
        'count': len(array)
    }

class Tracer:
    data = collections.defaultdict(list)
    lock = threading.Lock()

    @classmethod
    def trace(cls, func):
        def wrapper(*args, **kwargs):
            tstart = time.time()
            exc_str = None
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                exc_str = str(exc)
                raise
            finally:
                tend = time.time()

                with cls.lock:
                    cls.data[func.__qualname__] += [{
                        "duration": tend - tstart,
                        "error": exc_str,
                    }]

        return wrapper

    @classmethod
    def get_results(cls):
        functions = sorted(cls.data.keys())
        stats = collections.OrderedDict()
        for f in functions:
            stats[f] = array_stats([i['duration'] for i in cls.data[f]])
            errors = []
            for i in cls.data[f]:
                if i['error']:
                    errors.append(i['error'])

            stats[f]['errors'] = errors
        return stats

    @classmethod
    def print_results(cls):
        r = cls.get_results()

        table = prettytable.PrettyTable(title="Duration (s)")
        table.field_names = [
            "function", "min", "max", "total",
            "mean", "std_dev", "median", "count", "errors"]
        table.float_format = ".4"
        for f, s in r.items():
            table.add_row([f, s['min'], s['max'], s['sum'],
                           s['mean'], s['std_dev'], s['median'],
                           s['count'], len(s['errors'])])
        print(table)


class RbdImage(object):
    def __init__(self, name, size_mb, is_shared=True,
                 disk_number=-1, mapped=False):
        self.name = name
        self.size_mb = size_mb
        self.is_shared = is_shared
        self.disk_number = disk_number
        self.mapped = mapped
        self.removed = False

    @classmethod
    @Tracer.trace
    def create(cls, name, size_mb=1024, is_shared=True):
        LOG.info("Creating image: %s. Size: %s.",
                 name, "%sM" % size_mb)
        cmd = ["rbd", "create", name,
               "--size", "%sM" % size_mb]
        if is_shared:
            cmd += ["--image-shared"]
        execute(*cmd)

        return RbdImage(name, size_mb, is_shared)

    @Tracer.trace
    def get_disk_number(self, tries=20, retry_interval=2):
        while tries:
            LOG.info("Retrieving disk number: %s", self.name)
            result = execute("rbd-wnbd", "show", self.name, "--format=json")
            disk_info = json.loads(result.stdout)
            disk_number = disk_info["disk_number"]
            if disk_number > 0:
                LOG.debug("Image %s disk number: %d", self.name, disk_number)
                return disk_number
            tries -= 1
            time.sleep(retry_interval)

        raise CephTestException("Could not get disk number for %s", self.name)

    @Tracer.trace
    def map(self):
        LOG.info("Mapping image: %s", self.name)

        execute("rbd-wnbd", "map", self.name)
        self.mapped = True

        self.disk_number = self.get_disk_number()

    @Tracer.trace
    def unmap(self):
        if self.mapped:
            LOG.info("Unmapping image: %s", self.name)
            execute("rbd-wnbd", "unmap", self.name)
            self.mapped = False

    @Tracer.trace
    def remove(self):
        if not self.removed:
            LOG.info("Removing image: %s", self.name)
            execute("rbd", "rm", self.name)
            self.removed = True

    def cleanup(self):
        try:
            self.unmap()
        finally:
            self.remove()


class RbdTest(object):
    image = None

    def __init__(self, image_prefix="cephTest-", image_size_mb=1024):
        self.image_size_mb = image_size_mb
        self.image_name = image_prefix + str(uuid.uuid4())

    def initialize(self):
        self.image = RbdImage.create(
            self.image_name,
            self.image_size_mb)
        self.image.map()

    def run(self):
        pass

    def cleanup(self):
        if self.image:
            self.image.cleanup()

class RbdFioTest(RbdTest):
    data = []
    lock = threading.Lock()

    def __init__(self, *args, fio_size_mb=None,
                 iterations=1, workers=1,
                 bs="2M", iodepth=64, op="read",
                 **kwargs):
        super(RbdFioTest, self).__init__(*args, **kwargs)

        self.fio_size_mb = fio_size_mb or self.image_size_mb
        self.iterations = iterations
        self.workers = workers
        self.bs = bs
        self.iodepth = iodepth
        self.op = op

    def process_result(self, raw_fio_output):
        result = json.loads(raw_fio_output)
        with self.lock:
            for job in result["jobs"]:
                self.data.append({
                    'error': job['error'],
                    'io_bytes': job[self.op]['io_bytes'],
                    'bw_bytes': job[self.op]['bw_bytes'],
                    'runtime': job[self.op]['runtime'] / 1000,  # seconds
                    'total_ios': job[self.op]['short_ios'],
                    'short_ios': job[self.op]['short_ios'],
                    'dropped_ios': job[self.op]['short_ios'],
                })

    @Tracer.trace
    def run(self):
        cmd = [
            "fio", "--thread", "--output-format=json",
            "--randrepeat=%d" % self.iterations,
            "--direct=1", "--gtod_reduce=1", "--name=test",
            "--bs=%s" % self.bs, "--iodepth=%s" % self.iodepth,
            "--size=%sM" % self.fio_size_mb,
            "--readwrite=%s" % self.op,
            "--numjobs=%s" % self.workers,
            "--filename=\\\\.\\PhysicalDrive%d" % self.image.disk_number,
        ]
        result = execute(*cmd)
        self.process_result(result.stdout)

    @classmethod
    def print_results(cls, title="Benchmark results"):
        table = prettytable.PrettyTable(title=title)
        table.field_names = ["stat", "min", "max", "mean",
                             "median", "std_dev", "total"]
        table.float_format = ".4"

        s = array_stats(i["bw_bytes"] / 1000_000 for i in cls.data)
        table.add_row(["bandwidth (MB/s)", s['min'], s['max'], s['mean'],
                       s['median'], s['std_dev'], 'N/A'])

        s = array_stats(i["runtime"] / 1000 for i in cls.data)
        table.add_row(["duration (s)", s['min'], s['max'], s['mean'],
                       s['median'], s['std_dev'], s['sum']])

        s = array_stats(i["error"] for i in cls.data)
        table.add_row(["errors", s['min'], s['max'], s['mean'],
                       s['median'], s['std_dev'], s['sum']])

        s = array_stats(i["short_ios"] for i in cls.data)
        table.add_row(["incomplete IOs", s['min'], s['max'], s['mean'],
                       s['median'], s['std_dev'], s['sum']])

        s = array_stats(i["dropped_ios"] for i in cls.data)
        table.add_row(["dropped IOs", s['min'], s['max'], s['mean'],
                       s['median'], s['std_dev'], s['sum']])
        print(table)


class TestRunner(object):
    def __init__(self, test_cls, test_params=None, iterations=1, workers=1):
        self.test_cls = test_cls
        self.test_params = test_params or {}
        self.iterations = iterations
        self.workers = workers
        self.executor = futures.ThreadPoolExecutor(max_workers=workers)
        self.lock = threading.Lock()
        self.completed = 0
        self.errors = 0

    def run(self):
        tasks = []
        for i in range(self.iterations):
            task = self.executor.submit(self.run_single_test)
            tasks.append(task)

        LOG.info("Waiting for %d tests to complete.", self.iterations)
        for task in tasks:
            task.result()

    def run_single_test(self):
        try:
            test = self.test_cls(**self.test_params)
            test.initialize()
            test.run()
        except Exception as ex:
            with self.lock:
                self.errors += 1
                LOG.debug(
                    "Test exception: %s. Total exceptions: %d",
                    ex, self.errors)
        finally:
            try:
                test.cleanup()
            except Exception as ex:
                LOG.error("Test cleanup failed.")

            with self.lock:
                self.completed += 1
                LOG.info("Completed tests: %d. Pending: %d",
                         self.completed, self.iterations - self.completed)


if __name__ == '__main__':
    args = parser.parse_args()

    log_level = logging.WARNING
    if args.verbose:
        log_level = logging.INFO
    if args.debug:
        log_level = logging.DEBUG
    setup_logging(log_level)

    test_params = dict(
        image_size_mb=args.image_size_mb,
        image_prefix=args.image_prefix,
        bs=args.bs,
        op=args.op,
        iodepth=args.fio_depth,
    )
    runner = TestRunner(
        RbdFioTest,
        test_params=test_params,
        iterations=args.iterations,
        workers=args.concurrency)
    runner.run()

    Tracer.print_results()
    RbdFioTest.print_results(
        "Benchmark results (count: %d, concurrency: %d)" %
            (args.iterations, args.concurrency))
