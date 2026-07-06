"""
Core test engine for S3 load testing.
Handles worker threads, metrics collection, and test orchestration.
"""

import time
import threading
import os
import random
import string
import tempfile
import shutil
from dataclasses import dataclass, asdict
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from statistics import mean, quantiles
from datetime import datetime, timezone, timedelta
from boto3.s3.transfer import TransferConfig
import logging

logger = logging.getLogger(__name__)


@dataclass
class OperationResult:
    """Result of a single S3 operation."""
    success: bool
    operation: str  # put, get, delete, list, multipart
    elapsed: float  # seconds
    bytes_transferred: int
    timestamp: float
    error: str = None


class MetricsCollector:
    """Thread-safe metrics collector."""

    def __init__(self, max_timeseries_points=3600):
        self._lock = threading.Lock()
        self._start_time = None
        self._total_ops = 0
        self._successful_ops = 0
        self._failed_ops = 0
        self._bytes_transferred = 0
        self._wire_bytes = 0  # Bytes moved over the wire, incl. in-progress uploads (live throughput)
        self._latencies = []
        self._error_counts = {}
        self._operation_counts = {}
        self._timeseries = deque(maxlen=max_timeseries_points)
        self._last_snapshot_time = None

    def mark_start(self):
        """Mark the test start time so elapsed/throughput advance before the first op completes."""
        with self._lock:
            if self._start_time is None:
                self._start_time = time.time()

    def add_progress(self, nbytes):
        """Record bytes transferred over the wire (called incrementally during uploads)."""
        with self._lock:
            if self._start_time is None:
                self._start_time = time.time()
            self._wire_bytes += nbytes

    def add_result(self, result: OperationResult):
        """Add a result and update metrics."""
        with self._lock:
            if self._start_time is None:
                self._start_time = time.time()

            self._total_ops += 1
            if result.success:
                self._successful_ops += 1
                self._bytes_transferred += result.bytes_transferred
                # PUT bytes are already counted live via add_progress; only add read bytes here
                if result.operation in ('get', 'list'):
                    self._wire_bytes += result.bytes_transferred
            else:
                self._failed_ops += 1
                if result.error:
                    self._error_counts[result.error] = self._error_counts.get(result.error, 0) + 1

            self._latencies.append(result.elapsed)
            self._operation_counts[result.operation] = self._operation_counts.get(result.operation, 0) + 1

    def _compute_snapshot(self):
        """Compute derived metrics (call with lock held)."""
        elapsed = time.time() - self._start_time if self._start_time else 0
        if elapsed == 0:
            elapsed = 0.001  # Avoid division by zero

        # IOPS
        iops = self._total_ops / elapsed if elapsed > 0 else 0

        # Read/Write IOPS
        read_ops = (self._operation_counts.get('get', 0) + self._operation_counts.get('list', 0)
                    + self._operation_counts.get('head', 0))
        write_ops = (self._operation_counts.get('put', 0) + self._operation_counts.get('delete', 0)
                     + self._operation_counts.get('multipart', 0) + self._operation_counts.get('put_locked', 0))
        read_iops = read_ops / elapsed if elapsed > 0 else 0
        write_iops = write_ops / elapsed if elapsed > 0 else 0

        # Throughput — based on bytes actually moved over the wire (incl. in-progress
        # uploads) so the dashboard stays live during long large-object transfers
        throughput_mbs = (self._wire_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0

        # Latency percentiles
        if self._latencies:
            avg_latency = mean(self._latencies)
            try:
                p_values = quantiles(self._latencies, n=100)  # 0-100 percentiles
                p50 = p_values[49] if len(p_values) > 49 else avg_latency
                p90 = p_values[89] if len(p_values) > 89 else avg_latency
                p95 = p_values[94] if len(p_values) > 94 else avg_latency
                p99 = p_values[98] if len(p_values) > 98 else avg_latency
            except:
                p50 = p90 = p95 = p99 = avg_latency
            min_latency = min(self._latencies)
            max_latency = max(self._latencies)
        else:
            avg_latency = p50 = p90 = p95 = p99 = 0
            min_latency = max_latency = 0

        success_rate = (self._successful_ops / self._total_ops * 100) if self._total_ops > 0 else 0

        return {
            'timestamp': time.time(),
            'elapsed': elapsed,
            'total_ops': self._total_ops,
            'successful_ops': self._successful_ops,
            'failed_ops': self._failed_ops,
            'iops': round(iops, 2),
            'read_iops': round(read_iops, 2),
            'write_iops': round(write_iops, 2),
            'throughput_mbs': round(throughput_mbs, 2),
            'bytes_transferred': self._wire_bytes,
            'data_transferred_mb': round(self._wire_bytes / (1024 * 1024), 2),
            'latency_avg': round(avg_latency * 1000, 2),  # ms
            'latency_p50': round(p50 * 1000, 2),
            'latency_p90': round(p90 * 1000, 2),
            'latency_p95': round(p95 * 1000, 2),
            'latency_p99': round(p99 * 1000, 2),
            'latency_min': round(min_latency * 1000, 2),
            'latency_max': round(max_latency * 1000, 2),
            'success_rate': round(success_rate, 2),
            'operation_counts': dict(self._operation_counts),
            'error_counts': dict(self._error_counts),
        }

    def snapshot(self):
        """Return current metrics snapshot."""
        with self._lock:
            return self._compute_snapshot()

    def record_timeseries(self):
        """Record current snapshot to timeseries (call periodically)."""
        with self._lock:
            snapshot = self._compute_snapshot()
            self._timeseries.append(snapshot)

    def get_timeseries(self, last_n=60):
        """Get last N timeseries points."""
        with self._lock:
            return list(self._timeseries)[-last_n:] if self._timeseries else []


class RandomDataStream:
    """File-like object that yields `size` bytes of random data on demand.

    Generates data chunk-by-chunk so a large object (e.g. 1GB) is never held
    fully in memory. Without this, os.urandom(size) for big objects under high
    concurrency exhausts RAM and the process gets OOM-killed.
    """

    def __init__(self, size, chunk_size=8 * 1024 * 1024):
        self.size = size
        self.remaining = size
        self.chunk_size = chunk_size

    def read(self, amt=-1):
        if self.remaining <= 0:
            return b''
        if amt is None or amt < 0:
            amt = self.remaining
        n = min(amt, self.remaining, self.chunk_size)
        self.remaining -= n
        return os.urandom(n)

    def __len__(self):
        return self.size


class S3Worker:
    """Worker thread that executes S3 operations."""

    def __init__(self, s3_client, bucket_name, scenario_config, key_pool, metrics_collector, temp_file_generator=None, locked_key_pool=None, key_prefix=''):
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.scenario_config = scenario_config
        self.key_pool = key_pool  # Shared deque of uploaded keys
        self.locked_key_pool = locked_key_pool if locked_key_pool is not None else deque()
        self.metrics = metrics_collector
        self.temp_file_generator = temp_file_generator  # Function to get temp files
        self.key_prefix = key_prefix  # Optional prefix prepended to all generated keys

    def _generate_object_key(self):
        """Generate a random object key, optionally under key_prefix."""
        suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        return f"{self.key_prefix}test-obj-{suffix}"

    def _upload_to_key_pool(self, key):
        """Add an uploaded key to the pool for subsequent reads/deletes."""
        self.key_pool.append(key)
        if len(self.key_pool) > 1000:  # Keep pool from growing unbounded
            self.key_pool.popleft()

    def _get_or_create_key(self):
        """Get a key from pool, or return a new one if pool is empty."""
        if self.key_pool:
            return random.choice(list(self.key_pool))
        return self._generate_object_key()

    def execute_put(self):
        """Execute a PUT operation (streamed, never holds the full object in memory)."""
        start = time.time()
        try:
            key = self._generate_object_key()
            size = self.scenario_config['object_size']
            multipart_threshold = self.scenario_config.get('multipart_threshold', 5 * 1024 * 1024)

            # Bounded multipart config: caps memory per upload regardless of object size
            transfer_config = TransferConfig(
                multipart_threshold=multipart_threshold,
                multipart_chunksize=8 * 1024 * 1024,
                max_concurrency=4,
                use_threads=True,
            )

            # Report bytes to the metrics collector as they flow, so throughput is live
            progress_cb = (lambda n: self.metrics.add_progress(n)) if self.metrics else None

            if self.temp_file_generator:
                # Real file on disk: upload_file streams from disk (no full read into memory)
                temp_file_path = self.temp_file_generator()
                size = os.path.getsize(temp_file_path)
                self.s3_client.upload_file(temp_file_path, self.bucket_name, key, Config=transfer_config, Callback=progress_cb)
            else:
                # Generated data: stream random bytes chunk-by-chunk via a file-like object
                stream = RandomDataStream(size)
                self.s3_client.upload_fileobj(stream, self.bucket_name, key, Config=transfer_config, Callback=progress_cb)

            self._upload_to_key_pool(key)
            elapsed = time.time() - start

            return OperationResult(
                success=True,
                operation='put',
                elapsed=elapsed,
                bytes_transferred=size,
                timestamp=time.time(),
            )
        except Exception as e:
            elapsed = time.time() - start
            return OperationResult(
                success=False,
                operation='put',
                elapsed=elapsed,
                bytes_transferred=0,
                timestamp=time.time(),
                error=str(e),
            )

    def execute_get(self):
        """Execute a GET operation."""
        start = time.time()
        try:
            key = self._get_or_create_key()
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            bytes_read = response['ContentLength']
            elapsed = time.time() - start

            return OperationResult(
                success=True,
                operation='get',
                elapsed=elapsed,
                bytes_transferred=bytes_read,
                timestamp=time.time(),
            )
        except Exception as e:
            elapsed = time.time() - start
            return OperationResult(
                success=False,
                operation='get',
                elapsed=elapsed,
                bytes_transferred=0,
                timestamp=time.time(),
                error=str(e),
            )

    def execute_delete(self):
        """Execute a DELETE operation."""
        start = time.time()
        try:
            key = self._get_or_create_key()
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
            elapsed = time.time() - start

            return OperationResult(
                success=True,
                operation='delete',
                elapsed=elapsed,
                bytes_transferred=0,
                timestamp=time.time(),
            )
        except Exception as e:
            elapsed = time.time() - start
            return OperationResult(
                success=False,
                operation='delete',
                elapsed=elapsed,
                bytes_transferred=0,
                timestamp=time.time(),
                error=str(e),
            )

    def execute_list(self):
        """Execute a LIST operation."""
        start = time.time()
        try:
            self.s3_client.list_objects_v2(Bucket=self.bucket_name, MaxKeys=100)
            elapsed = time.time() - start

            return OperationResult(
                success=True,
                operation='list',
                elapsed=elapsed,
                bytes_transferred=0,
                timestamp=time.time(),
            )
        except Exception as e:
            elapsed = time.time() - start
            return OperationResult(
                success=False,
                operation='list',
                elapsed=elapsed,
                bytes_transferred=0,
                timestamp=time.time(),
                error=str(e),
            )

    def execute_head(self):
        """Execute a HEAD operation (HeadObject — metadata check, no data download)."""
        start = time.time()
        try:
            key = self._get_or_create_key()
            self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            elapsed = time.time() - start
            return OperationResult(success=True, operation='head', elapsed=elapsed, bytes_transferred=0, timestamp=time.time())
        except Exception as e:
            elapsed = time.time() - start
            return OperationResult(success=False, operation='head', elapsed=elapsed, bytes_transferred=0, timestamp=time.time(), error=str(e))

    def execute_put_locked(self):
        """PUT an object and immediately apply a retention lock."""
        start = time.time()
        try:
            key = self._generate_object_key()
            size = self.scenario_config['object_size']
            retention_mode = self.scenario_config.get('retention_mode', 'GOVERNANCE')
            retention_seconds = self.scenario_config.get('retention_seconds', 60)
            multipart_threshold = self.scenario_config.get('multipart_threshold', 5 * 1024 * 1024)

            transfer_config = TransferConfig(
                multipart_threshold=multipart_threshold,
                multipart_chunksize=8 * 1024 * 1024,
                max_concurrency=4,
                use_threads=True,
            )
            progress_cb = (lambda n: self.metrics.add_progress(n)) if self.metrics else None

            stream = RandomDataStream(size)
            self.s3_client.upload_fileobj(stream, self.bucket_name, key, Config=transfer_config, Callback=progress_cb)

            retain_until = datetime.now(timezone.utc) + timedelta(seconds=retention_seconds)
            self.s3_client.put_object_retention(
                Bucket=self.bucket_name,
                Key=key,
                Retention={
                    'Mode': retention_mode,
                    'RetainUntilDate': retain_until,
                }
            )

            self._upload_to_key_pool(key)
            self.locked_key_pool.append(key)
            if len(self.locked_key_pool) > 500:
                self.locked_key_pool.popleft()

            elapsed = time.time() - start
            return OperationResult(success=True, operation='put_locked', elapsed=elapsed, bytes_transferred=size, timestamp=time.time())
        except Exception as e:
            elapsed = time.time() - start
            return OperationResult(success=False, operation='put_locked', elapsed=elapsed, bytes_transferred=0, timestamp=time.time(), error=str(e))

    def execute_delete_locked(self):
        """Attempt to delete a retention-locked object.
        success=True means the delete was correctly BLOCKED (lock is working).
        success=False means the delete unexpectedly succeeded (lock not enforced).
        """
        start = time.time()
        try:
            if not self.locked_key_pool:
                # No locked objects yet — skip this op gracefully
                elapsed = time.time() - start
                return OperationResult(success=True, operation='delete_locked', elapsed=elapsed, bytes_transferred=0, timestamp=time.time())

            key = random.choice(list(self.locked_key_pool))
            try:
                self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
                # Delete succeeded — retention lock is NOT working
                elapsed = time.time() - start
                return OperationResult(
                    success=False, operation='delete_locked', elapsed=elapsed,
                    bytes_transferred=0, timestamp=time.time(),
                    error='DELETE_SUCCEEDED_LOCK_NOT_ENFORCED',
                )
            except Exception as delete_err:
                err_str = str(delete_err)
                # These codes/strings indicate the server correctly blocked the delete
                blocked_indicators = ('ObjectLocked', 'AccessDenied', 'locked', 'MethodNotAllowed', 'InvalidRequest', '403')
                if any(ind.lower() in err_str.lower() for ind in blocked_indicators):
                    elapsed = time.time() - start
                    return OperationResult(success=True, operation='delete_locked', elapsed=elapsed, bytes_transferred=0, timestamp=time.time())
                # Unexpected error (network, auth, etc.)
                elapsed = time.time() - start
                return OperationResult(success=False, operation='delete_locked', elapsed=elapsed, bytes_transferred=0, timestamp=time.time(), error=err_str)
        except Exception as e:
            elapsed = time.time() - start
            return OperationResult(success=False, operation='delete_locked', elapsed=elapsed, bytes_transferred=0, timestamp=time.time(), error=str(e))

    def execute_operation(self, operation_type):
        """Execute an operation based on type."""
        if operation_type == 'put':
            return self.execute_put()
        elif operation_type == 'get':
            return self.execute_get()
        elif operation_type == 'delete':
            return self.execute_delete()
        elif operation_type == 'list':
            return self.execute_list()
        elif operation_type == 'head':
            return self.execute_head()
        elif operation_type == 'put_locked':
            return self.execute_put_locked()
        elif operation_type == 'delete_locked':
            return self.execute_delete_locked()
        else:
            raise ValueError(f"Unknown operation type: {operation_type}")


class TestEngine:
    """Main test engine orchestrator."""

    def __init__(self, max_timeseries_points=3600):
        self.metrics = MetricsCollector(max_timeseries_points)
        self._stop_event = threading.Event()
        self._test_thread = None
        self._running = False
        self._key_pool = deque()
        self._locked_key_pool = deque()
        self._sampler_thread = None
        self._temp_dir = None
        self._temp_file_cache = {}  # Cache of generated temp files

    def start(self, scenario_config, bucket_name, s3_client, custom_overrides=None, key_prefix=''):
        """Start a test with given scenario."""
        if self._running:
            raise RuntimeError("Test already running")

        logger.info(f"TestEngine.start() called: duration_limit from scenario_config={scenario_config.get('duration_limit')}, custom_overrides={custom_overrides}")

        # Apply custom overrides
        config = dict(scenario_config)
        if custom_overrides:
            config.update(custom_overrides)

        logger.info(f"Final config for _run_test: duration_limit={config.get('duration_limit')}, concurrent_ops={config.get('concurrent_ops')}")

        self._stop_event.clear()
        self._running = True
        self._key_pool.clear()
        self._locked_key_pool.clear()
        self._temp_file_cache.clear()

        # Create temp directory if using real files
        temp_file_generator = None
        if config.get('use_real_files'):
            self._temp_dir = tempfile.mkdtemp(prefix='s3_stress_')
            logger.info(f"Created temp directory for real files: {self._temp_dir}")

            # Pre-create a few temp files to reuse
            def get_temp_file():
                """Get or create a temp file for uploads."""
                file_id = random.randint(0, 4)  # Use 5 pre-created files
                cache_key = f"file_{file_id}"

                if cache_key not in self._temp_file_cache:
                    temp_file_path = os.path.join(self._temp_dir, f"test_file_{file_id}.bin")
                    if not os.path.exists(temp_file_path):
                        # Create the temp file with random data
                        with open(temp_file_path, 'wb') as f:
                            f.write(os.urandom(config['object_size']))
                    self._temp_file_cache[cache_key] = temp_file_path

                return self._temp_file_cache[cache_key]

            temp_file_generator = get_temp_file

        # Mark start so elapsed/throughput advance immediately (before first op completes)
        self.metrics.mark_start()

        # Start sampler thread (records metrics every 1 second)
        self._sampler_thread = threading.Thread(target=self._sampler_loop, daemon=True)
        self._sampler_thread.start()

        # Start main test thread
        self._test_thread = threading.Thread(
            target=self._run_test,
            args=(config, bucket_name, s3_client, temp_file_generator, key_prefix),
            daemon=True,
        )
        self._test_thread.start()

    def stop(self):
        """Stop the running test."""
        if self._running:
            self._stop_event.set()

    def _sampler_loop(self):
        """Record timeseries snapshots every 1 second."""
        while self._running:
            time.sleep(1)
            self.metrics.record_timeseries()

    def _run_test(self, scenario_config, bucket_name, s3_client, temp_file_generator=None, key_prefix=''):
        """Run the test in a background thread."""
        try:
            concurrent_ops = scenario_config['concurrent_ops']
            operations_per_thread = scenario_config['operations_per_thread']
            operation_mix = scenario_config['operation_mix']
            duration_limit = scenario_config.get('duration_limit', None)

            start_time = time.time()
            logger.info(f"Test started: duration_limit={duration_limit}, concurrent_ops={concurrent_ops}, operation_mix={operation_mix}, use_real_files={scenario_config.get('use_real_files', False)}")

            # If duration limit is specified, run for that duration; otherwise use operation count
            if duration_limit:
                # Duration-based test
                # Bound in-flight operations so workers actually finish their tasks
                # (without this, the loop queues thousands of futures and large PUTs
                #  never complete within the window — only fast-failing GETs get counted)
                max_pending = max(concurrent_ops * 2, concurrent_ops + 1)
                with ThreadPoolExecutor(max_workers=concurrent_ops) as executor:
                    futures = []
                    operation_counter = 0

                    while not self._stop_event.is_set():
                        elapsed = time.time() - start_time

                        if elapsed >= duration_limit:
                            logger.info(f"Duration limit reached: elapsed={elapsed:.1f}s >= duration_limit={duration_limit}s, breaking")
                            break

                        # Submit new operations only while under the in-flight cap
                        while len(futures) < max_pending and not self._stop_event.is_set():
                            if time.time() - start_time >= duration_limit:
                                break
                            op_type = random.choices(list(operation_mix.keys()), weights=list(operation_mix.values()))[0]
                            if operation_counter < 10:
                                logger.info(f"DEBUG: Selected operation type: {op_type} (mix: {operation_mix})")
                            worker = S3Worker(s3_client, bucket_name, scenario_config, self._key_pool, self.metrics, temp_file_generator, self._locked_key_pool, key_prefix)
                            future = executor.submit(worker.execute_operation, op_type)
                            futures.append(future)
                            operation_counter += 1

                        # Collect completed results without blocking
                        completed = []
                        for future in futures:
                            if future.done():
                                try:
                                    result = future.result()
                                    self.metrics.add_result(result)
                                except Exception as e:
                                    logger.error(f"Worker error: {e}")
                                completed.append(future)

                        # Remove completed futures from list
                        for future in completed:
                            futures.remove(future)

                        # Brief pause to avoid busy-waiting when all workers are saturated
                        if len(futures) >= max_pending:
                            time.sleep(0.05)

                    logger.info(f"Test duration loop exited: submitted {operation_counter} operations, {len(futures)} pending")
                    # Wait for remaining operations to complete
                    # Each operation can take a long time (especially 1GB uploads), so use generous timeout
                    try:
                        for future in as_completed(futures, timeout=300):
                            try:
                                result = future.result(timeout=120)  # Per-future: allow large uploads to finish
                                self.metrics.add_result(result)
                            except Exception as e:
                                logger.error(f"Worker error: {e}")
                    except Exception as e:
                        logger.warning(f"Timeout waiting for remaining operations: {e}")
                        logger.info(f"Collecting any remaining completed futures without timeout...")
                        # Try to collect any that are already done without waiting
                        for future in futures:
                            if future.done():
                                try:
                                    result = future.result(timeout=1)
                                    self.metrics.add_result(result)
                                except:
                                    pass
            else:
                # Operation-count based test (original behavior)
                total_operations = concurrent_ops * operations_per_thread

                # Build operation list weighted by mix
                operations = []
                for op_type, weight in operation_mix.items():
                    count = int(total_operations * (weight / 100))
                    operations.extend([op_type] * count)
                random.shuffle(operations)

                with ThreadPoolExecutor(max_workers=concurrent_ops) as executor:
                    futures = []

                    for op_type in operations:
                        if self._stop_event.is_set():
                            break

                        worker = S3Worker(s3_client, bucket_name, scenario_config, self._key_pool, self.metrics, temp_file_generator, self._locked_key_pool, key_prefix)
                        future = executor.submit(worker.execute_operation, op_type)
                        futures.append(future)

                    # Collect results
                    for future in as_completed(futures):
                        if self._stop_event.is_set():
                            break
                        try:
                            result = future.result()
                            self.metrics.add_result(result)
                        except Exception as e:
                            logger.error(f"Worker error: {e}")

        finally:
            self._running = False
            # Clean up temp directory if it was created
            if self._temp_dir and os.path.exists(self._temp_dir):
                try:
                    shutil.rmtree(self._temp_dir)
                    logger.info(f"Cleaned up temp directory: {self._temp_dir}")
                except Exception as e:
                    logger.error(f"Error cleaning up temp directory: {e}")

    def is_running(self):
        """Check if test is running."""
        return self._running

    def get_status(self):
        """Get current test status."""
        snapshot = self.metrics.snapshot()
        timeseries = self.metrics.get_timeseries(last_n=60)

        return {
            'running': self._running,
            **snapshot,
            'timeseries': timeseries,
        }
