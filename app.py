"""
Ceph S3 Load Testing Tool - Flask Web Application
"""

from flask import Flask, render_template, request, jsonify, send_file
from flask_cors import CORS
import boto3
import logging
import os
import json
import uuid
from pathlib import Path
from io import StringIO, BytesIO
from datetime import datetime

from config import (
    DEBUG, HOST, PORT, LOG_LEVEL, LOG_FILE, REPORTS_DIR,
    METRICS_POLL_INTERVAL, MAX_TIMESERIES_POINTS
)
from scenarios import SCENARIOS, get_scenario_info
from engine import TestEngine
from reporter import Reporter

# Setup logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

reporter = Reporter(REPORTS_DIR)


class SessionManager:
    """Manages multiple concurrent test sessions."""

    def __init__(self):
        self.sessions = {}

    def create_session(self):
        """Create a new test session and return its ID."""
        session_id = str(uuid.uuid4())[:8]
        self.sessions[session_id] = {
            'test_engine': None,
            'current_scenario': None,
            's3_client': None,
            'bucket_stats_start': None,
        }
        return session_id

    def get_session(self, session_id):
        """Get a session by ID."""
        return self.sessions.get(session_id)

    def delete_session(self, session_id):
        """Delete a session."""
        if session_id in self.sessions:
            del self.sessions[session_id]

    def set_test_engine(self, session_id, engine):
        """Set test engine for a session."""
        if session_id in self.sessions:
            self.sessions[session_id]['test_engine'] = engine

    def set_scenario(self, session_id, scenario):
        """Set scenario for a session."""
        if session_id in self.sessions:
            self.sessions[session_id]['current_scenario'] = scenario

    def set_s3_client(self, session_id, client):
        """Set S3 client for a session."""
        if session_id in self.sessions:
            self.sessions[session_id]['s3_client'] = client

    def set_bucket_stats_start(self, session_id, stats):
        """Set bucket stats start for a session."""
        if session_id in self.sessions:
            self.sessions[session_id]['bucket_stats_start'] = stats


# Session manager
session_manager = SessionManager()


def create_s3_client(endpoint_url, access_key, secret_key, region='default'):
    """Create S3 client for Ceph or other S3-compatible storage."""
    return boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )


def capture_bucket_stats(s3_client, bucket_name, timeout_seconds=10):
    """Capture bucket statistics at a point in time (with pagination and timeout)."""
    try:
        import signal

        def timeout_handler(signum, frame):
            raise TimeoutError(f"Bucket stats capture exceeded {timeout_seconds} seconds")

        # Set timeout to prevent hanging on large buckets
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout_seconds)

        try:
            objects = []
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name)

            for page in pages:
                if 'Contents' in page:
                    objects.extend(page['Contents'])
        finally:
            signal.alarm(0)  # Cancel the alarm

        if not objects:
            return {
                'bucket_name': bucket_name,
                'object_count': 0,
                'total_size_bytes': 0,
                'average_size_bytes': 0,
            }

        object_count = len(objects)
        total_size_bytes = sum(obj['Size'] for obj in objects)
        average_size = total_size_bytes / object_count if object_count > 0 else 0

        return {
            'bucket_name': bucket_name,
            'object_count': object_count,
            'total_size_bytes': total_size_bytes,
            'average_size_bytes': average_size,
        }
    except Exception as e:
        logger.error(f"Error capturing bucket stats: {e}")
        return None


@app.route('/')
def index():
    """Serve main dashboard."""
    return render_template('index.html', poll_interval=METRICS_POLL_INTERVAL)


@app.route('/reports')
def reports_page():
    """Serve reports page."""
    return render_template('reports.html')


@app.route('/analyze')
def analyze_page():
    """Serve report analysis page."""
    return render_template('analyze.html')


@app.route('/api/retention/check', methods=['POST'])
def check_retention_support():
    """Check if a bucket has S3 Object Lock enabled."""
    try:
        data = request.json
        bucket_name = data.get('bucket_name', '').strip()
        endpoint_url = data.get('endpoint_url', '').strip()
        access_key = data.get('access_key', '').strip()
        secret_key = data.get('secret_key', '').strip()
        region = data.get('region', 'default')

        if not all([bucket_name, endpoint_url, access_key, secret_key]):
            return jsonify({'error': 'Missing required fields'}), 400

        s3_client = create_s3_client(endpoint_url, access_key, secret_key, region)

        try:
            response = s3_client.get_object_lock_configuration(Bucket=bucket_name)
            lock_config = response.get('ObjectLockConfiguration', {})
            enabled = lock_config.get('ObjectLockEnabled') == 'Enabled'
            return jsonify({
                'object_lock_enabled': enabled,
                'lock_configuration': lock_config,
                'bucket': bucket_name,
            })
        except Exception as e:
            err_str = str(e)
            if 'ObjectLockConfigurationNotFoundError' in err_str or 'NoSuchObjectLockConfiguration' in err_str:
                return jsonify({'object_lock_enabled': False, 'reason': 'Object Lock not configured on this bucket', 'bucket': bucket_name})
            if 'AccessDenied' in err_str:
                return jsonify({'object_lock_enabled': None, 'reason': 'Access denied — cannot check Object Lock configuration', 'bucket': bucket_name})
            return jsonify({'error': err_str}), 500

    except Exception as e:
        logger.error(f"Error checking retention support: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/bucket-stats', methods=['POST'])
def get_bucket_stats():
    """Get statistics for a specific S3 bucket."""
    try:
        data = request.json
        bucket_name = data.get('bucket_name', '').strip()
        endpoint_url = data.get('endpoint_url', '').strip()
        access_key = data.get('access_key', '').strip()
        secret_key = data.get('secret_key', '').strip()

        if not all([bucket_name, endpoint_url, access_key, secret_key]):
            return jsonify({'error': 'Missing required fields'}), 400

        # Create S3 client
        try:
            s3_client = create_s3_client(endpoint_url, access_key, secret_key)
            s3_client.head_bucket(Bucket=bucket_name)
        except Exception as e:
            logger.error(f"Failed to connect to S3 bucket: {e}")
            return jsonify({'error': f'Cannot access bucket: {str(e)}'}), 400

        # Get bucket stats
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name)

            if 'Contents' not in response:
                # Bucket is empty
                return jsonify({
                    'bucket_name': bucket_name,
                    'object_count': 0,
                    'total_size_bytes': 0,
                    'total_size_mb': 0,
                    'total_size_gb': 0,
                    'average_size_mb': 0,
                    'formatted_size': '0 B',
                    'last_modified': 'N/A'
                })

            objects = response['Contents']
            object_count = len(objects)
            total_size_bytes = sum(obj['Size'] for obj in objects)
            average_size = total_size_bytes / object_count if object_count > 0 else 0

            # Format size
            def format_size(bytes_val):
                for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                    if bytes_val < 1024:
                        return f"{bytes_val:.2f} {unit}"
                    bytes_val /= 1024
                return f"{bytes_val:.2f} TB"

            # Get most recent object
            last_modified = max(obj['LastModified'] for obj in objects).isoformat() if objects else 'N/A'

            return jsonify({
                'bucket_name': bucket_name,
                'object_count': object_count,
                'total_size_bytes': total_size_bytes,
                'total_size_mb': round(total_size_bytes / (1024 * 1024), 2),
                'total_size_gb': round(total_size_bytes / (1024 * 1024 * 1024), 2),
                'average_size_mb': round(average_size / (1024 * 1024), 2),
                'formatted_size': format_size(total_size_bytes),
                'last_modified': last_modified,
                'formatted_avg_size': format_size(average_size)
            })

        except Exception as e:
            logger.error(f"Error getting bucket stats: {e}")
            return jsonify({'error': f'Error retrieving bucket stats: {str(e)}'}), 500

    except Exception as e:
        logger.error(f"Error in bucket-stats endpoint: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/scenarios', methods=['GET'])
def get_scenarios():
    """Get all available test scenarios (including custom)."""
    scenarios = {}

    # Add built-in scenarios
    for key in SCENARIOS:
        info = get_scenario_info(key)
        if info:
            scenarios[key] = info

    # Add custom scenarios
    try:
        custom_dir = Path('/opt/s3-stress-tool/custom_scenarios')
        if custom_dir.exists():
            for scenario_file in custom_dir.glob('*.json'):
                try:
                    with open(scenario_file) as f:
                        custom = json.load(f)
                        key = scenario_file.stem
                        object_size = custom.get('object_size', 1024*1024)
                        scenarios[key] = {
                            'name': custom.get('name'),
                            'description': custom.get('description'),
                            'category': 'custom',
                            'concurrent_ops': custom.get('concurrent_ops'),
                            'object_size_mb': object_size / (1024 * 1024),
                        }
                except Exception as e:
                    logger.error(f"Error loading custom scenario {scenario_file}: {e}")
    except Exception as e:
        logger.error(f"Error loading custom scenarios: {e}")

    return jsonify({'scenarios': scenarios})


@app.route('/api/test/start', methods=['POST'])
def start_test():
    """Start a new stress test."""
    try:
        data = request.json
        scenario_key = data.get('scenario')
        bucket_name = data.get('bucket_name')
        endpoint_url = data.get('endpoint_url')
        access_key = data.get('access_key')
        secret_key = data.get('secret_key')
        region = data.get('region', 'default')
        duration = data.get('duration', 300)  # Duration in seconds, default 5 min
        custom_overrides = data.get('custom', {})

        # Check if scenario exists (built-in or custom)
        scenario_config = None
        if scenario_key in SCENARIOS:
            scenario_config = SCENARIOS[scenario_key].copy()
        else:
            # Try to load custom scenario
            try:
                custom_path = Path(f'/opt/s3-stress-tool/custom_scenarios/{scenario_key}.json')
                if custom_path.exists():
                    with open(custom_path) as f:
                        scenario_config = json.load(f)
                else:
                    return jsonify({'error': 'Invalid scenario'}), 400
            except Exception as e:
                logger.error(f"Error loading custom scenario: {e}")
                return jsonify({'error': 'Invalid scenario'}), 400

        if not scenario_config:
            return jsonify({'error': 'Invalid scenario'}), 400
        if not bucket_name:
            return jsonify({'error': 'Bucket name required'}), 400
        if not endpoint_url:
            return jsonify({'error': 'Endpoint URL required'}), 400
        if not access_key or not secret_key:
            return jsonify({'error': 'Access key and secret key required'}), 400

        # Create S3 client
        try:
            s3_client = create_s3_client(endpoint_url, access_key, secret_key, region)
            s3_client.head_bucket(Bucket=bucket_name)
        except Exception as e:
            logger.error(f"Failed to connect to S3: {e}")
            return jsonify({'error': f'Cannot access bucket: {str(e)}'}), 400

        # Create new session
        session_id = session_manager.create_session()
        session = session_manager.get_session(session_id)

        # Capture bucket stats at test start
        bucket_stats_start = capture_bucket_stats(s3_client, bucket_name)
        logger.info(f"[Session {session_id}] Bucket stats at test start: {bucket_stats_start}")

        # Start test
        test_engine = TestEngine(max_timeseries_points=MAX_TIMESERIES_POINTS)
        current_scenario = {
            'key': scenario_key,
            'bucket': bucket_name,
            'config': scenario_config,
            'endpoint': endpoint_url,
            'custom': custom_overrides,
            'duration': duration,
            'bucket_stats_start': bucket_stats_start,
        }

        scenario_config['duration_limit'] = duration  # Override with user-specified duration

        logger.info(f"[Session {session_id}] Scenario config: duration_limit={scenario_config.get('duration_limit')}, concurrent_ops={scenario_config.get('concurrent_ops')}")

        # Store in session
        session_manager.set_test_engine(session_id, test_engine)
        session_manager.set_scenario(session_id, current_scenario)
        session_manager.set_s3_client(session_id, s3_client)
        session_manager.set_bucket_stats_start(session_id, bucket_stats_start)

        test_engine.start(scenario_config, bucket_name, s3_client, custom_overrides)

        logger.info(f"[Session {session_id}] Started test: {scenario_key} on bucket {bucket_name} for {duration} seconds")
        return jsonify({
            'status': 'started',
            'scenario': scenario_key,
            'bucket': bucket_name,
            'session_id': session_id,
        })

    except Exception as e:
        logger.error(f"Error starting test: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/test/stop', methods=['POST'])
def stop_test():
    """Stop a test."""
    try:
        data = request.json or {}
        session_id = data.get('session_id')

        if not session_id:
            return jsonify({'error': 'Session ID required'}), 400

        session = session_manager.get_session(session_id)
        if not session:
            return jsonify({'error': 'Session not found'}), 404

        test_engine = session['test_engine']
        current_scenario = session['current_scenario']
        s3_client = session['s3_client']
        bucket_stats_start = session['bucket_stats_start']

        if test_engine and test_engine.is_running():
            test_engine.stop()
            logger.info(f"[Session {session_id}] Test stopped by user")

            # Wait a moment for test to actually stop
            import time as time_module
            time_module.sleep(0.5)

            # Save report for manually stopped test
            if current_scenario:
                try:
                    status = test_engine.get_status()
                    logger.info(f"[Session {session_id}] Final status: running={status.get('running')}, elapsed={status.get('elapsed', 0):.1f}s")

                    # Capture bucket stats at manual stop (with timeout to prevent hanging)
                    try:
                        bucket_stats_end = capture_bucket_stats(s3_client, current_scenario['bucket'])
                        logger.info(f"[Session {session_id}] Bucket stats at manual stop: {bucket_stats_end}")
                    except Exception as e:
                        logger.warning(f"[Session {session_id}] Timeout or error capturing bucket stats on stop: {e}")
                        bucket_stats_end = None

                    # Calculate delta
                    bucket_stats_delta = None
                    if bucket_stats_start and bucket_stats_end:
                        bucket_stats_delta = {
                            'objects_added': bucket_stats_end['object_count'] - bucket_stats_start['object_count'],
                            'bytes_added': bucket_stats_end['total_size_bytes'] - bucket_stats_start['total_size_bytes'],
                        }

                    run_id = reporter.save_report(
                        current_scenario['key'],
                        current_scenario['config'],
                        status,
                        status.get('timeseries', []),
                        bucket_stats_start=bucket_stats_start,
                        bucket_stats_end=bucket_stats_end,
                        bucket_stats_delta=bucket_stats_delta,
                        session_id=session_id
                    )
                    logger.info(f"[Session {session_id}] Report saved successfully: {run_id}")
                except Exception as e:
                    logger.error(f"[Session {session_id}] Error saving report: {e}", exc_info=True)
                    raise

            # Clean up session
            session_manager.delete_session(session_id)
            return jsonify({'status': 'stopped'})
        else:
            return jsonify({'error': 'No test running in this session'}), 400
    except Exception as e:
        logger.error(f"Error stopping test: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/test/status', methods=['GET'])
def test_status():
    """Get test status and metrics for a session."""
    try:
        session_id = request.args.get('session_id')

        if not session_id:
            return jsonify({
                'running': False,
                'total_ops': 0,
                'iops': 0,
                'throughput_mbs': 0,
                'latency_avg': 0,
            }), 200

        session = session_manager.get_session(session_id)
        if not session:
            return jsonify({
                'running': False,
                'error': 'Session not found',
                'total_ops': 0,
                'iops': 0,
                'throughput_mbs': 0,
                'latency_avg': 0,
            }), 200

        test_engine = session['test_engine']
        current_scenario = session['current_scenario']
        s3_client = session['s3_client']
        bucket_stats_start = session['bucket_stats_start']

        if not test_engine:
            return jsonify({
                'running': False,
                'total_ops': 0,
                'iops': 0,
                'throughput_mbs': 0,
                'latency_avg': 0,
            }), 200

        status = test_engine.get_status()

        # Safety check: if test running longer than duration_limit + 10 seconds, force stop it
        if status['running'] and current_scenario:
            duration_limit = current_scenario.get('duration', 300)
            elapsed = status.get('elapsed', 0)
            if elapsed > duration_limit + 10:
                logger.warning(f"[Session {session_id}] Test exceeded duration by {elapsed - duration_limit:.1f}s, force stopping")
                test_engine.stop()
                # Give it 1 second to stop gracefully
                import time as time_module
                time_module.sleep(1)
                status = test_engine.get_status()

        # If test just finished, save the report with bucket stats
        if not status['running'] and current_scenario:
            # Capture bucket stats at test end
            bucket_stats_end = capture_bucket_stats(s3_client, current_scenario['bucket'])
            logger.info(f"[Session {session_id}] Bucket stats at test end: {bucket_stats_end}")

            # Calculate delta
            bucket_stats_delta = None
            if bucket_stats_start and bucket_stats_end:
                bucket_stats_delta = {
                    'objects_added': bucket_stats_end['object_count'] - bucket_stats_start['object_count'],
                    'bytes_added': bucket_stats_end['total_size_bytes'] - bucket_stats_start['total_size_bytes'],
                }

            run_id = reporter.save_report(
                current_scenario['key'],
                current_scenario['config'],
                status,
                status.get('timeseries', []),
                bucket_stats_start=bucket_stats_start,
                bucket_stats_end=bucket_stats_end,
                bucket_stats_delta=bucket_stats_delta,
                session_id=session_id
            )
            logger.info(f"[Session {session_id}] Report saved: {run_id}")

            # Clean up session
            session_manager.delete_session(session_id)

        return jsonify(status)

    except Exception as e:
        logger.error(f"Error getting status: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/reports', methods=['GET'])
def list_reports():
    """List all saved test reports."""
    try:
        reports = reporter.list_reports()
        return jsonify({'reports': reports})
    except Exception as e:
        logger.error(f"Error listing reports: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/reports/<run_id>', methods=['GET'])
def get_report(run_id):
    """Get a specific report."""
    try:
        report = reporter.get_report(run_id)
        if not report:
            return jsonify({'error': 'Report not found'}), 404
        return jsonify(report)
    except Exception as e:
        logger.error(f"Error getting report: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/reports/<run_id>/export', methods=['GET'])
def export_report(run_id):
    """Export a report as CSV or JSON."""
    try:
        fmt = request.args.get('format', 'json').lower()

        if fmt == 'csv':
            content = reporter.export_csv(run_id)
            if not content:
                return jsonify({'error': 'Report not found'}), 404
            # Use BytesIO for binary file handling
            file_data = BytesIO(content.encode('utf-8'))
            return send_file(
                file_data,
                mimetype='text/csv',
                as_attachment=True,
                download_name=f"{run_id}.csv"
            )
        else:
            content = reporter.export_json(run_id)
            if not content:
                return jsonify({'error': 'Report not found'}), 404
            # Use BytesIO for binary file handling
            file_data = BytesIO(content.encode('utf-8'))
            return send_file(
                file_data,
                mimetype='application/json',
                as_attachment=True,
                download_name=f"{run_id}.json"
            )

    except Exception as e:
        logger.error(f"Error exporting report: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/reports/compare', methods=['GET'])
def compare_reports():
    """Compare two reports."""
    try:
        run_id_a = request.args.get('a')
        run_id_b = request.args.get('b')

        if not run_id_a or not run_id_b:
            return jsonify({'error': 'Both run IDs required'}), 400

        comparison = reporter.compare_reports(run_id_a, run_id_b)
        if not comparison:
            return jsonify({'error': 'One or both reports not found'}), 404

        return jsonify(comparison)

    except Exception as e:
        logger.error(f"Error comparing reports: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/scenarios')
def scenarios_guide():
    """Serve scenarios explanation page."""
    return render_template('scenarios.html')


@app.route('/custom')
def custom_scenarios_page():
    """Serve custom scenarios builder page."""
    return render_template('custom.html')


@app.route('/api/custom-scenarios', methods=['GET', 'POST'])
def manage_custom_scenarios():
    """Get or create custom scenarios."""
    if request.method == 'GET':
        # Return all custom scenarios
        try:
            custom_dir = Path('/opt/s3-stress-tool/custom_scenarios')
            custom_dir.mkdir(exist_ok=True)

            scenarios = []
            for scenario_file in custom_dir.glob('*.json'):
                try:
                    with open(scenario_file) as f:
                        scenario = json.load(f)
                        scenario['key'] = scenario_file.stem
                        scenarios.append(scenario)
                except Exception as e:
                    logger.error(f"Error reading {scenario_file}: {e}")

            return jsonify({'scenarios': scenarios})
        except Exception as e:
            logger.error(f"Error getting custom scenarios: {e}")
            return jsonify({'error': str(e)}), 500

    elif request.method == 'POST':
        # Create new custom scenario
        try:
            import re
            data = request.json
            name = data.get('name', '').strip()

            if not name:
                return jsonify({'error': 'Scenario name required'}), 400

            # Generate scenario key from name
            scenario_key = re.sub(r'[^a-z0-9_]', '_', name.lower())

            # Check if already exists
            scenario_path = Path(f'/opt/s3-stress-tool/custom_scenarios/{scenario_key}.json')
            scenario_path.parent.mkdir(parents=True, exist_ok=True)

            scenario = {
                'name': name,
                'description': f"Custom scenario: {name}",
                'category': 'custom',
                'concurrent_ops': int(data.get('concurrent_ops', 20)),
                'operations_per_thread': int(data.get('operations_per_thread', 50)),
                'object_size': int(data.get('object_size', 1024*1024)),
                'operation_mix': data.get('operation_mix', {'put': 50, 'get': 50}),
                'multipart_threshold': 5 * 1024 * 1024,
            }

            with open(scenario_path, 'w') as f:
                json.dump(scenario, f, indent=2)

            logger.info(f"Custom scenario saved: {scenario_key}")
            return jsonify({'scenario_key': scenario_key, 'name': name})

        except Exception as e:
            logger.error(f"Error creating custom scenario: {e}")
            return jsonify({'error': str(e)}), 500


@app.route('/api/custom-scenarios/<scenario_key>', methods=['DELETE'])
def delete_custom_scenario(scenario_key):
    """Delete a custom scenario."""
    try:
        scenario_path = Path(f'/opt/s3-stress-tool/custom_scenarios/{scenario_key}.json')

        if scenario_path.exists():
            scenario_path.unlink()
            logger.info(f"Custom scenario deleted: {scenario_key}")
            return jsonify({'status': 'deleted'})
        else:
            return jsonify({'error': 'Scenario not found'}), 404

    except Exception as e:
        logger.error(f"Error deleting custom scenario: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/scenarios/detailed', methods=['GET'])
def get_detailed_scenarios():
    """Get detailed information about all scenarios."""
    scenarios_list = []

    scenario_descriptions = {
        'sequential_write': '✍️ Write large files sequentially to measure maximum write throughput (MB/s). Best for benchmarking network and disk performance.',
        'sequential_read': '📖 Read large files sequentially to measure maximum read throughput. Useful for benchmarking read-heavy workloads.',
        'random_write_iops': '⚡ Small random writes to measure write operations per second (IOPS). Tests metadata operations and small object performance.',
        'random_read_iops': '🔍 Small random reads to measure read IOPS. Simulates database-like workloads with many small requests.',
        'mixed_7030': '⚙️ Realistic mixed workload with 70% reads and 30% writes. Simulates typical application behavior.',
        'large_object': '📦 Upload very large files using multipart transfers. Tests handling of large objects and multipart APIs.',
        'small_file_storm': '🌪️ Many concurrent small file operations with deletes. Tests metadata performance and concurrent access.',
        'endurance': '⏱️ Extended mixed workload running for several minutes. Tests cluster stability and consistency under sustained load.',
        'max_throughput': '🚀 Aggressive concurrent load to find absolute maximum throughput. Stress tests the cluster.',
        'concurrent_clients': '👥 Maximum concurrent client connections with mixed operations. Tests connection handling and scalability.',
    }

    for key in SCENARIOS:
        info = get_scenario_info(key)
        scenario = SCENARIOS[key]

        if info:
            # Calculate total operations
            total_ops = scenario.get('concurrent_ops', 0) * scenario.get('operations_per_thread', 0)

            scenarios_list.append({
                'key': key,
                'name': info['name'],
                'description': info['description'],
                'category': info['category'],
                'concurrent_ops': info['concurrent_ops'],
                'operations_per_thread': scenario.get('operations_per_thread', 0),
                'total_operations': total_ops,
                'object_size_bytes': scenario['object_size'],
                'object_size_mb': scenario['object_size'] / (1024 * 1024),
                'operation_mix': scenario.get('operation_mix', {}),
                'duration_limit': scenario.get('duration_limit', None),
                'use_case': scenario_descriptions.get(key, ''),
            })

    return jsonify({'scenarios': scenarios_list})


if __name__ == '__main__':
    logger.info(f"Starting Ceph S3 Load Testing Tool on {HOST}:{PORT}")
    app.run(debug=DEBUG, host=HOST, port=PORT, threaded=True)
