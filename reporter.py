"""
Report generation and management for load tests.
"""

import json
import csv
import os
from datetime import datetime
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class Reporter:
    """Handles saving, loading, and exporting test reports."""

    def __init__(self, reports_dir='reports'):
        self.reports_dir = Path(reports_dir)
        self.reports_dir.mkdir(exist_ok=True)

    @staticmethod
    def format_latency(latency_ms):
        """Format latency as 'X,XXX ms (X.X s)' format."""
        if latency_ms is None or latency_ms == 0:
            return "0 ms"
        seconds = latency_ms / 1000
        return f"{latency_ms:,.0f} ms ({seconds:.1f}s)"

    def _generate_run_id(self, scenario_key):
        """Generate a unique run ID."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"{scenario_key}_{timestamp}"

    def save_report(self, scenario_key, config, metrics_snapshot, timeseries,
                   bucket_stats_start=None, bucket_stats_end=None, bucket_stats_delta=None, session_id=None):
        """Save a test report to JSON."""
        run_id = self._generate_run_id(scenario_key)

        report = {
            'run_id': run_id,
            'scenario': scenario_key,
            'session_id': session_id,
            'timestamp': datetime.now().isoformat(),
            'config': {
                'concurrent_ops': config.get('concurrent_ops'),
                'operations_per_thread': config.get('operations_per_thread'),
                'object_size_kb': config.get('object_size', 0) / 1024,
                'operation_mix': config.get('operation_mix'),
            },
            'metrics': metrics_snapshot,
            'timeseries': timeseries,
            'bucket_stats': {
                'start': bucket_stats_start,
                'end': bucket_stats_end,
                'delta': bucket_stats_delta,
            } if (bucket_stats_start or bucket_stats_end) else None,
        }

        report_path = self.reports_dir / f"{run_id}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)

        logger.info(f"Report saved: {report_path}")
        return run_id

    def list_reports(self):
        """List all saved reports."""
        reports = []
        for report_file in sorted(self.reports_dir.glob("*.json"), reverse=True):
            try:
                with open(report_file) as f:
                    data = json.load(f)
                    metrics = data['metrics']
                    latency_avg = metrics.get('latency_avg', 0)
                    latency_p50 = metrics.get('latency_p50', 0)
                    latency_p99 = metrics.get('latency_p99', 0)
                    reports.append({
                        'run_id': data['run_id'],
                        'scenario': data['scenario'],
                        'session_id': data.get('session_id', 'N/A'),
                        'timestamp': data['timestamp'],
                        'iops': metrics.get('iops', 0),
                        'throughput_mbs': metrics.get('throughput_mbs', 0),
                        'success_rate': metrics.get('success_rate', 0),
                        'latency_avg': latency_avg,
                        'latency_avg_formatted': self.format_latency(latency_avg),
                        'latency_p50': latency_p50,
                        'latency_p50_formatted': self.format_latency(latency_p50),
                        'latency_p99': latency_p99,
                        'latency_p99_formatted': self.format_latency(latency_p99),
                        'total_ops': metrics.get('total_ops', 0),
                        'data_transferred_mb': metrics.get('data_transferred_mb', 0),
                        'duration': metrics.get('elapsed', 0),
                    })
            except Exception as e:
                logger.error(f"Error reading {report_file}: {e}")

        return reports

    def get_report(self, run_id):
        """Load a report by run ID."""
        report_path = self.reports_dir / f"{run_id}.json"
        if not report_path.exists():
            return None

        with open(report_path) as f:
            return json.load(f)

    def export_csv(self, run_id):
        """Export timeseries data as CSV string."""
        report = self.get_report(run_id)
        if not report:
            return None

        timeseries = report['timeseries']
        if not timeseries:
            return None

        output = []
        fieldnames = [
            'elapsed', 'total_ops', 'successful_ops', 'failed_ops',
            'iops', 'read_iops', 'write_iops', 'throughput_mbs',
            'data_transferred_mb', 'latency_avg', 'latency_p50', 'latency_p90',
            'latency_p95', 'latency_p99', 'latency_min', 'latency_max', 'success_rate'
        ]

        output.append(','.join(fieldnames))
        for point in timeseries:
            row = [str(point.get(field, '')) for field in fieldnames]
            output.append(','.join(row))

        return '\n'.join(output)

    def export_json(self, run_id):
        """Export full report as JSON string."""
        report = self.get_report(run_id)
        if not report:
            return None
        return json.dumps(report, indent=2)

    def compare_reports(self, run_id_a, run_id_b):
        """Compare two reports and return deltas."""
        report_a = self.get_report(run_id_a)
        report_b = self.get_report(run_id_b)

        if not report_a or not report_b:
            return None

        def delta(val_a, val_b):
            """Calculate delta percentage."""
            if val_a == 0:
                return 0 if val_b == 0 else 100
            return round(((val_b - val_a) / val_a) * 100, 2)

        metrics_a = report_a['metrics']
        metrics_b = report_b['metrics']

        return {
            'run_id_a': run_id_a,
            'run_id_b': run_id_b,
            'scenario_a': report_a['scenario'],
            'scenario_b': report_b['scenario'],
            'timestamp_a': report_a['timestamp'],
            'timestamp_b': report_b['timestamp'],
            'comparison': {
                'iops': {
                    'a': metrics_a.get('iops'),
                    'b': metrics_b.get('iops'),
                    'delta_percent': delta(metrics_a.get('iops', 0), metrics_b.get('iops', 0)),
                },
                'throughput_mbs': {
                    'a': metrics_a.get('throughput_mbs'),
                    'b': metrics_b.get('throughput_mbs'),
                    'delta_percent': delta(metrics_a.get('throughput_mbs', 0), metrics_b.get('throughput_mbs', 0)),
                },
                'latency_avg': {
                    'a': metrics_a.get('latency_avg'),
                    'b': metrics_b.get('latency_avg'),
                    'delta_percent': delta(metrics_a.get('latency_avg', 0), metrics_b.get('latency_avg', 0)),
                },
                'success_rate': {
                    'a': metrics_a.get('success_rate'),
                    'b': metrics_b.get('success_rate'),
                    'delta_percent': delta(metrics_a.get('success_rate', 0), metrics_b.get('success_rate', 0)),
                },
                'total_data_mb': {
                    'a': metrics_a.get('data_transferred_mb'),
                    'b': metrics_b.get('data_transferred_mb'),
                    'delta_percent': delta(metrics_a.get('data_transferred_mb', 0), metrics_b.get('data_transferred_mb', 0)),
                },
            },
        }
