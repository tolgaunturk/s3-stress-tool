"""
Predefined load test scenarios for Ceph S3 platform.
Each scenario targets specific performance characteristics.
"""

SCENARIOS = {
    'sequential_write': {
        'name': 'Sequential Write',
        'description': 'Large sequential write operations to measure throughput',
        'category': 'throughput',
        'concurrent_ops': 10,
        'operations_per_thread': 50,
        'object_size': 10 * 1024 * 1024,  # 10 MB
        'operation_mix': {'put': 100},
        'multipart_threshold': 5 * 1024 * 1024,  # 5 MB
    },
    'sequential_read': {
        'name': 'Sequential Read',
        'description': 'Large sequential read operations to measure read throughput',
        'category': 'throughput',
        'concurrent_ops': 10,
        'operations_per_thread': 50,
        'object_size': 10 * 1024 * 1024,  # 10 MB
        'operation_mix': {'get': 100},
        'multipart_threshold': 5 * 1024 * 1024,
    },
    'random_write_iops': {
        'name': 'Random Write IOPS',
        'description': 'Small random writes to measure write IOPS',
        'category': 'iops',
        'concurrent_ops': 32,
        'operations_per_thread': 100,
        'object_size': 4 * 1024,  # 4 KB
        'operation_mix': {'put': 100},
        'multipart_threshold': 5 * 1024 * 1024,
    },
    'random_read_iops': {
        'name': 'Random Read IOPS',
        'description': 'Small random reads to measure read IOPS',
        'category': 'iops',
        'concurrent_ops': 32,
        'operations_per_thread': 100,
        'object_size': 4 * 1024,  # 4 KB
        'operation_mix': {'get': 100},
        'multipart_threshold': 5 * 1024 * 1024,
    },
    'mixed_7030': {
        'name': 'Mixed 70/30 Read/Write',
        'description': 'Realistic read-heavy workload (70% read, 30% write)',
        'category': 'mixed',
        'concurrent_ops': 20,
        'operations_per_thread': 75,
        'object_size': 1 * 1024 * 1024,  # 1 MB
        'operation_mix': {'get': 70, 'put': 30},
        'multipart_threshold': 5 * 1024 * 1024,
    },
    'large_object': {
        'name': 'Large Object Upload',
        'description': 'Large object uploads with multipart transfers',
        'category': 'throughput',
        'concurrent_ops': 5,
        'operations_per_thread': 20,
        'object_size': 100 * 1024 * 1024,  # 100 MB
        'operation_mix': {'put': 100},
        'multipart_threshold': 5 * 1024 * 1024,  # Uses multipart for 100MB objects
    },
    'small_file_storm': {
        'name': 'Small File Storm',
        'description': 'High concurrent metadata operations with small files',
        'category': 'metadata',
        'concurrent_ops': 50,
        'operations_per_thread': 80,
        'object_size': 4 * 1024,  # 4 KB
        'operation_mix': {'put': 50, 'list': 30, 'delete': 20},
        'multipart_threshold': 5 * 1024 * 1024,
    },
    'endurance': {
        'name': 'Endurance Test',
        'description': 'Extended mixed workload to test stability over time',
        'category': 'endurance',
        'concurrent_ops': 20,
        'operations_per_thread': 150,  # 3000 total ops
        'object_size': 1 * 1024 * 1024,  # 1 MB
        'operation_mix': {'put': 40, 'get': 40, 'list': 10, 'delete': 10},
        'multipart_threshold': 5 * 1024 * 1024,
        'duration_limit': 300,  # 5 minutes max
    },
    'max_throughput': {
        'name': 'Maximum Throughput',
        'description': 'Aggressive concurrent load to find max cluster throughput',
        'category': 'throughput',
        'concurrent_ops': 50,
        'operations_per_thread': 40,
        'object_size': 50 * 1024 * 1024,  # 50 MB
        'operation_mix': {'put': 50, 'get': 50},
        'multipart_threshold': 5 * 1024 * 1024,
    },
    'real_file_upload': {
        'name': 'Real File Upload (100MB)',
        'description': 'Upload large real files from disk (100MB each) - realistic scenario',
        'category': 'real-world',
        'concurrent_ops': 5,
        'operations_per_thread': 10000,  # High number for duration-based tests
        'object_size': 100 * 1024 * 1024,  # 100 MB
        'operation_mix': {'put': 100},
        'multipart_threshold': 5 * 1024 * 1024,
        'use_real_files': True,
        'duration_limit': 300,  # Default 5 minutes
    },
    'bulk_migration': {
        'name': 'Bulk Migration (10MB files)',
        'description': 'Upload many medium-size files in parallel (like data migration)',
        'category': 'real-world',
        'concurrent_ops': 20,
        'operations_per_thread': 10000,  # High number for duration-based tests
        'object_size': 10 * 1024 * 1024,  # 10 MB per file
        'operation_mix': {'put': 100},
        'multipart_threshold': 5 * 1024 * 1024,
        'use_real_files': True,
        'duration_limit': 300,  # Default 5 minutes
    },
    'concurrent_clients': {
        'name': 'Concurrent Clients',
        'description': 'Maximum concurrent connections with mixed operations',
        'category': 'concurrency',
        'concurrent_ops': 100,
        'operations_per_thread': 50,
        'object_size': 512 * 1024,  # 512 KB
        'operation_mix': {'get': 50, 'put': 30, 'list': 20},
        'multipart_threshold': 5 * 1024 * 1024,
    },
    'gigabyte_7030': {
        'name': 'Gigabyte Files (70% Write / 30% Read)',
        'description': 'Large 1GB file operations with 70% write, 30% read mix',
        'category': 'throughput',
        'concurrent_ops': 3,
        'operations_per_thread': 10,
        'object_size': 1 * 1024 * 1024 * 1024,  # 1 GB
        'operation_mix': {'put': 70, 'get': 30},
        'multipart_threshold': 5 * 1024 * 1024,
    },
    'retention_governance': {
        'name': 'Retention Lock — GOVERNANCE Mode',
        'description': 'Write 1MB objects with GOVERNANCE retention lock (60s). Tests lock performance and throughput. Requires Object Lock enabled bucket.',
        'category': 'retention',
        'concurrent_ops': 10,
        'operations_per_thread': 100,
        'object_size': 1 * 1024 * 1024,  # 1 MB
        'operation_mix': {'put_locked': 100},
        'retention_mode': 'GOVERNANCE',
        'retention_seconds': 60,
        'multipart_threshold': 5 * 1024 * 1024,
    },
    'retention_compliance': {
        'name': 'Retention Lock — COMPLIANCE Mode',
        'description': 'Write 1MB objects with COMPLIANCE retention lock (60s). Strictest WORM — no one can delete until expiry. Requires Object Lock enabled bucket.',
        'category': 'retention',
        'concurrent_ops': 10,
        'operations_per_thread': 100,
        'object_size': 1 * 1024 * 1024,  # 1 MB
        'operation_mix': {'put_locked': 100},
        'retention_mode': 'COMPLIANCE',
        'retention_seconds': 60,
        'multipart_threshold': 5 * 1024 * 1024,
    },
    'retention_worm_verify': {
        'name': 'WORM Verification (Write + Delete Attempt)',
        'description': 'Write locked objects (GOVERNANCE, 5min lock) then attempt deletions. Success rate = % of deletes correctly blocked by retention lock.',
        'category': 'retention',
        'concurrent_ops': 5,
        'operations_per_thread': 100,
        'object_size': 512 * 1024,  # 512 KB — small so puts complete fast, leaving time for delete attempts
        'operation_mix': {'put_locked': 60, 'delete_locked': 40},
        'retention_mode': 'GOVERNANCE',
        'retention_seconds': 300,  # 5-minute lock so objects stay locked throughout the test
        'multipart_threshold': 5 * 1024 * 1024,
    },
}


def get_scenario(key):
    """Get a scenario by key."""
    return SCENARIOS.get(key)


def list_scenarios():
    """Return list of all scenarios."""
    return list(SCENARIOS.keys())


def get_scenario_info(key):
    """Get scenario info for API response."""
    scenario = SCENARIOS.get(key)
    if not scenario:
        return None
    return {
        'key': key,
        'name': scenario['name'],
        'description': scenario['description'],
        'category': scenario['category'],
        'concurrent_ops': scenario['concurrent_ops'],
        'object_size_mb': scenario['object_size'] / (1024 * 1024),
        'operation_mix': scenario['operation_mix'],
    }
