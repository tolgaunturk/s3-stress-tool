"""
Ceph S3 Load Testing Tool - Flask Web Application
"""

from flask import Flask, render_template, request, jsonify, send_file
from flask_cors import CORS
import boto3
from botocore.exceptions import ClientError
import logging
import os
import json
import uuid
import time
from pathlib import Path
from io import StringIO, BytesIO
from datetime import datetime, timezone, timedelta

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
    """Create S3 client for AWS S3 or any S3-compatible endpoint (Ceph, MinIO, etc.).
    Leave endpoint_url blank/None to connect to native AWS S3.
    """
    from botocore.config import Config
    kwargs = {
        'aws_access_key_id': access_key,
        'aws_secret_access_key': secret_key,
        'region_name': region if region not in ('default', '', None) else 'us-east-1',
        'config': Config(connect_timeout=10, read_timeout=30, retries={'max_attempts': 1}),
    }
    if endpoint_url and endpoint_url.strip():
        kwargs['endpoint_url'] = endpoint_url.strip()
    return boto3.client('s3', **kwargs)


def capture_bucket_stats(s3_client, bucket_name):
    """Capture bucket statistics at a point in time (paginated, recurses through all folders).

    Relies on the S3 client's connect/read timeouts (set in create_s3_client) to bound
    hangs — signal.alarm cannot be used here since Flask runs with threaded=True and
    SIGALRM only works on the main interpreter thread.
    """
    try:
        objects = []
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)

        for page in pages:
            if 'Contents' in page:
                objects.extend(page['Contents'])

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


def capture_bucket_version_stats(s3_client, bucket_name):
    """Sum ALL object versions (current + noncurrent) + delete markers via list_object_versions.

    On a versioned bucket, list_objects_v2 (used by capture_bucket_stats) only sees the
    current, non-deleted state of each key — it misses noncurrent versions entirely, which
    is exactly what's usually still retention-locked and un-purgeable. This is the true
    storage picture; the discrepancy between this and capture_bucket_stats() is the signal
    that a bucket "looks empty" but isn't.
    """
    try:
        version_count = 0
        delete_marker_count = 0
        total_size_bytes = 0
        paginator = s3_client.get_paginator('list_object_versions')
        pages = paginator.paginate(Bucket=bucket_name)

        for page in pages:
            for v in page.get('Versions', []):
                version_count += 1
                total_size_bytes += v.get('Size', 0)
            delete_marker_count += len(page.get('DeleteMarkers', []))

        return {
            'version_count': version_count,
            'delete_marker_count': delete_marker_count,
            'total_size_bytes': total_size_bytes,
        }
    except Exception as e:
        logger.error(f"Error capturing version stats for {bucket_name}: {e}")
        return None


def _creds_from_request(data):
    """Extract and validate S3 connection params from a request payload."""
    endpoint_url = data.get('endpoint_url', '').strip()
    access_key   = data.get('access_key', '').strip()
    secret_key   = data.get('secret_key', '').strip()
    region       = data.get('region', 'us-east-1')
    bucket       = data.get('bucket_name', '').strip()
    if not (access_key and secret_key):
        raise ValueError('access_key and secret_key are required')
    return create_s3_client(endpoint_url or None, access_key, secret_key, region), bucket


@app.route('/api/manager/list-buckets', methods=['POST'])
def manager_list_buckets():
    """List all buckets accessible with the given credentials."""
    try:
        s3, _ = _creds_from_request(request.json)
        resp = s3.list_buckets()
        buckets = [
            {'name': b['Name'], 'created': b['CreationDate'].isoformat()}
            for b in resp.get('Buckets', [])
        ]
        return jsonify({'buckets': buckets})
    except Exception as e:
        logger.error(f"list-buckets error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/manager/create-bucket', methods=['POST'])
def manager_create_bucket():
    """Create a new S3 bucket."""
    try:
        data = request.json
        new_bucket = data.get('new_bucket_name', '').strip()
        if not new_bucket:
            return jsonify({'error': 'new_bucket_name is required'}), 400

        s3, _ = _creds_from_request(data)
        region = data.get('region', 'us-east-1')

        kwargs = {'Bucket': new_bucket}
        # us-east-1 must NOT send a LocationConstraint (AWS quirk)
        if region and region not in ('default', 'us-east-1', ''):
            kwargs['CreateBucketConfiguration'] = {'LocationConstraint': region}

        s3.create_bucket(**kwargs)
        logger.info(f"Created bucket: {new_bucket}")
        return jsonify({'status': 'created', 'bucket': new_bucket})
    except ClientError as e:
        code = e.response['Error']['Code']
        msg  = e.response['Error'].get('Message', str(e))
        logger.warning(f"create-bucket ClientError: {code} — {msg}")
        return jsonify({'error': f'{code}: {msg}'}), 400
    except Exception as e:
        logger.error(f"create-bucket error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/manager/delete-bucket', methods=['POST'])
def manager_delete_bucket():
    """Delete a bucket. Bucket MUST be empty — this endpoint never force-deletes."""
    try:
        data = request.json
        target = data.get('target_bucket', '').strip()
        if not target:
            return jsonify({'error': 'target_bucket is required'}), 400

        s3, _ = _creds_from_request(data)

        # Safety: refuse if bucket has any objects
        check = s3.list_objects_v2(Bucket=target, MaxKeys=1)
        if check.get('KeyCount', 0) > 0:
            return jsonify({'error': 'Bucket is not empty. Delete all objects first.'}), 400

        s3.delete_bucket(Bucket=target)
        logger.info(f"Deleted bucket: {target}")
        return jsonify({'status': 'deleted', 'bucket': target})
    except ClientError as e:
        code = e.response['Error']['Code']
        msg  = e.response['Error'].get('Message', str(e))
        return jsonify({'error': f'{code}: {msg}'}), 400
    except Exception as e:
        logger.error(f"delete-bucket error: {e}")
        return jsonify({'error': str(e)}), 500


CANNED_ACLS = {'private', 'public-read', 'public-read-write', 'authenticated-read'}
ACL_GROUP_URIS = {
    'AllUsers': 'http://acs.amazonaws.com/groups/global/AllUsers',
    'AuthenticatedUsers': 'http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
}


@app.route('/api/manager/get-bucket-acl', methods=['POST'])
def manager_get_bucket_acl():
    """Read the current ACL (owner + grants) for a bucket."""
    try:
        data = request.json
        s3, bucket = _creds_from_request(data)
        if not bucket:
            return jsonify({'error': 'bucket_name is required'}), 400

        resp = s3.get_bucket_acl(Bucket=bucket)
        owner = resp.get('Owner', {})
        grants = []
        for g in resp.get('Grants', []):
            grantee = g.get('Grantee', {})
            grants.append({
                'type': grantee.get('Type'),
                'id': grantee.get('ID'),
                'uri': grantee.get('URI'),
                'email': grantee.get('EmailAddress'),
                'display_name': grantee.get('DisplayName'),
                'permission': g.get('Permission'),
            })
        return jsonify({'bucket': bucket, 'owner': owner, 'grants': grants})
    except ClientError as e:
        code = e.response['Error']['Code']
        msg  = e.response['Error'].get('Message', str(e))
        return jsonify({'error': f'{code}: {msg}'}), 400
    except Exception as e:
        logger.error(f"get-bucket-acl error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/manager/put-bucket-acl', methods=['POST'])
def manager_put_bucket_acl():
    """Apply a canned ACL, or a full custom grant list, to a bucket.

    Two mutually exclusive modes:
      - canned_acl: one of CANNED_ACLS — simplest path, single s3:PutBucketAcl call.
      - owner + grants: full replacement grant list (S3 ACL PUT always replaces,
        never merges — the frontend fetches the current ACL, edits it locally,
        then sends the complete desired list back here).
    """
    try:
        data = request.json
        s3, bucket = _creds_from_request(data)
        if not bucket:
            return jsonify({'error': 'bucket_name is required'}), 400

        canned = (data.get('canned_acl') or '').strip()
        if canned:
            if canned not in CANNED_ACLS:
                return jsonify({'error': f'Invalid canned ACL. Must be one of: {", ".join(sorted(CANNED_ACLS))}'}), 400
            s3.put_bucket_acl(Bucket=bucket, ACL=canned)
            logger.info(f"Applied canned ACL '{canned}' to bucket {bucket}")
            return jsonify({'status': 'applied', 'bucket': bucket, 'canned_acl': canned})

        owner = data.get('owner')
        grants_in = data.get('grants', [])
        if not owner or not grants_in:
            return jsonify({'error': 'Either canned_acl, or owner + grants, are required'}), 400

        boto_grants = []
        for g in grants_in:
            grantee = {'Type': g.get('type')}
            if g.get('type') == 'CanonicalUser':
                grantee['ID'] = g.get('id', '')
            elif g.get('type') == 'Group':
                grantee['URI'] = g.get('uri', '')
            elif g.get('type') == 'AmazonCustomerByEmail':
                grantee['EmailAddress'] = g.get('email', '')
            boto_grants.append({'Grantee': grantee, 'Permission': g.get('permission')})

        s3.put_bucket_acl(Bucket=bucket, AccessControlPolicy={'Owner': owner, 'Grants': boto_grants})
        logger.info(f"Applied {len(boto_grants)} custom ACL grant(s) to bucket {bucket}")
        return jsonify({'status': 'applied', 'bucket': bucket, 'grant_count': len(boto_grants)})
    except ClientError as e:
        code = e.response['Error']['Code']
        msg  = e.response['Error'].get('Message', str(e))
        return jsonify({'error': f'{code}: {msg}'}), 400
    except Exception as e:
        logger.error(f"put-bucket-acl error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/manager/list-objects', methods=['POST'])
def manager_list_objects():
    """List objects and sub-folders at a given prefix inside a bucket."""
    try:
        data = request.json
        prefix = data.get('prefix', '')
        s3, bucket = _creds_from_request(data)
        if not bucket:
            return jsonify({'error': 'bucket_name is required'}), 400

        resp = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter='/',
            MaxKeys=500,
        )

        folders = [p['Prefix'] for p in resp.get('CommonPrefixes', [])]
        objects = [
            {
                'key':  o['Key'],
                'size': o['Size'],
                'last_modified': o['LastModified'].isoformat(),
            }
            for o in resp.get('Contents', [])
            if o['Key'] != prefix  # exclude the folder marker itself
        ]
        return jsonify({
            'bucket':  bucket,
            'prefix':  prefix,
            'folders': folders,
            'objects': objects,
            'truncated': resp.get('IsTruncated', False),
        })
    except ClientError as e:
        code = e.response['Error']['Code']
        msg  = e.response['Error'].get('Message', str(e))
        return jsonify({'error': f'{code}: {msg}'}), 400
    except Exception as e:
        logger.error(f"list-objects error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/manager/create-folder', methods=['POST'])
def manager_create_folder():
    """Create a folder by writing an empty object with a trailing-slash key."""
    try:
        data = request.json
        folder_name = data.get('folder_name', '').strip().strip('/')
        prefix      = data.get('prefix', '')
        if not folder_name:
            return jsonify({'error': 'folder_name is required'}), 400

        s3, bucket = _creds_from_request(data)
        if not bucket:
            return jsonify({'error': 'bucket_name is required'}), 400

        key = f"{prefix}{folder_name}/"
        s3.put_object(Bucket=bucket, Key=key, Body=b'')
        logger.info(f"Created folder: s3://{bucket}/{key}")
        return jsonify({'status': 'created', 'key': key, 'bucket': bucket})
    except ClientError as e:
        code = e.response['Error']['Code']
        msg  = e.response['Error'].get('Message', str(e))
        return jsonify({'error': f'{code}: {msg}'}), 400
    except Exception as e:
        logger.error(f"create-folder error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/manager/delete-object', methods=['POST'])
def manager_delete_object():
    """Delete a single object from a bucket."""
    try:
        data = request.json
        key = data.get('key', '').strip()
        if not key:
            return jsonify({'error': 'key is required'}), 400

        s3, bucket = _creds_from_request(data)
        if not bucket:
            return jsonify({'error': 'bucket_name is required'}), 400

        s3.delete_object(Bucket=bucket, Key=key)
        logger.info(f"Deleted object: s3://{bucket}/{key}")
        return jsonify({'status': 'deleted', 'key': key, 'bucket': bucket})
    except ClientError as e:
        code = e.response['Error']['Code']
        msg  = e.response['Error'].get('Message', str(e))
        return jsonify({'error': f'{code}: {msg}'}), 400
    except Exception as e:
        logger.error(f"delete-object error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/manager/delete-objects', methods=['POST'])
def manager_delete_objects():
    """Bulk delete up to 1000 objects from a bucket."""
    try:
        data = request.json
        keys = data.get('keys', [])
        if not keys:
            return jsonify({'error': 'keys array is required'}), 400

        s3, bucket = _creds_from_request(data)
        if not bucket:
            return jsonify({'error': 'bucket_name is required'}), 400

        to_delete = [{'Key': k} for k in keys[:1000]]
        resp = s3.delete_objects(
            Bucket=bucket,
            Delete={'Objects': to_delete, 'Quiet': False}
        )
        deleted = [d['Key'] for d in resp.get('Deleted', [])]
        errors  = resp.get('Errors', [])
        logger.info(f"Bulk deleted {len(deleted)} objects from s3://{bucket}/, {len(errors)} errors")
        return jsonify({'status': 'done', 'deleted': deleted, 'errors': errors, 'bucket': bucket})
    except ClientError as e:
        code = e.response['Error']['Code']
        msg  = e.response['Error'].get('Message', str(e))
        return jsonify({'error': f'{code}: {msg}'}), 400
    except Exception as e:
        logger.error(f"delete-objects error: {e}")
        return jsonify({'error': str(e)}), 500


MAX_VERSION_LIST_ENTRIES = 2000


@app.route('/api/manager/list-object-versions', methods=['POST'])
def manager_list_object_versions():
    """List all object versions + delete markers in a bucket, optionally filtered by prefix.

    Unlike regular object listing (list_objects_v2), this surfaces noncurrent versions and
    delete markers too — the storage that's invisible to the normal Folders & Objects view on
    a versioned bucket, and the only way to find+purge specific stale versions (e.g. leaked
    policy-probe test objects) by Key+VersionId.
    """
    try:
        data = request.json
        s3, bucket = _creds_from_request(data)
        if not bucket:
            return jsonify({'error': 'bucket_name is required'}), 400
        prefix = data.get('prefix', '')

        entries = []
        truncated = False
        paginator = s3.get_paginator('list_object_versions')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            for v in page.get('Versions', []):
                entries.append({
                    'key': v['Key'], 'version_id': v['VersionId'], 'is_latest': v.get('IsLatest', False),
                    'size': v.get('Size', 0), 'last_modified': v['LastModified'].isoformat(), 'type': 'version',
                })
            for d in page.get('DeleteMarkers', []):
                entries.append({
                    'key': d['Key'], 'version_id': d['VersionId'], 'is_latest': d.get('IsLatest', False),
                    'size': 0, 'last_modified': d['LastModified'].isoformat(), 'type': 'delete_marker',
                })
            if len(entries) >= MAX_VERSION_LIST_ENTRIES:
                truncated = True
                break

        entries.sort(key=lambda e: e['last_modified'], reverse=True)
        return jsonify({'bucket': bucket, 'prefix': prefix, 'entries': entries, 'truncated': truncated})
    except ClientError as e:
        code = e.response['Error']['Code']
        msg = e.response['Error'].get('Message', str(e))
        return jsonify({'error': f'{code}: {msg}'}), 400
    except Exception as e:
        logger.error(f"list-object-versions error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/manager/delete-object-versions', methods=['POST'])
def manager_delete_object_versions():
    """Bulk delete specific object versions (or delete markers) by Key+VersionId.

    This is the only way to actually purge noncurrent versions — the regular delete/delete-objects
    endpoints only ever target the current version of a key.
    """
    try:
        data = request.json
        s3, bucket = _creds_from_request(data)
        if not bucket:
            return jsonify({'error': 'bucket_name is required'}), 400
        versions = data.get('versions', [])
        if not versions:
            return jsonify({'error': 'versions array is required'}), 400

        to_delete = [{'Key': v['key'], 'VersionId': v['version_id']} for v in versions[:1000]]
        resp = s3.delete_objects(Bucket=bucket, Delete={'Objects': to_delete, 'Quiet': False})
        deleted = [{'key': d['Key'], 'version_id': d.get('VersionId')} for d in resp.get('Deleted', [])]
        errors = resp.get('Errors', [])
        logger.info(f"Bulk deleted {len(deleted)} object version(s) from s3://{bucket}/, {len(errors)} errors")
        return jsonify({'status': 'done', 'deleted': deleted, 'errors': errors, 'bucket': bucket})
    except ClientError as e:
        code = e.response['Error']['Code']
        msg = e.response['Error'].get('Message', str(e))
        return jsonify({'error': f'{code}: {msg}'}), 400
    except Exception as e:
        logger.error(f"delete-object-versions error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/manager/empty-bucket', methods=['POST'])
def manager_empty_bucket():
    """Delete ALL objects in a bucket across all pages (paginated).
    Pass delete_bucket=True to also delete the bucket itself when empty.
    Returns total objects deleted.
    """
    try:
        data = request.json
        s3, bucket = _creds_from_request(data)
        if not bucket:
            return jsonify({'error': 'bucket_name is required'}), 400

        delete_bucket_after = data.get('delete_bucket', False)
        deleted_count = 0
        continuation_token = None

        while True:
            kwargs = {'Bucket': bucket, 'MaxKeys': 1000}
            if continuation_token:
                kwargs['ContinuationToken'] = continuation_token

            resp = s3.list_objects_v2(**kwargs)
            objects = resp.get('Contents', [])
            if not objects:
                break

            to_delete = [{'Key': o['Key']} for o in objects]
            s3.delete_objects(Bucket=bucket, Delete={'Objects': to_delete, 'Quiet': True})
            deleted_count += len(objects)

            if resp.get('IsTruncated'):
                continuation_token = resp.get('NextContinuationToken')
            else:
                break

        if delete_bucket_after:
            s3.delete_bucket(Bucket=bucket)
            logger.info(f"Emptied and deleted bucket: {bucket} ({deleted_count} objects)")
            return jsonify({'status': 'deleted', 'bucket': bucket, 'deleted_count': deleted_count})

        logger.info(f"Emptied bucket: {bucket} ({deleted_count} objects)")
        return jsonify({'status': 'emptied', 'bucket': bucket, 'deleted_count': deleted_count})
    except ClientError as e:
        code = e.response['Error']['Code']
        msg  = e.response['Error'].get('Message', str(e))
        return jsonify({'error': f'{code}: {msg}'}), 400
    except Exception as e:
        logger.error(f"empty-bucket error: {e}")
        return jsonify({'error': str(e)}), 500


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


def _cleanup_probe_object(s3, bucket_name, key, max_attempts=3, wait_seconds=2):
    """Best-effort cleanup for a policy-probe test object.

    A GOVERNANCE-locked object becomes deletable once its retention window naturally
    expires, so retry with a short backoff instead of giving up after one failed attempt —
    a silent one-shot failure here permanently leaks a version into the bucket every time
    the probe runs against a bucket where BypassGovernanceRetention is denied.
    """
    for attempt in range(max_attempts):
        try:
            s3.delete_object(Bucket=bucket_name, Key=key)
            return True
        except Exception as e:
            if attempt < max_attempts - 1:
                time.sleep(wait_seconds)
            else:
                logger.warning(f"Policy probe cleanup failed for s3://{bucket_name}/{key} after {max_attempts} attempts: {e}")
    return False


@app.route('/api/policy/probe', methods=['POST'])
def policy_probe():
    """Run a permission matrix scan against a bucket with the given credentials.
    Tests each S3 operation and reports ALLOWED / DENIED / ERROR / SKIPPED.
    Safe to run — uses a short-lived probe object that is cleaned up automatically.
    """
    try:
        data = request.json
        bucket_name = data.get('bucket_name', '').strip()
        endpoint_url = data.get('endpoint_url', '').strip()
        access_key = data.get('access_key', '').strip()
        secret_key = data.get('secret_key', '').strip()
        region = data.get('region', 'us-east-1')

        if not all([bucket_name, access_key, secret_key]):
            return jsonify({'error': 'bucket_name, access_key and secret_key are required'}), 400

        s3 = create_s3_client(endpoint_url or None, access_key, secret_key, region)
        results = {}
        PROBE_KEY = '__policy_probe_test__'

        # Errors that mean "allowed but nothing configured yet"
        EMPTY_CONFIG_CODES = {
            'NoSuchCORSConfiguration', 'NoSuchBucketPolicy',
            'NoSuchLifecycleConfiguration', 'ReplicationConfigurationNotFoundError',
            'ObjectLockConfigurationNotFoundError', 'NoSuchObjectLockConfiguration',
        }
        DENIED_CODES = {'AccessDenied', 'AllAccessDisabled', 'Forbidden', '403'}

        def probe(name, fn, skip_if_denied=None):
            """Run fn(); classify result as ALLOWED / DENIED / ERROR / SKIPPED."""
            if skip_if_denied and results.get(skip_if_denied, {}).get('status') == 'DENIED':
                results[name] = {'status': 'SKIPPED', 'detail': f'Skipped — {skip_if_denied} was DENIED'}
                return
            try:
                fn()
                results[name] = {'status': 'ALLOWED', 'detail': None}
            except ClientError as e:
                code = e.response['Error']['Code']
                msg = e.response['Error'].get('Message', '')
                if code in EMPTY_CONFIG_CODES:
                    results[name] = {'status': 'ALLOWED', 'detail': f'Not configured ({code})'}
                elif code in DENIED_CODES or 'AccessDenied' in msg:
                    results[name] = {'status': 'DENIED', 'detail': code}
                else:
                    results[name] = {'status': 'ERROR', 'detail': code}
            except Exception as e:
                results[name] = {'status': 'ERROR', 'detail': str(e)[:80]}

        # --- Bucket-level operations ---
        probe('s3:HeadBucket',             lambda: s3.head_bucket(Bucket=bucket_name))
        probe('s3:ListBucket',             lambda: s3.list_objects_v2(Bucket=bucket_name, MaxKeys=1))
        probe('s3:GetBucketAcl',           lambda: s3.get_bucket_acl(Bucket=bucket_name))
        probe('s3:GetBucketPolicy',        lambda: s3.get_bucket_policy(Bucket=bucket_name))

        # GetBucketVersioning: capture actual versioning status in Detail column
        try:
            vr = s3.get_bucket_versioning(Bucket=bucket_name)
            vstatus = vr.get('Status') or 'Disabled (never enabled)'
            mfa = vr.get('MFADelete')
            detail = f'Versioning: {vstatus}' + (f' | MFA Delete: {mfa}' if mfa else '')
            results['s3:GetBucketVersioning'] = {'status': 'ALLOWED', 'detail': detail}
        except ClientError as e:
            code = e.response['Error']['Code']
            msg  = e.response['Error'].get('Message', '')
            if code in DENIED_CODES or 'AccessDenied' in msg:
                results['s3:GetBucketVersioning'] = {'status': 'DENIED', 'detail': code}
            else:
                results['s3:GetBucketVersioning'] = {'status': 'ERROR', 'detail': code}
        except Exception as e:
            results['s3:GetBucketVersioning'] = {'status': 'ERROR', 'detail': str(e)[:80]}

        probe('s3:GetBucketCors',          lambda: s3.get_bucket_cors(Bucket=bucket_name))
        probe('s3:GetObjectLockConfiguration', lambda: s3.get_object_lock_configuration(Bucket=bucket_name))

        # --- Object operations: write probe object first, then read/head/delete ---
        probe('s3:PutObject',    lambda: s3.put_object(Bucket=bucket_name, Key=PROBE_KEY, Body=b'probe'))

        if results.get('s3:PutObject', {}).get('status') == 'ALLOWED':
            probe('s3:GetObject',    lambda: s3.get_object(Bucket=bucket_name, Key=PROBE_KEY))
            probe('s3:HeadObject',   lambda: s3.head_object(Bucket=bucket_name, Key=PROBE_KEY))
            probe('s3:GetObjectAcl', lambda: s3.get_object_acl(Bucket=bucket_name, Key=PROBE_KEY))
            probe('s3:DeleteObject', lambda: s3.delete_object(Bucket=bucket_name, Key=PROBE_KEY))
        else:
            for op in ('s3:GetObject', 's3:HeadObject', 's3:GetObjectAcl', 's3:DeleteObject'):
                results[op] = {'status': 'SKIPPED', 'detail': 'Skipped — PutObject was not ALLOWED'}

        # Clean up probe object if it wasn't deleted (e.g. DeleteObject was DENIED)
        if results.get('s3:PutObject', {}).get('status') == 'ALLOWED' and \
           results.get('s3:DeleteObject', {}).get('status') != 'ALLOWED':
            try:
                s3.delete_object(Bucket=bucket_name, Key=PROBE_KEY)
            except Exception:
                pass

        # --- Write / Control-Plane operations (commonly restricted by providers) ---
        import uuid as _uuid

        # CreateBucket: use a short-lived probe bucket name
        _probe_bucket = f'probe-{_uuid.uuid4().hex[:10]}'
        _probe_bucket_created = False
        try:
            _cb_kwargs = {'Bucket': _probe_bucket}
            if region and region not in ('default', 'us-east-1', ''):
                _cb_kwargs['CreateBucketConfiguration'] = {'LocationConstraint': region}
            s3.create_bucket(**_cb_kwargs)
            results['s3:CreateBucket'] = {'status': 'ALLOWED', 'detail': f'Created probe bucket ({_probe_bucket})'}
            _probe_bucket_created = True
        except ClientError as e:
            _code = e.response['Error']['Code']
            _msg = e.response['Error'].get('Message', '')
            if _code in DENIED_CODES or 'AccessDenied' in _msg:
                results['s3:CreateBucket'] = {'status': 'DENIED', 'detail': _code}
            else:
                results['s3:CreateBucket'] = {'status': 'ERROR', 'detail': _code}
        except Exception as e:
            results['s3:CreateBucket'] = {'status': 'ERROR', 'detail': str(e)[:80]}

        # DeleteBucket: delete the probe bucket created above
        if _probe_bucket_created:
            try:
                s3.delete_bucket(Bucket=_probe_bucket)
                results['s3:DeleteBucket'] = {'status': 'ALLOWED', 'detail': f'Deleted probe bucket ({_probe_bucket})'}
            except ClientError as e:
                _code = e.response['Error']['Code']
                _msg = e.response['Error'].get('Message', '')
                if _code in DENIED_CODES or 'AccessDenied' in _msg:
                    results['s3:DeleteBucket'] = {'status': 'DENIED', 'detail': _code + ' (probe bucket left behind — delete manually)'}
                else:
                    results['s3:DeleteBucket'] = {'status': 'ERROR', 'detail': _code}
            except Exception as e:
                results['s3:DeleteBucket'] = {'status': 'ERROR', 'detail': str(e)[:80]}
        else:
            results['s3:DeleteBucket'] = {'status': 'SKIPPED', 'detail': 'Skipped — CreateBucket was not ALLOWED'}

        # PutBucketAcl: set to 'private' (same as default — no-op, safe)
        probe('s3:PutBucketAcl', lambda: s3.put_bucket_acl(Bucket=bucket_name, ACL='private'))

        # PutBucketVersioning: re-set current status if already set;
        # if never enabled, use Suspended (only way to test — side effect: leaves bucket in Suspended state)
        _vd = results.get('s3:GetBucketVersioning', {}).get('detail', '')
        if 'Enabled' in _vd:
            probe('s3:PutBucketVersioning', lambda: s3.put_bucket_versioning(
                Bucket=bucket_name, VersioningConfiguration={'Status': 'Enabled'}))
        elif 'Suspended' in _vd:
            probe('s3:PutBucketVersioning', lambda: s3.put_bucket_versioning(
                Bucket=bucket_name, VersioningConfiguration={'Status': 'Suspended'}))
        else:
            # Never enabled — try Suspended (will deny immediately if no permission; if allowed,
            # leaves bucket in Suspended state which is the lowest-impact versioning state)
            try:
                s3.put_bucket_versioning(Bucket=bucket_name,
                                         VersioningConfiguration={'Status': 'Suspended'})
                results['s3:PutBucketVersioning'] = {'status': 'ALLOWED', 'detail': 'Versioning set to Suspended on probe bucket'}
            except ClientError as e:
                _code = e.response['Error']['Code']
                _msg = e.response['Error'].get('Message', '')
                if _code in DENIED_CODES or 'AccessDenied' in _msg:
                    results['s3:PutBucketVersioning'] = {'status': 'DENIED', 'detail': _code}
                else:
                    results['s3:PutBucketVersioning'] = {'status': 'ERROR', 'detail': _code}
            except Exception as e:
                results['s3:PutBucketVersioning'] = {'status': 'ERROR', 'detail': str(e)[:80]}

        # PutBucketObjectLockConfiguration: non-lock buckets return InvalidBucketState (ALLOWED but not configured)
        _LOCK_CFG_CODES = {'InvalidBucketState', 'InvalidRequest', 'OperationNotSupported',
                           'ObjectLockConfigurationNotSupportedError'}
        try:
            s3.put_object_lock_configuration(
                Bucket=bucket_name,
                ObjectLockConfiguration={'ObjectLockEnabled': 'Enabled'})
            results['s3:PutBucketObjectLockConfiguration'] = {'status': 'ALLOWED', 'detail': 'Object Lock enabled/re-confirmed'}
        except ClientError as e:
            _code = e.response['Error']['Code']
            _msg = e.response['Error'].get('Message', '')
            if _code in DENIED_CODES or 'AccessDenied' in _msg:
                results['s3:PutBucketObjectLockConfiguration'] = {'status': 'DENIED', 'detail': _code}
            elif _code in _LOCK_CFG_CODES or 'not supported' in _msg.lower():
                results['s3:PutBucketObjectLockConfiguration'] = {'status': 'ALLOWED', 'detail': f'Can configure (bucket needs lock at creation: {_code})'}
            else:
                results['s3:PutBucketObjectLockConfiguration'] = {'status': 'ERROR', 'detail': _code}
        except Exception as e:
            results['s3:PutBucketObjectLockConfiguration'] = {'status': 'ERROR', 'detail': str(e)[:80]}

        # PutObjectRetention / PutObjectLegalHold: need a temporary probe object.
        # Keys are unique per run (not fixed names) so repeated probe runs on a versioned
        # bucket don't keep stacking new versions on top of one another.
        _WRITE_KEY = f'__policy_write_probe_{uuid.uuid4().hex[:8]}__'
        _write_ok = False
        try:
            s3.put_object(Bucket=bucket_name, Key=_WRITE_KEY, Body=b'x')
            _write_ok = True
        except Exception:
            pass

        if _write_ok:
            # PutObjectRetention: use a past date → returns InvalidArgument if permission ALLOWED
            # (date must be in the future — but the AccessDenied check fires before date validation)
            _RETEN_OK_CODES = {'InvalidArgument', 'InvalidRequest', 'ObjectLockConfigurationNotFoundError',
                               'InvalidObjectState', 'NoSuchObjectLockConfiguration'}
            try:
                _past = datetime.now(timezone.utc) - timedelta(days=1)
                s3.put_object_retention(Bucket=bucket_name, Key=_WRITE_KEY,
                                        Retention={'Mode': 'GOVERNANCE', 'RetainUntilDate': _past})
                results['s3:PutObjectRetention'] = {'status': 'ALLOWED', 'detail': 'Can set retention'}
            except ClientError as e:
                _code = e.response['Error']['Code']
                _msg = e.response['Error'].get('Message', '')
                if _code in DENIED_CODES or 'AccessDenied' in _msg:
                    results['s3:PutObjectRetention'] = {'status': 'DENIED', 'detail': _code}
                elif _code in _RETEN_OK_CODES:
                    results['s3:PutObjectRetention'] = {'status': 'ALLOWED', 'detail': f'Can set retention (bucket not lock-enabled: {_code})'}
                else:
                    results['s3:PutObjectRetention'] = {'status': 'ERROR', 'detail': _code}
            except Exception as e:
                results['s3:PutObjectRetention'] = {'status': 'ERROR', 'detail': str(e)[:80]}

            # PutObjectLegalHold: set to OFF (no-op, safe — OFF on a non-held object does nothing)
            try:
                s3.put_object_legal_hold(Bucket=bucket_name, Key=_WRITE_KEY,
                                         LegalHold={'Status': 'OFF'})
                results['s3:PutObjectLegalHold'] = {'status': 'ALLOWED', 'detail': 'Can set legal hold'}
            except ClientError as e:
                _code = e.response['Error']['Code']
                _msg = e.response['Error'].get('Message', '')
                if _code in DENIED_CODES or 'AccessDenied' in _msg:
                    results['s3:PutObjectLegalHold'] = {'status': 'DENIED', 'detail': _code}
                elif _code in ('InvalidArgument', 'InvalidRequest', 'ObjectLockConfigurationNotFoundError'):
                    results['s3:PutObjectLegalHold'] = {'status': 'ALLOWED', 'detail': f'Can set legal hold (bucket not lock-enabled: {_code})'}
                else:
                    results['s3:PutObjectLegalHold'] = {'status': 'ERROR', 'detail': _code}
            except Exception as e:
                results['s3:PutObjectLegalHold'] = {'status': 'ERROR', 'detail': str(e)[:80]}

            # BypassGovernanceRetention: test only if PutObjectRetention was ALLOWED
            # (requires a live GOVERNANCE-locked object to probe)
            if results.get('s3:PutObjectRetention', {}).get('status') == 'ALLOWED':
                _bypass_key = f'__policy_bypass_probe_{uuid.uuid4().hex[:8]}__'
                _bypass_lock_seconds = 6
                try:
                    s3.put_object(Bucket=bucket_name, Key=_bypass_key, Body=b'x')
                    _future = datetime.now(timezone.utc) + timedelta(seconds=_bypass_lock_seconds)
                    s3.put_object_retention(Bucket=bucket_name, Key=_bypass_key,
                                            Retention={'Mode': 'GOVERNANCE', 'RetainUntilDate': _future})
                    # Now attempt deletion with bypass — this is the actual permission check
                    s3.delete_object(Bucket=bucket_name, Key=_bypass_key,
                                     BypassGovernanceRetention=True)
                    results['s3:BypassGovernanceRetention'] = {'status': 'ALLOWED', 'detail': 'Can bypass GOVERNANCE retention on delete'}
                except ClientError as e:
                    _code = e.response['Error']['Code']
                    _msg = e.response['Error'].get('Message', '')
                    if _code in DENIED_CODES or 'AccessDenied' in _msg:
                        results['s3:BypassGovernanceRetention'] = {'status': 'DENIED', 'detail': _code}
                    else:
                        results['s3:BypassGovernanceRetention'] = {'status': 'ERROR', 'detail': _code}
                    # Bypass was denied (or errored) — the object is still under its short
                    # GOVERNANCE lock. Retry the plain delete with backoff until the lock
                    # naturally expires, instead of giving up silently and leaking a version.
                    _cleanup_probe_object(s3, bucket_name, _bypass_key,
                                          max_attempts=5, wait_seconds=2)
                except Exception as e:
                    results['s3:BypassGovernanceRetention'] = {'status': 'ERROR', 'detail': str(e)[:80]}
                    _cleanup_probe_object(s3, bucket_name, _bypass_key,
                                          max_attempts=5, wait_seconds=2)
            else:
                results['s3:BypassGovernanceRetention'] = {'status': 'SKIPPED', 'detail': 'Skipped — requires PutObjectRetention to be ALLOWED first'}

            _cleanup_probe_object(s3, bucket_name, _WRITE_KEY, max_attempts=3, wait_seconds=2)
        else:
            for _op in ('s3:PutObjectRetention', 's3:PutObjectLegalHold', 's3:BypassGovernanceRetention'):
                results[_op] = {'status': 'SKIPPED', 'detail': 'Skipped — write probe object could not be created'}

        # ── IAM Operations ──────────────────────────────────────────────────────
        _iam_probe_user = f'probe-iam-{uuid.uuid4().hex[:8]}'
        _iam_user_created = False
        _iam_key_id = None
        try:
            from botocore.config import Config as _IamConfig
            iam = boto3.client('iam',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                endpoint_url=endpoint_url or None,
                region_name=region if region not in ('default', '', None) else 'us-east-1',
                config=_IamConfig(connect_timeout=10, read_timeout=30, retries={'max_attempts': 1}),
            )

            # iam:CreateUser
            try:
                iam.create_user(UserName=_iam_probe_user)
                results['iam:CreateUser'] = {'status': 'ALLOWED', 'detail': f'Created probe user {_iam_probe_user}'}
                _iam_user_created = True
            except ClientError as e:
                _code = e.response['Error']['Code']
                _msg  = e.response['Error'].get('Message', '')
                if _code in DENIED_CODES or 'AccessDenied' in _msg:
                    results['iam:CreateUser'] = {'status': 'DENIED', 'detail': _code}
                else:
                    results['iam:CreateUser'] = {'status': 'ERROR', 'detail': _code}

            # iam:CreateAccessKey
            if _iam_user_created:
                try:
                    _kr = iam.create_access_key(UserName=_iam_probe_user)
                    _iam_key_id = _kr['AccessKey']['AccessKeyId']
                    results['iam:CreateAccessKey'] = {'status': 'ALLOWED', 'detail': f'Key {_iam_key_id[:8]}... created'}
                except ClientError as e:
                    _code = e.response['Error']['Code']
                    _msg  = e.response['Error'].get('Message', '')
                    if _code in DENIED_CODES or 'AccessDenied' in _msg:
                        results['iam:CreateAccessKey'] = {'status': 'DENIED', 'detail': _code}
                    else:
                        results['iam:CreateAccessKey'] = {'status': 'ERROR', 'detail': _code}
            else:
                results['iam:CreateAccessKey'] = {'status': 'SKIPPED', 'detail': 'Requires iam:CreateUser'}

            # iam:PutUserPolicy
            if _iam_user_created:
                _inline = json.dumps({"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:ListBucket"],"Resource":[f"arn:aws:s3:::{bucket_name}",f"arn:aws:s3:::{bucket_name}/*"]}]})
                try:
                    iam.put_user_policy(UserName=_iam_probe_user, PolicyName='probe-policy', PolicyDocument=_inline)
                    results['iam:PutUserPolicy'] = {'status': 'ALLOWED', 'detail': 'Inline policy attached'}
                except ClientError as e:
                    _code = e.response['Error']['Code']
                    _msg  = e.response['Error'].get('Message', '')
                    if _code in DENIED_CODES or 'AccessDenied' in _msg:
                        results['iam:PutUserPolicy'] = {'status': 'DENIED', 'detail': _code}
                    else:
                        results['iam:PutUserPolicy'] = {'status': 'ERROR', 'detail': _code}
            else:
                results['iam:PutUserPolicy'] = {'status': 'SKIPPED', 'detail': 'Requires iam:CreateUser'}

        except Exception as e:
            for _op in ('iam:CreateUser', 'iam:CreateAccessKey', 'iam:PutUserPolicy'):
                if _op not in results:
                    results[_op] = {'status': 'ERROR', 'detail': str(e)[:80]}
        finally:
            if _iam_user_created:
                try:
                    if _iam_key_id:
                        iam.delete_access_key(UserName=_iam_probe_user, AccessKeyId=_iam_key_id)
                except Exception: pass
                try:
                    iam.delete_user_policy(UserName=_iam_probe_user, PolicyName='probe-policy')
                except Exception: pass
                try:
                    iam.delete_user(UserName=_iam_probe_user)
                except Exception: pass

        logger.info(f"Policy probe on bucket '{bucket_name}': {json.dumps({k: v['status'] for k, v in results.items()})}")
        return jsonify({'results': results, 'bucket': bucket_name, 'endpoint': endpoint_url or 'AWS S3'})

    except Exception as e:
        logger.error(f"Policy probe error: {e}")
        return jsonify({'error': str(e)}), 500


def _iam_client_from_request(data):
    """Create a boto3 IAM client from request credentials."""
    from botocore.config import Config as _IamCfg
    endpoint_url = data.get('endpoint_url', '').strip()
    access_key   = data.get('access_key', '').strip()
    secret_key   = data.get('secret_key', '').strip()
    region       = data.get('region', 'us-east-1')
    if not (access_key and secret_key):
        raise ValueError('access_key and secret_key are required')
    return boto3.client('iam',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url or None,
        region_name=region if region not in ('default', '', None) else 'us-east-1',
        config=_IamCfg(connect_timeout=10, read_timeout=30, retries={'max_attempts': 1}),
    )


@app.route('/api/iam/create-test-user', methods=['POST'])
def iam_create_test_user():
    """Create an IAM test user with a bucket-scoped inline policy."""
    try:
        data = request.json
        iam    = _iam_client_from_request(data)
        bucket = data.get('bucket_name', '').strip()
        username   = data.get('username', f'test-user-{uuid.uuid4().hex[:8]}').strip()
        permission = data.get('permission', 'read-only')

        PERM_ACTIONS = {
            'read-only':  ['s3:GetObject', 's3:HeadObject', 's3:ListBucket'],
            'write-only': ['s3:PutObject'],
            'read-write': ['s3:GetObject', 's3:HeadObject', 's3:ListBucket', 's3:PutObject', 's3:DeleteObject'],
            'full-s3':    ['s3:*'],
        }
        actions = PERM_ACTIONS.get(permission, PERM_ACTIONS['read-only'])

        iam.create_user(UserName=username)

        key_resp = iam.create_access_key(UserName=username)
        new_key    = key_resp['AccessKey']['AccessKeyId']
        new_secret = key_resp['AccessKey']['SecretAccessKey']

        policy_doc = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": actions,
                "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"]
            }]
        })
        iam.put_user_policy(UserName=username, PolicyName='bucket-access', PolicyDocument=policy_doc)

        logger.info(f"Created IAM test user: {username} ({permission}) on bucket {bucket}")
        return jsonify({
            'status': 'created',
            'username': username,
            'access_key_id': new_key,
            'secret_access_key': new_secret,
            'permission': permission,
            'bucket': bucket,
        })
    except ClientError as e:
        code = e.response['Error']['Code']
        msg  = e.response['Error'].get('Message', str(e))
        return jsonify({'error': f'{code}: {msg}'}), 400
    except Exception as e:
        logger.error(f"IAM create-test-user error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/iam/list-test-users', methods=['POST'])
def iam_list_test_users():
    """List IAM users."""
    try:
        data = request.json
        iam  = _iam_client_from_request(data)
        resp = iam.list_users()
        users = [{'username': u['UserName'], 'created': u['CreateDate'].isoformat(), 'arn': u.get('Arn', '')}
                 for u in resp.get('Users', [])]
        return jsonify({'users': users})
    except ClientError as e:
        code = e.response['Error']['Code']
        msg  = e.response['Error'].get('Message', str(e))
        return jsonify({'error': f'{code}: {msg}'}), 400
    except Exception as e:
        logger.error(f"IAM list-users error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/iam/delete-test-user', methods=['POST'])
def iam_delete_test_user():
    """Delete an IAM user — removes all access keys and inline policies first."""
    try:
        data     = request.json
        iam      = _iam_client_from_request(data)
        username = data.get('username', '').strip()
        if not username:
            return jsonify({'error': 'username is required'}), 400

        for k in iam.list_access_keys(UserName=username).get('AccessKeyMetadata', []):
            iam.delete_access_key(UserName=username, AccessKeyId=k['AccessKeyId'])
        for p in iam.list_user_policies(UserName=username).get('PolicyNames', []):
            iam.delete_user_policy(UserName=username, PolicyName=p)
        iam.delete_user(UserName=username)

        logger.info(f"Deleted IAM test user: {username}")
        return jsonify({'status': 'deleted', 'username': username})
    except ClientError as e:
        code = e.response['Error']['Code']
        msg  = e.response['Error'].get('Message', str(e))
        return jsonify({'error': f'{code}: {msg}'}), 400
    except Exception as e:
        logger.error(f"IAM delete-user error: {e}")
        return jsonify({'error': str(e)}), 500


IMMUTABILITY_TEST_PREFIX = '_immutability-audit-test/'
DELETE_BLOCKED_INDICATORS = ('objectlocked', 'accessdenied', 'locked', 'methodnotallowed', 'invalidrequest', '403')


@app.route('/api/immutability/test-delete', methods=['POST'])
def immutability_test_delete():
    """One audit round: write a small test object, then immediately attempt to delete it.

    Provider-claimed "immutable" buckets have been observed to block deletes on the first
    try but allow them on a later try — this endpoint exists to catch and log that
    inconsistency with a timestamp + the raw error code/message, so it can be exported as
    evidence. Unlike the retention_worm_verify scenario, this does NOT set any retention
    lock itself — it only observes whatever the bucket already enforces.
    """
    try:
        data = request.json
        s3, bucket = _creds_from_request(data)
        if not bucket:
            return jsonify({'error': 'bucket_name is required'}), 400

        key = f"{IMMUTABILITY_TEST_PREFIX}{uuid.uuid4().hex}.txt"
        body = f"immutability audit test - {datetime.now(timezone.utc).isoformat()}".encode()
        round_timestamp = datetime.now(timezone.utc).isoformat()

        put_start = time.time()
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=body)
            put_result = {'status': 'SUCCESS', 'elapsed_ms': round((time.time() - put_start) * 1000, 1), 'error': None}
        except ClientError as e:
            code = e.response['Error']['Code']
            msg = e.response['Error'].get('Message', str(e))
            return jsonify({
                'key': key, 'timestamp': round_timestamp,
                'put': {'status': 'ERROR', 'elapsed_ms': round((time.time() - put_start) * 1000, 1), 'error': f'{code}: {msg}'},
                'delete': {'status': 'SKIPPED', 'elapsed_ms': 0, 'error_code': None, 'error_message': 'Write failed — delete not attempted'},
            })
        except Exception as e:
            return jsonify({
                'key': key, 'timestamp': round_timestamp,
                'put': {'status': 'ERROR', 'elapsed_ms': round((time.time() - put_start) * 1000, 1), 'error': str(e)},
                'delete': {'status': 'SKIPPED', 'elapsed_ms': 0, 'error_code': None, 'error_message': 'Write failed — delete not attempted'},
            })

        delete_start = time.time()
        try:
            s3.delete_object(Bucket=bucket, Key=key)
            delete_result = {'status': 'ALLOWED', 'elapsed_ms': round((time.time() - delete_start) * 1000, 1), 'error_code': None, 'error_message': None}
        except ClientError as e:
            code = e.response['Error']['Code']
            msg = e.response['Error'].get('Message', str(e))
            status = 'BLOCKED' if any(ind in f"{code} {msg}".lower() for ind in DELETE_BLOCKED_INDICATORS) else 'ERROR'
            delete_result = {'status': status, 'elapsed_ms': round((time.time() - delete_start) * 1000, 1), 'error_code': code, 'error_message': msg}
        except Exception as e:
            delete_result = {'status': 'ERROR', 'elapsed_ms': round((time.time() - delete_start) * 1000, 1), 'error_code': None, 'error_message': str(e)}

        return jsonify({'key': key, 'timestamp': round_timestamp, 'put': put_result, 'delete': delete_result})
    except Exception as e:
        logger.error(f"immutability-test-delete error: {e}")
        return jsonify({'error': str(e)}), 500


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


def _format_bytes(bytes_val):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024
    return f"{bytes_val:.2f} TB"


@app.route('/api/bucket-stats', methods=['POST'])
def get_bucket_stats():
    """Get object count + total size for a bucket (paginated — recurses through all folders)."""
    try:
        data = request.json
        bucket_name = data.get('bucket_name', '').strip()
        endpoint_url = data.get('endpoint_url', '').strip()
        access_key = data.get('access_key', '').strip()
        secret_key = data.get('secret_key', '').strip()
        region = data.get('region', 'us-east-1')

        if not all([bucket_name, access_key, secret_key]):
            return jsonify({'error': 'Missing required fields'}), 400

        try:
            s3_client = create_s3_client(endpoint_url, access_key, secret_key, region)
            s3_client.head_bucket(Bucket=bucket_name)
        except Exception as e:
            logger.error(f"Failed to connect to S3 bucket: {e}")
            return jsonify({'error': f'Cannot access bucket: {str(e)}'}), 400

        stats = capture_bucket_stats(s3_client, bucket_name)
        if stats is None:
            return jsonify({'error': 'Error retrieving bucket stats'}), 500

        total_size_bytes = stats['total_size_bytes']
        average_size = stats['average_size_bytes']

        result = {
            'bucket_name': bucket_name,
            'object_count': stats['object_count'],
            'total_size_bytes': total_size_bytes,
            'total_size_mb': round(total_size_bytes / (1024 * 1024), 2),
            'total_size_gb': round(total_size_bytes / (1024 * 1024 * 1024), 2),
            'average_size_mb': round(average_size / (1024 * 1024), 2),
            'formatted_size': _format_bytes(total_size_bytes),
            'formatted_avg_size': _format_bytes(average_size),
        }

        # Versioning check — cheap single call. If enabled/suspended, the "current objects"
        # numbers above can under-report what's actually stored (see capture_bucket_version_stats).
        try:
            vresp = s3_client.get_bucket_versioning(Bucket=bucket_name)
            versioning_status = vresp.get('Status')  # 'Enabled' | 'Suspended' | absent
        except Exception:
            versioning_status = None
        result['versioning_status'] = versioning_status or 'Disabled'

        if versioning_status in ('Enabled', 'Suspended'):
            vstats = capture_bucket_version_stats(s3_client, bucket_name)
            if vstats:
                result['all_versions'] = {
                    'version_count': vstats['version_count'],
                    'delete_marker_count': vstats['delete_marker_count'],
                    'total_size_bytes': vstats['total_size_bytes'],
                    'formatted_size': _format_bytes(vstats['total_size_bytes']),
                }
            else:
                result['all_versions_error'] = 'Could not list object versions (check ListBucketVersions / s3:GetBucketVersioning permission)'

        return jsonify(result)

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
        bucket_name = data.get('bucket_name', '') or ''
        endpoint_url = data.get('endpoint_url')
        access_key = data.get('access_key')
        secret_key = data.get('secret_key')
        region = data.get('region', 'default')
        duration = data.get('duration', 300)  # Duration in seconds, default 5 min
        custom_overrides = data.get('custom', {})

        # Allow "bucket/prefix" shorthand — split into bucket name and key prefix
        key_prefix = data.get('key_prefix', '').strip().strip('/')
        if '/' in bucket_name:
            parts = bucket_name.split('/', 1)
            bucket_name = parts[0].strip()
            if not key_prefix:
                key_prefix = parts[1].strip().strip('/')
        key_prefix = (key_prefix + '/') if key_prefix else ''

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

        test_engine.start(scenario_config, bucket_name, s3_client, custom_overrides, key_prefix)

        logger.info(f"[Session {session_id}] Started test: {scenario_key} on bucket {bucket_name} prefix='{key_prefix}' for {duration} seconds")
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


@app.route('/actions-dictionary')
def actions_dictionary_page():
    """Serve the Policy Probe action dictionary/glossary page."""
    return render_template('actions_dictionary.html')


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
