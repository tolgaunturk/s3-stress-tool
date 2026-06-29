# Ceph S3 Load Testing Tool

A comprehensive, professional-grade load testing tool for Ceph S3 object storage. Run realistic workloads, monitor real-time metrics, and compare performance across test runs.

## Features

✅ **10 Predefined Test Scenarios**
- Sequential Write/Read — Measure throughput
- Random Write/Read IOPS — Measure operations per second
- Mixed Workloads — Real-world read/write ratios (70/30)
- Large Object Upload — Multipart transfer testing
- Small File Storm — Metadata operation stress
- Endurance Tests — Extended stability testing
- Maximum Throughput — Find cluster limits
- Concurrent Clients — Scalability testing

✅ **Comprehensive Metrics**
- IOPS (operations per second)
- Throughput (MB/s) — Read and write separately
- Latency — Average, P50, P90, P95, P99, Min, Max
- Success Rate & Error Breakdown
- Data Transfer Rate
- Real-time trending with 60-point timeseries

✅ **Web Dashboard**
- Beautiful Bootstrap 5 UI with responsive design
- Live Chart.js graphs for throughput, IOPS, and latency
- Real-time metric cards updating every 1 second
- Detailed metrics table
- Test history with one-click export
- Report comparison tool

✅ **Command-Line Tool**
- Run tests directly from terminal
- Live rich terminal output with colored tables
- Export results to CSV or JSON
- Compare multiple test runs side-by-side
- Automation-friendly JSON output

✅ **Report Management**
- Auto-save reports after each test
- Full timeseries data for analysis
- Export to CSV (spreadsheet) or JSON (API integration)
- Compare metrics between runs with delta percentages
- Test history browser

✅ **Flexible Authentication**
- Custom credentials (Access Key + Secret Key)
- Support for any S3-compatible endpoint
- Regional configuration
- Environment variable overrides

## Quick Start

### Installation

```bash
cd s3-stress-tool

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Web UI (Recommended)

```bash
python app.py
```

Then open your browser to **http://localhost:5000**

1. Enter your Ceph endpoint URL (e.g., `http://ceph.example.com:7480`)
2. Provide access key and secret key
3. Specify a test bucket
4. Select a scenario from the list
5. Click **Start Test** and watch metrics in real-time
6. View history and export results as CSV/JSON

### Command-Line Interface

```bash
# List all scenarios
python cli.py scenarios

# Run a test
python cli.py run \
  --scenario sequential_write \
  --bucket test-bucket \
  --endpoint http://ceph:7480 \
  --access-key YOUR_KEY \
  --secret-key YOUR_SECRET

# List saved reports
python cli.py reports

# Export a report
python cli.py export --run-id sequential_write_20260620_143000 --format csv

# Compare two runs
python cli.py compare sequential_write_20260620_143000 sequential_write_20260620_150000
```

## Test Scenarios

| Scenario | Purpose | Concurrency | Object Size |
|----------|---------|---|---|
| `sequential_write` | Bulk write throughput | 10 | 10 MB |
| `sequential_read` | Bulk read throughput | 10 | 10 MB |
| `random_write_iops` | Write operation performance | 32 | 4 KB |
| `random_read_iops` | Read operation performance | 32 | 4 KB |
| `mixed_7030` | Realistic mixed workload | 20 | 1 MB |
| `large_object` | Multipart upload (100MB objects) | 5 | 100 MB |
| `small_file_storm` | Metadata stress test | 50 | 4 KB |
| `endurance` | Extended stability (5min) | 20 | 1 MB |
| `max_throughput` | Find cluster limits | 50 | 50 MB |
| `concurrent_clients` | Concurrent connection scaling | 100 | 512 KB |

## Configuration

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

**Key settings:**
- `AWS_ENDPOINT_URL` — Your Ceph endpoint
- `AWS_REGION` — Region/zone (usually `default` for Ceph)
- `FLASK_ENV` — Set to `production` for public deployment
- `HOST` — Bind address (0.0.0.0 for network access)
- `PORT` — Web server port (default 5000)
- `REPORTS_DIR` — Where to save test reports
- `LOG_LEVEL` — INFO, DEBUG, WARNING

All settings can be overridden via environment variables:

```bash
export FLASK_ENV=production
export PORT=8000
python app.py
```

## API Endpoints

### Test Control
- `POST /api/test/start` — Start a test
- `POST /api/test/stop` — Stop running test
- `GET /api/test/status` — Get current metrics

### Data
- `GET /api/scenarios` — List all test scenarios
- `GET /api/reports` — List saved reports
- `GET /api/reports/<run_id>` — Get full report
- `GET /api/reports/<run_id>/export?format=csv|json` — Export report
- `GET /api/reports/compare?a=<id>&b=<id>` — Compare runs

## Performance Tuning

**For High-Concurrency Tests:**
```bash
export MAX_CONCURRENT_OPS=256
python app.py
```

**For Long-Running Endurance Tests:**
```bash
export MAX_TIMESERIES_POINTS=10000
python app.py
```

**Systemd Service** (Rocky9):
```ini
[Unit]
Description=Ceph S3 Load Testing Tool
After=network.target

[Service]
Type=simple
User=your-user
WorkingDirectory=/path/to/S3_Stress_Tool
Environment="FLASK_ENV=production"
Environment="HOST=0.0.0.0"
ExecStart=/path/to/venv/bin/gunicorn --bind 0.0.0.0:5000 app:app

[Install]
WantedBy=multi-user.target
```

## Troubleshooting

**"Cannot access bucket" Error**
- Verify endpoint URL format (include protocol and port)
- Check credentials are correct
- Ensure bucket exists and is accessible
- Try connecting with `aws s3 ls` first

**Charts not updating**
- Check browser console (F12) for JavaScript errors
- Clear browser cache
- Try a different browser

**High memory usage with long tests**
- Reduce `MAX_TIMESERIES_POINTS` to limit data retention
- Run reports export and delete old reports periodically

**Permission denied on reports directory**
- Ensure process has write access to `reports/` directory
- Run with proper user permissions

## Deployment on Rocky9 Linux

1. Install Python and dependencies:
```bash
sudo dnf install python3 python3-pip python3-venv
```

2. Clone/transfer project to server

3. Install dependencies:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

4. Configure .env:
```bash
cp .env.example .env
# Edit .env with your Ceph endpoint and credentials
```

5. Create systemd service (see above)

6. Start service:
```bash
sudo systemctl enable ceph-load-test
sudo systemctl start ceph-load-test
sudo systemctl status ceph-load-test
```

7. Access web UI:
```
http://your-rocky-server:5000
```

## License

MIT

## Notes

- Tests are non-destructive by default
- Random data is used to avoid caching effects
- Multipart upload is automatically used for large objects (>5MB)
- Reports are saved as JSON with full timeseries data
- No external services required — all data stored locally
