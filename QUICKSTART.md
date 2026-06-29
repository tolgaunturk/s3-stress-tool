# Quick Start — Ceph S3 Load Testing Tool

Get the tool running in 5 minutes!

## Installation

```bash
# Navigate to project directory
cd /Users/unturk/Desktop/S3_Stress_Tool

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Option A: Web Dashboard (Recommended)

### Step 1: Start the Server

```bash
python app.py
```

You should see:
```
Starting Ceph S3 Load Testing Tool on 0.0.0.0:5000
```

### Step 2: Open Dashboard

Go to: **http://localhost:5000**

You should see the colorful Ceph S3 Load Testing dashboard.

### Step 3: Configure Connection

1. **Endpoint URL**: Enter your Ceph endpoint (e.g., `http://ceph.example.com:7480`)
2. **Access Key**: Your Ceph access key
3. **Secret Key**: Your Ceph secret key
4. **Region**: Usually `default` for Ceph
5. **Bucket Name**: The test bucket to use

### Step 4: Select a Scenario

Scroll through the scenario list on the left:
- **Sequential Write** — Bulk upload testing
- **Random Read IOPS** — Random read performance
- **Mixed 70/30** — Realistic workload (recommended for first test)
- **Small File Storm** — Metadata stress test
- And 6 more specialized scenarios...

Click on a scenario to select it.

### Step 5: Start Test

Click the **▶ Start Test** button.

Watch the real-time metrics update:
- **IOPS** — Operations per second
- **Read/Write MB/s** — Throughput
- **Latency** — Average and P99 response times
- **Success Rate** — Percentage of successful operations

The 3 live charts show throughput, IOPS, and latency trends over time.

### Step 6: Monitor & Stop

- Test runs until all operations complete
- Click **⏹ Stop Test** to stop early
- View results in the **Detailed Metrics** table
- Check **Test History** at the bottom to see previous runs

### Step 7: Export Results

Click **CSV** or **JSON** next to any test run to download the data.

## Option B: Command-Line

### List Scenarios

```bash
python cli.py scenarios
```

Shows all 10 test scenarios with details.

### Run a Test

```bash
python cli.py run \
  --scenario mixed_7030 \
  --bucket test-bucket \
  --endpoint http://ceph:7480 \
  --access-key YOUR_KEY \
  --secret-key YOUR_SECRET
```

You'll see a live-updating display:
```
┌─ Ceph S3 Load Test: Mixed 70/30 ────────────────────────────────────┐
│ Status: ● RUNNING    Duration: 00:02:34                             │
├─────────────────────────────────────────────────────────────────────┤
│ IOPS: 523.4   Throughput: 521.3 MB/s   Latency: 12.3ms   Success: 99.8%
│ Total Ops: 1,486    Read IOPS: 366.4    Write IOPS: 157.0           │
│ Data: 1,304.2 MB    P50: 8.5ms  P99: 35.2ms                         │
└─────────────────────────────────────────────────────────────────────┘
```

### View Reports

```bash
python cli.py reports
```

Lists all saved test runs with key metrics.

### Export a Report

```bash
python cli.py export \
  --run-id mixed_7030_20260620_143000 \
  --format csv
```

Creates a CSV file you can open in Excel/Sheets.

### Compare Two Runs

```bash
python cli.py compare \
  mixed_7030_20260620_143000 \
  mixed_7030_20260620_150000
```

Shows side-by-side comparison with delta percentages.

## Common Scenarios to Try

### 1. First Test (Start Here)
```bash
python cli.py run \
  --scenario mixed_7030 \
  --bucket test \
  --endpoint http://ceph:7480 \
  --access-key KEY \
  --secret-key SECRET
```
Realistic 70% read / 30% write workload — good baseline.

### 2. Throughput Testing
```bash
python cli.py run --scenario max_throughput ...
```
Aggressive 50 concurrent clients uploading 50MB objects.

### 3. IOPS Stress
```bash
python cli.py run --scenario random_write_iops ...
```
32 clients hammering small 4KB writes.

### 4. Extended Stability
```bash
python cli.py run --scenario endurance ...
```
5-minute test with balanced read/write/list/delete operations.

### 5. Concurrent Scalability
```bash
python cli.py run --scenario concurrent_clients ...
```
100 concurrent connections — test connection handling limits.

## Viewing Results

### In Web Dashboard
1. Results update in real-time during test
2. **Test History** shows all past runs
3. Click **CSV** to download timeseries data
4. Click **JSON** to download full report

### In Terminal
1. Final summary prints automatically
2. `python cli.py reports` to list all
3. `python cli.py export --run-id X --format csv` to download

## Environment Configuration

Create `.env` file for custom settings:

```bash
cp .env.example .env
```

Edit to configure:
- Default endpoint URL
- Default credentials
- Server host/port
- Report directory location
- Logging level

## Troubleshooting

### Connection Refused
- Check endpoint URL is correct (http://, not https://)
- Verify port (usually 7480 for Ceph)
- Test with: `curl -I http://your-endpoint:7480`

### Authentication Failed
- Verify access key and secret key
- Test with: `aws s3 ls --endpoint-url http://... --access-key KEY --secret-key SECRET`

### Bucket Not Found
- Ensure bucket exists
- Verify bucket name spelling
- Test with: `aws s3 ls s3://bucket-name --endpoint-url http://...`

### No Charts Appearing
- Open browser Developer Tools (F12)
- Check Console tab for errors
- Try refreshing the page
- Check that JavaScript is enabled

### Out of Memory
- Reduce test concurrency in scenario selection
- Run shorter tests
- Set `MAX_TIMESERIES_POINTS=1000` in .env

## Next Steps

- Read [README.md](README.md) for full documentation
- Check [scenarios.py](scenarios.py) for all available tests
- Create custom `.env` file for your Ceph cluster
- Deploy to Rocky9 Linux server (see README for systemd setup)

**Happy testing! 🚀**
