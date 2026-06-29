# S3 Stress Testing Tool - New Features

## 📋 Test Scenarios Explanation Page

A dedicated page that explains all test scenarios in detail.

### Access
- **URL**: `http://YOUR_SERVER_IP:5000/scenarios`
- **From Dashboard**: Click "📋 View Test Scenarios" button in header

### What You'll Find
- **Category Overview**: Visual grid showing all 6 test categories
  - Throughput - Large sequential operations
  - IOPS - Small random operations  
  - Mixed - Realistic workloads
  - Metadata - Concurrent operations
  - Endurance - Extended tests
  - Concurrency - Connection scaling

- **Detailed Scenario Cards**: For each of 10 scenarios:
  - Full description and purpose
  - Concurrent operations count
  - Total operations to be performed
  - Object size used
  - Operation mix visualization (% of each operation type)
  - Duration limits (if applicable)
  - When to use this scenario

- **Quick Navigation**: Table of contents to jump to each scenario

---

## 📊 Report Download Feature

Download test results in multiple formats for analysis and sharing.

### How to Use

**After a test completes:**
1. Green "📊 Test Complete - Download Report" section appears
2. Choose download format:
   - **📥 Download CSV** - Great for spreadsheets and data analysis in Excel
   - **📥 Download JSON** - Great for API integration and programmatic analysis

**Or from Test History:**
1. Scroll to "Test History" table at bottom
2. Each past test shows:
   - Run ID
   - Scenario name
   - IOPS achieved
   - Throughput (MB/s)
   - Success rate
   - Download buttons (CSV & JSON)

### Report Contents

**CSV Format:**
- Timeseries data with timestamps
- IOPS, throughput, latency at each measurement interval
- Success/failure counts
- Easy to import into Excel or Google Sheets

**JSON Format:**
- Complete test metadata
- Full timeseries data
- Operation mix details
- Configuration used
- Ideal for programmatic analysis and integration

### Example Use Cases

1. **Performance Tracking** - Download results and track performance over time
2. **Compliance Reporting** - Save test results for audit trails
3. **Data Analysis** - Import CSV into Excel for charts and analysis
4. **CI/CD Integration** - Parse JSON reports in automated pipelines
5. **Comparison** - Download multiple tests and compare results

---

## 🎯 API Endpoints (New)

### Get Detailed Scenarios Info
```bash
GET /api/scenarios/detailed
```
Returns detailed information about all scenarios with explanations.

### Download Report
```bash
GET /api/reports/{run_id}/export?format=csv
GET /api/reports/{run_id}/export?format=json
```
Download a specific test report in the chosen format.

---

## 🖥️ Dashboard Updates

### Header Enhancement
- **"📋 View Test Scenarios"** - Link to scenarios explanation page
- **"📊 View Reports"** - Button to jump to test history (shows after test completes)

### Results Section
- **Current Test Report Card** - Appears after test completion
  - Green notification box with download buttons
  - Quick CSV/JSON download options
  - Tips on which format to use

### History Table Improvement
- **Better styled download buttons**
  - CSV button (blue) - for spreadsheet analysis
  - JSON button (green) - for API integration
  - Each button clearly labeled with icon
  - Easy to click and download

---

## 📈 Workflow Example

1. **Plan Your Test**
   - Click "📋 View Test Scenarios" 
   - Read explanations to pick the right scenario
   - Return to main dashboard

2. **Run Test**
   - Enter your S3 credentials
   - Select scenario
   - Click "▶ Start Test"
   - Watch metrics update in real-time

3. **Get Results**
   - Test completes automatically
   - Green report card appears
   - Download CSV or JSON with one click
   - Results also stored in Test History

4. **Analyze & Share**
   - **CSV** → Open in Excel → Create charts
   - **JSON** → Parse in scripts → Integrate with monitoring
   - Keep test history for comparison

---

## 📝 Scenario Descriptions

### Throughput Scenarios
- **Sequential Write** (10 concurrent, 10MB files)
  - Measures maximum write speed
  - Tests network and disk throughput
  
- **Sequential Read** (10 concurrent, 10MB files)
  - Measures maximum read speed
  - Tests read-heavy workloads

- **Large Object Upload** (5 concurrent, 100MB files)
  - Tests multipart upload handling
  - Large file transfer stability

- **Maximum Throughput** (50 concurrent, 50MB files)
  - Aggressive load to find cluster limits
  - Stress testing the cluster

### IOPS Scenarios
- **Random Write IOPS** (32 concurrent, 4KB objects)
  - Measures write operations per second
  - Small random writes = metadata stress

- **Random Read IOPS** (32 concurrent, 4KB objects)
  - Measures read operations per second
  - Tests random access performance

### Mixed Workload
- **Mixed 70/30 Read/Write** (20 concurrent, 1MB objects)
  - Realistic application workload
  - 70% reads, 30% writes
  - Best for typical use case testing

### Metadata & Stress
- **Small File Storm** (50 concurrent, 4KB objects)
  - High concurrency with small files
  - Tests metadata operation limits
  - Mix of create, read, delete operations

### Endurance & Scalability
- **Endurance Test** (20 concurrent, runs 5+ minutes)
  - Extended stability testing
  - Detects memory leaks or performance degradation
  - Realistic mixed workload over time

- **Concurrent Clients** (100 concurrent connections)
  - Tests connection handling limits
  - Scalability under high concurrency
  - Mixed small/medium operations

---

## 🔍 Key Benefits

✅ **Know What Each Test Does** - Read detailed explanations before running
✅ **Export Results** - Download in CSV or JSON for analysis
✅ **Track Performance** - Keep history of all test runs
✅ **Share Reports** - Easy format for sharing with team
✅ **Integrate with Tools** - JSON format for API integration
✅ **Visual Analysis** - CSV format for charts in Excel/Sheets

---

## 💾 Accessing Old Reports

All test reports are automatically saved. Access them anytime:

1. Scroll to "Test History" section
2. Find the test you want
3. Click CSV or JSON to download
4. Results are timestamped with run ID

No limit on how many reports you can keep!

---

Questions? Check `/scenarios` page for detailed scenario information.
