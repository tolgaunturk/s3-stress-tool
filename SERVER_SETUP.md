# Server Setup Guide - Ubuntu 22.04

This guide will help you set up the S3 Stress Tool on your Ubuntu 22.04 server for internet access.

## Prerequisites

- Ubuntu 22.04 server in the US (or anywhere with public IP)
- SSH access to the server
- Project files uploaded to `/home/ubuntu/S3_Stress_Tool` (or your preferred path)

## Step 1: SSH into Your Server

```bash
ssh ubuntu@YOUR_SERVER_IP
```

Replace `YOUR_SERVER_IP` with your actual server IP address.

## Step 2: Install Python and Dependencies

```bash
# Update system
sudo apt update
sudo apt upgrade -y

# Install Python and pip
sudo apt install -y python3 python3-pip python3-venv

# Install git (optional but useful)
sudo apt install -y git
```

## Step 3: Setup Virtual Environment

```bash
# Navigate to project directory
cd /home/ubuntu/S3_Stress_Tool

# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

## Step 4: Configure Flask to Listen on All Interfaces

Edit `app.py` and change the last line from:
```python
app.run(debug=True, port=5000, host='127.0.0.1')
```

To:
```python
app.run(debug=False, port=5000, host='0.0.0.0')
```

This allows the app to accept connections from any IP address.

## Step 5: Create a Systemd Service (Auto-Start on Reboot)

Create a new systemd service file:

```bash
sudo nano /etc/systemd/system/s3-stress-tool.service
```

Paste the following content:

```ini
[Unit]
Description=S3 Stress Testing Tool
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/S3_Stress_Tool
Environment="PATH=/home/ubuntu/S3_Stress_Tool/venv/bin"
ExecStart=/home/ubuntu/S3_Stress_Tool/venv/bin/python app.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

**Note:** Adjust paths if your project is in a different location.

Save and exit (Ctrl+X, then Y, then Enter).

## Step 6: Enable and Start the Service

```bash
# Reload systemd daemon
sudo systemctl daemon-reload

# Enable service to start on boot
sudo systemctl enable s3-stress-tool

# Start the service now
sudo systemctl start s3-stress-tool

# Check status
sudo systemctl status s3-stress-tool
```

## Step 7: Check if Service is Running

```bash
# View real-time logs
sudo journalctl -u s3-stress-tool -f

# Or check status
sudo systemctl status s3-stress-tool
```

You should see output like:
```
* Running on http://0.0.0.0:5000
```

## Step 8: Access from Your Local Machine

Open your browser and go to:

```
http://YOUR_SERVER_IP:5000
```

Replace `YOUR_SERVER_IP` with your actual server IP address.

**Example:** `http://203.0.113.45:5000`

## Firewall Configuration

Make sure port 5000 is open on your server. Check your cloud provider's security group/firewall settings.

### For UFW (Ubuntu Firewall):

```bash
# Allow port 5000
sudo ufw allow 5000/tcp

# Check rules
sudo ufw status
```

## Common Tasks

### View Logs
```bash
sudo journalctl -u s3-stress-tool -f
```

### Stop the Service
```bash
sudo systemctl stop s3-stress-tool
```

### Restart the Service
```bash
sudo systemctl restart s3-stress-tool
```

### Remove Auto-Start
```bash
sudo systemctl disable s3-stress-tool
```

### View Last 100 Log Lines
```bash
sudo journalctl -u s3-stress-tool -n 100
```

## Troubleshooting

### "Connection refused" from local machine
- Check if service is running: `sudo systemctl status s3-stress-tool`
- Check firewall: `sudo ufw status`
- Make sure port 5000 is allowed in cloud provider's security group

### Service not starting
```bash
# Check logs for errors
sudo journalctl -u s3-stress-tool -n 50

# Try running manually to see error
cd /home/ubuntu/S3_Stress_Tool
source venv/bin/activate
python app.py
```

### High memory usage
- Reduce test concurrency
- Restart service periodically: `sudo systemctl restart s3-stress-tool`

### Permission denied
```bash
# Make sure ubuntu user owns the directory
sudo chown -R ubuntu:ubuntu /home/ubuntu/S3_Stress_Tool
```

## Find Your Server IP

If you don't know your server's IP:

```bash
# From the server itself
hostname -I

# Or check with
ip addr show
```

## Next Steps

1. Access the tool: `http://YOUR_SERVER_IP:5000`
2. Enter your S3 endpoint, credentials, and bucket name
3. Select a scenario and start testing
4. Results are displayed in real-time

## Stopping the Tool

If you want to stop the service:

```bash
sudo systemctl stop s3-stress-tool
```

It will auto-restart if you reboot the server (unless you disable it first).

## Using a Custom Domain

If you have a domain, you can point it to your server IP, then access via:
```
http://your-domain.com:5000
```

Or setup nginx as a reverse proxy (see production setup guide).
