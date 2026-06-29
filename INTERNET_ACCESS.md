# Internet Access Setup - Quick Reference

## TL;DR - Fast Setup

### On Your Ubuntu Server:

```bash
# 1. SSH into server
ssh ubuntu@YOUR_SERVER_IP

# 2. Navigate to project
cd /home/ubuntu/S3_Stress_Tool

# 3. Make setup script executable
chmod +x setup-server.sh

# 4. Run automated setup (requires sudo)
sudo bash setup-server.sh

# Done! Service is running
```

### From Your Local Machine:

Open browser: `http://YOUR_SERVER_IP:5000`

---

## Manual Setup (If You Prefer)

### Step 1: Prepare Server
```bash
ssh ubuntu@YOUR_SERVER_IP
sudo apt update && sudo apt install -y python3 python3-pip python3-venv
cd /home/ubuntu/S3_Stress_Tool
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Step 2: Configure Flask
Edit `app.py`, find the last line:
```python
# Change FROM:
app.run(debug=True, port=5000, host='127.0.0.1')

# TO:
app.run(debug=False, port=5000, host='0.0.0.0')
```

### Step 3: Run in Background
```bash
cd /home/ubuntu/S3_Stress_Tool
nohup python app.py > stress_tool.log 2>&1 &

# Check it's running
tail -f stress_tool.log
```

### Step 4: Open Firewall
```bash
sudo ufw allow 5000/tcp
```

### Step 5: Access
From local machine: `http://YOUR_SERVER_IP:5000`

---

## Finding Your Server IP

```bash
# From server terminal:
hostname -I

# Example output:
# 203.0.113.45 10.0.0.5
# Use the first IP (public IP)
```

---

## Using the Tool

1. **Endpoint URL**: Enter your S3 endpoint
   - Example: `http://s3.example.com:7480`
   - Or AWS: `https://s3.amazonaws.com`

2. **Credentials**: Enter access key and secret key for the S3 account you want to test

3. **Bucket**: Specify the bucket name to test

4. **Scenario**: Choose Light, Medium, or Heavy load

5. **Start Test**: Click "Start Test" and watch real-time metrics

---

## Managing the Service

### Automated Setup (Systemd)

```bash
# Check status
sudo systemctl status s3-stress-tool

# View live logs
sudo journalctl -u s3-stress-tool -f

# Stop service
sudo systemctl stop s3-stress-tool

# Restart service
sudo systemctl restart s3-stress-tool

# View last 50 log lines
sudo journalctl -u s3-stress-tool -n 50
```

### Manual Background (Nohup)

```bash
# Start in background
cd /home/ubuntu/S3_Stress_Tool
nohup python app.py > stress_tool.log 2>&1 &

# View logs
tail -f stress_tool.log

# Kill process
pkill -f "python app.py"
```

---

## Testing Multiple S3 Accounts

You can test different S3 accounts by changing credentials in the web interface:

1. Enter **Account A** credentials → Run test
2. Change to **Account B** credentials → Run test
3. Change to **Account C** credentials → Run test

Each test's results will be displayed in real-time.

---

## Troubleshooting

### Can't Connect from Local Machine
```bash
# 1. Check if service is running
sudo systemctl status s3-stress-tool

# 2. Check firewall
sudo ufw status

# 3. Check if port is listening
sudo netstat -tlnp | grep 5000

# Expected output should show Python listening on 0.0.0.0:5000
```

### Service Won't Start
```bash
# Check logs for errors
sudo journalctl -u s3-stress-tool -n 100

# Try running manually
cd /home/ubuntu/S3_Stress_Tool
source venv/bin/activate
python app.py
```

### "Address already in use" Error
```bash
# Kill process using port 5000
sudo lsof -i :5000
sudo kill -9 <PID>

# Restart service
sudo systemctl restart s3-stress-tool
```

### Module Import Error
```bash
# Reinstall dependencies
cd /home/ubuntu/S3_Stress_Tool
source venv/bin/activate
pip install --force-reinstall -r requirements.txt
```

---

## Security Notes

⚠️ **Important for Production:**

- The tool listens on all interfaces (0.0.0.0)
- Anyone with the IP can access it
- Consider:
  - Firewall rules to restrict access to trusted IPs only
  - Using a reverse proxy (nginx) with authentication
  - Running on a private network only
  - Using HTTPS with a self-signed certificate

For now, it's fine for testing in a private/trusted network.

---

## Accessing From Different Locations

Once running, you can access from:

- **Same Network**: `http://192.168.1.x:5000` (internal IP)
- **Internet**: `http://PUBLICIP:5000` (from anywhere)
- **With Domain**: `http://yourdomain.com:5000` (if domain points to server)

---

## Keep Service Running After Logout

If you used **Systemd setup**:
- ✅ Service auto-starts on reboot
- ✅ Service keeps running even if you SSH logout
- ✅ Automatically restarts if it crashes

If you used **Nohup setup**:
- Service runs in background
- Use `disown` or `nohup` to keep it running after logout
- Not auto-restarted if it crashes

**Recommendation: Use Systemd setup** (automated script does this)

---

## Questions?

Check `SERVER_SETUP.md` for detailed instructions.
