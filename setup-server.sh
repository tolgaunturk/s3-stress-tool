#!/bin/bash

# S3 Stress Tool - Automated Ubuntu Server Setup
# Usage: bash setup-server.sh

set -e

echo "================================"
echo "S3 Stress Tool - Server Setup"
echo "================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if running as root
if [[ $EUID -ne 0 ]]; then
   echo -e "${RED}This script must be run with sudo${NC}"
   exit 1
fi

echo -e "${YELLOW}Step 1: Updating system...${NC}"
apt update
apt upgrade -y

echo -e "${YELLOW}Step 2: Installing Python and dependencies...${NC}"
apt install -y python3 python3-pip python3-venv curl wget

echo -e "${YELLOW}Step 3: Setting up project directory...${NC}"
PROJECT_DIR="/home/ubuntu/S3_Stress_Tool"

if [ ! -d "$PROJECT_DIR" ]; then
    echo -e "${RED}Error: Project not found at $PROJECT_DIR${NC}"
    echo "Please upload the project files first"
    exit 1
fi

echo -e "${YELLOW}Step 4: Creating Python virtual environment...${NC}"
cd "$PROJECT_DIR"
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo -e "${YELLOW}Step 5: Updating Flask configuration...${NC}"
# Update app.py to listen on 0.0.0.0
sed -i "s/host='127.0.0.1'/host='0.0.0.0'/g" "$PROJECT_DIR/app.py"
sed -i "s/debug=True/debug=False/g" "$PROJECT_DIR/app.py"

echo -e "${YELLOW}Step 6: Creating systemd service...${NC}"
cat > /etc/systemd/system/s3-stress-tool.service << EOF
[Unit]
Description=S3 Stress Testing Tool
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=$PROJECT_DIR
Environment="PATH=$PROJECT_DIR/venv/bin"
ExecStart=$PROJECT_DIR/venv/bin/python app.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

echo -e "${YELLOW}Step 7: Enabling and starting service...${NC}"
systemctl daemon-reload
systemctl enable s3-stress-tool
systemctl start s3-stress-tool

echo -e "${YELLOW}Step 8: Setting up firewall...${NC}"
# Enable UFW and allow port 5000
ufw --force enable
ufw allow 22/tcp
ufw allow 5000/tcp

echo ""
echo -e "${GREEN}================================${NC}"
echo -e "${GREEN}✓ Setup Complete!${NC}"
echo -e "${GREEN}================================${NC}"
echo ""

echo "Service Status:"
systemctl status s3-stress-tool --no-pager

echo ""
echo "Next Steps:"
echo "1. Get your server IP:"
echo "   hostname -I"
echo ""
echo "2. Access the tool from your browser:"
echo "   http://YOUR_SERVER_IP:5000"
echo ""
echo "3. View logs:"
echo "   journalctl -u s3-stress-tool -f"
echo ""
echo "4. Service management:"
echo "   sudo systemctl stop s3-stress-tool"
echo "   sudo systemctl restart s3-stress-tool"
echo "   sudo systemctl status s3-stress-tool"
echo ""

# Get IP address
echo -e "${YELLOW}Your server IP addresses:${NC}"
hostname -I

echo ""
echo -e "${GREEN}Happy testing! 🚀${NC}"
