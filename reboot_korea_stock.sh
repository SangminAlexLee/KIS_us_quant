#!/bin/bash

# Script Name: reboot_korea_stock.sh
# Description: Check if KoreaStockAutoTrade_Alex.py is running; if not, start it.

# Define the script to execute
SCRIPT_NAME="USStockAutoTrade_Alex.py"

# Find the process ID (PID) of the running script
PID=$(ps aux | grep "$SCRIPT_NAME" | grep -v grep | awk '{print $2}')

if [ -n "$PID" ]; then
  echo "Found running process: $SCRIPT_NAME (PID: $PID)"
  echo "Killing process..."
  kill -9 "$PID"
  echo "Process killed."
else
  echo "No running process found for: $SCRIPT_NAME"
  echo "Starting the script as it is not running..."
fi

# Navigate to the script directory (adjust as needed)
SCRIPT_DIR="/home/ec2-user/quant_invest/KIS_us_quant"  # 변경: KoreaStockAutoTrade_Alex.py가 위치한 디렉토리
cd "$SCRIPT_DIR" || exit

# Activate Python virtual environment (if needed)
# source /path/to/venv/bin/activate  # 필요 시 가상환경 활성화

# Start the script in the background
echo "Starting $SCRIPT_NAME in the background..."
nohup python3 "$SCRIPT_NAME" > "${SCRIPT_NAME%.py}.log" 2>&1 &

# Get the new process ID
NEW_PID=$(ps aux | grep "$SCRIPT_NAME" | grep -v grep | awk '{print $2}')
if [ -n "$NEW_PID" ]; then
  echo "Process started successfully: $SCRIPT_NAME (PID: $NEW_PID)"
else
  echo "Failed to start the process."
fi

