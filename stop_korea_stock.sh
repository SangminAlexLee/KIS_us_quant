#!/bin/bash

# Script Name: kill_korea_stock.sh
# Description: Kill the running KoreaStockAutoTrade_Alex.py process

# Define the script name
SCRIPT_NAME="KoreaStockAutoTrade_Alex.py"

# Find the process ID (PID) of the running script
PID=$(ps aux | grep "$SCRIPT_NAME" | grep -v grep | awk '{print $2}')

if [ -n "$PID" ]; then
  echo "Found running process: $SCRIPT_NAME (PID: $PID)"
  echo "Killing process..."
  kill -9 "$PID"
  if [ $? -eq 0 ]; then
    echo "Process $SCRIPT_NAME (PID: $PID) has been successfully killed."
  else
    echo "Failed to kill process $SCRIPT_NAME (PID: $PID)."
  fi
else
  echo "No running process found for: $SCRIPT_NAME"
fi

