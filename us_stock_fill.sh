#!/bin/bash

# Define the script to execute
SCRIPT_NAME="us_stock_fill.py"

# Navigate to the script directory (adjust as needed)
SCRIPT_DIR="/home/ec2-user/quant_invest/KIS_us_quant"  
cd "$SCRIPT_DIR" || exit

LOG_FILE="${SCRIPT_NAME%}_$(date '+%Y-%m-%d_%H-%M-%S').log"

# Start the script in the background
echo "Starting $SCRIPT_NAME ..."
nohup python3 -u "$SCRIPT_NAME" > "$LOG_FILE" &