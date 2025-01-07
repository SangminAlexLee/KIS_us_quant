#!/bin/bash

# Script Name: git_sync.sh
# Description: Automates pull, add, commit (with current time), and push operations.

# Pull latest changes from the remote repository
echo "Pulling latest changes from the remote repository..."
git pull origin main
if [ $? -ne 0 ]; then
  echo "Error: git pull failed."
  exit 1
fi

# Add all changes to the staging area
echo "Adding all changes..."
git add .
if [ $? -ne 0 ]; then
  echo "Error: git add failed."
  exit 1
fi

# Commit changes with the current timestamp as the message
CURRENT_TIME=$(date +"%Y-%m-%d %H:%M:%S")
echo "Committing changes with timestamp: $CURRENT_TIME"
git commit -m "Update: $CURRENT_TIME"
if [ $? -ne 0 ]; then
  echo "No changes to commit."
else
  # Push changes to the remote repository
  echo "Pushing changes to the remote repository..."
  git push origin main
  if [ $? -ne 0 ]; then
    echo "Error: git push failed."
    exit 1
  fi
fi

echo "All operations completed successfully."

