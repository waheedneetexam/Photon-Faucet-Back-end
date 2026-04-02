#!/bin/bash

if [ -z "$1" ]; then
  echo "❌ Error: No commit message provided."
  echo "Usage: ./gpush \"Your commit message\""
  exit 1
fi

MESSAGE="$1"

echo "--- 🚀 Starting Git Automation ---"

git add .
git commit -m "$MESSAGE"

if [ $? -eq 0 ]; then
  git push
  echo "--- ✅ Successfully pushed to remote ---"
else
  echo "--- ⚠️ Nothing to commit or git error occurred ---"
fi
