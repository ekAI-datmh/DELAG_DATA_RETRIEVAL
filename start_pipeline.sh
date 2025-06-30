#!/bin/bash

# Tay Nguyen Pipeline Background Starter
# This script starts the pipeline in the background and saves all output to a log file

echo "🚀 Starting Tay Nguyen Pipeline in Background..."

# Create timestamp for log file
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="pipeline_output_${TIMESTAMP}.log"

echo "📝 Output will be saved to: $LOG_FILE"
echo "📍 Working directory: $(pwd)"

# Start pipeline in background with output redirection
nohup python3 run_tay_nguyen_pipeline.py > "$LOG_FILE" 2>&1 &

# Get the process ID
PID=$!

echo "🔄 Pipeline started with PID: $PID"
echo "📊 To monitor progress: tail -f $LOG_FILE"
echo "🛑 To stop pipeline: kill $PID"
echo ""
echo "Pipeline is now running in the background. You can safely close this terminal."
echo "Check the log file for progress updates."

# Save PID to file for easy reference
echo $PID > pipeline.pid
echo "💾 PID saved to pipeline.pid" 