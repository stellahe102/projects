#!/bin/bash
PROJECT_DIR="/home/ubuntu/capstone_dbt_project/capstone"
LOG_DIR="/home/ubuntu/capstone_dbt_project/capstone/logs"
LOG_FILE="${LOG_DIR}/dbt_schedule_$(date +\%Y-\%m-\%d_\%H-\%M-\%S).log"

# Create the log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# switch to dbt project directory
cd "$PROJECT_DIR"

# Redirect standard output and standard error to the log file
/home/ubuntu/.local/bin/dbt build >> "$LOG_FILE" 2>&1

RC=$?

# add an exit code
if [ $RC = 0 ]; then
    echo "dbt job is done sucessfully."
    exit 0  # Indicates an success
else
    echo "error happened to dbt job. check log file for details"
    exit 1  # Indicates error
fi