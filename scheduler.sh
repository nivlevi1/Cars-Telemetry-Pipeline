#!/bin/bash

# List of scripts in order (adjust paths if needed)
scripts=(
    "Car_Models.py"
    "Cars_Colors.py"
    "Cars.py"
    "Data_Generator.py"
    "Data_Enrichment.py"
    "Alerting_Detection.py"
    "Alerting_Counter.py"
)

# Run each script in the background
for script in "${scripts[@]}"; do
    echo "Starting $script..."
    python3 "/app/$script" &
    sleep 30  # Small delay between script starts
done

echo "All scripts are running. Press Ctrl+C to terminate."

# Keep the script alive (so the container doesn’t exit)
wait
