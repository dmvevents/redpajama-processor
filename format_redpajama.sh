#!/bin/bash

# Set the base directory
BASE_DIR="/data/rpv2-processed"
# Create a destination directory for all files
DEST_DIR="/data/rpv2-processed-combined"

# Create the destination directory if it doesn't exist
mkdir -p "$DEST_DIR"

# Loop through all subdirectories
for dir in "$BASE_DIR"/*/; do
    if [ -d "$dir" ]; then
        # Get the folder name (number)
        folder_num=$(basename "$dir")

        # Move and rename each file, adding the folder number as prefix
        for file in "$dir"*.jsonl; do
            if [ -f "$file" ]; then
                filename=$(basename "$file")
                mv "$file" "$DEST_DIR/${folder_num}_${filename}"
            fi
        done
    fi
done

