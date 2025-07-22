#!/bin/bash
# fixed_smart_resume.sh

set -euo pipefail

S3_BUCKET="redpajama-v2-bucket"
S3_PREFIX="redpajama-v2/en"
BASE_URL="https://data.together.xyz/redpajama-data-v2/v1.0.0"
WORK_DIR="/tmp/redpajama_transfer"
MAX_PARALLEL=30  # Reduced to avoid too many conflicts

# Create unique temp directories
mkdir -p "$WORK_DIR"/{temp,logs,progress}

# Check what's already in S3
echo "Checking existing uploads..."
aws s3 ls s3://${S3_BUCKET}/${S3_PREFIX}/documents/ --recursive | awk '{print $4}' | sed 's/.*documents\///' | sed 's/\.json\.gz$//' > "$WORK_DIR/already_uploaded.txt"

echo "Found $(wc -l < $WORK_DIR/already_uploaded.txt) files already uploaded"

# Process function with unique temp files
process_file_smart() {
    local line=$1
    local snapshot=$2
    local process_id=$$
    local timestamp=$(date +%s%N)
    
    # Create unique temp file names
    local unique_id="${process_id}_${timestamp}"
    local doc_temp="$WORK_DIR/temp/doc_${unique_id}.json.gz"
    local signal_temp="$WORK_DIR/temp/signal_${unique_id}.json.gz"
    
    # Skip if already in S3
    if grep -Fxq "$line" "$WORK_DIR/already_uploaded.txt"; then
        return 0
    fi
    
    # Double-check if file exists in S3
    if aws s3api head-object --bucket "$S3_BUCKET" --key "${S3_PREFIX}/documents/${line}.json.gz" &>/dev/null; then
        echo "$line" >> "$WORK_DIR/already_uploaded.txt"
        return 0
    fi
    
    # Process document
    local doc_url="${BASE_URL}/documents/${line}.json.gz"
    local s3_doc_path="s3://${S3_BUCKET}/${S3_PREFIX}/documents/${line}.json.gz"
    
    if wget -q -T 30 "$doc_url" -O "$doc_temp" 2>/dev/null; then
        if aws s3 cp "$doc_temp" "$s3_doc_path" --quiet 2>/dev/null; then
            rm -f "$doc_temp"
            
            # Also process quality signals
            local signal_url="${BASE_URL}/quality_signals/${line}.signals.json.gz"
            local s3_signal_path="s3://${S3_BUCKET}/${S3_PREFIX}/quality_signals/${line}.signals.json.gz"
            
            if wget -q -T 30 "$signal_url" -O "$signal_temp" 2>/dev/null; then
                aws s3 cp "$signal_temp" "$s3_signal_path" --quiet 2>/dev/null
                rm -f "$signal_temp"
            fi
            
            echo "✓ Processed: $line"
            return 0
        else
            rm -f "$doc_temp"
        fi
    fi
    
    # Cleanup on failure
    rm -f "$doc_temp" "$signal_temp"
    echo "✗ Failed: $line"
    return 1
}

export -f process_file_smart
export S3_BUCKET S3_PREFIX BASE_URL WORK_DIR

# Main execution
main() {
    echo "Starting fixed smart resume transfer..."
    
    # Get the listings file
    listings_file="$WORK_DIR/listings/en-2023-06-head_middle.txt"
    if [ ! -f "$listings_file" ]; then
        mkdir -p "$WORK_DIR/listings"
        wget -q "${BASE_URL}/listings/en-2023-06-head_middle.txt" -O "$listings_file"
    fi
    
    echo "Processing remaining files with $MAX_PARALLEL workers..."
    
    # Install parallel if not available
    if ! command -v parallel &> /dev/null; then
        echo "Installing GNU parallel..."
        sudo apt-get update && sudo apt-get install -y parallel
    fi
    
    cat "$listings_file" | parallel -j $MAX_PARALLEL process_file_smart {} "2023-06"
    
    # Cleanup temp directory
    rm -rf "$WORK_DIR/temp"
    
    echo "Transfer complete!"
}

main "$@"
