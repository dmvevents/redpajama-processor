#!/bin/bash
# redpajama_transfer.sh

set -euo pipefail

# Configuration
S3_BUCKET="redpajama-v2-bucket"
S3_PREFIX="redpajama-v2/en"
BASE_URL="https://data.together.xyz/redpajama-data-v2/v1.0.0"
WORK_DIR="/tmp/redpajama_transfer"
MAX_WORKERS=32

# Create working directories
mkdir -p "$WORK_DIR"/{logs,listings,documents,quality_signals,minhash,duplicates}
LOG_FILE="$WORK_DIR/logs/transfer_$(date +%Y%m%d_%H%M%S).log"

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

# Download English data for a specific snapshot
download_snapshot() {
    local CC_SNAPSHOT=$1
    local LANG="en"
    local PARTITION="head_middle"
    
    log "Processing snapshot: $CC_SNAPSHOT"
    
    # Get listings file
    local listings_tag="${LANG}-${CC_SNAPSHOT}-${PARTITION}"
    local listings_file="$WORK_DIR/listings/${listings_tag}.txt"
    
    log "Downloading listings file for $CC_SNAPSHOT..."
    wget -q "${BASE_URL}/listings/${listings_tag}.txt" -O "$listings_file"
    
    if [ ! -f "$listings_file" ]; then
        log "Failed to download listings for $CC_SNAPSHOT"
        return 1
    fi
    
    # Process documents
    log "Processing documents for $CC_SNAPSHOT..."
    while IFS= read -r line; do
        local url="${BASE_URL}/documents/${line}.json.gz"
        local dest="$WORK_DIR/documents/${line}.json.gz"
        local s3_path="s3://${S3_BUCKET}/${S3_PREFIX}/documents/${line}.json.gz"
        
        mkdir -p "$(dirname "$dest")"
        wget -q "$url" -O "$dest"
        
        if [ -f "$dest" ]; then
            aws s3 cp "$dest" "$s3_path"
            rm "$dest"
        else
            log "Failed to download: $url"
        fi
    done < "$listings_file"
    
    # Process quality signals
    log "Processing quality signals for $CC_SNAPSHOT..."
    while IFS= read -r line; do
        local url="${BASE_URL}/quality_signals/${line}.signals.json.gz"
        local dest="$WORK_DIR/quality_signals/${line}.signals.json.gz"
        local s3_path="s3://${S3_BUCKET}/${S3_PREFIX}/quality_signals/${line}.signals.json.gz"
        
        mkdir -p "$(dirname "$dest")"
        wget -q "$url" -O "$dest"
        
        if [ -f "$dest" ]; then
            aws s3 cp "$dest" "$s3_path"
            rm "$dest"
        else
            log "Failed to download: $url"
        fi
    done < "$listings_file"
}

# Main execution
main() {
    log "Starting RedPajama V2 English dataset transfer"
    
    # Process recent snapshots first
    SNAPSHOTS=(
        "2023-06"
        "2022-49"
    )
    
    for snapshot in "${SNAPSHOTS[@]}"; do
        download_snapshot "$snapshot"
    done
    
    log "Transfer complete!"
}

# Execute main function
main "$@"
