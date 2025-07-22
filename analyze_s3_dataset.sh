#!/bin/bash
# analyze_s3_dataset.sh

S3_BUCKET="redpajama-v2-bucket"
S3_PREFIX="redpajama-v2/en"

echo "=== RedPajama V2 Dataset Analysis ==="

# Check total size and file count
echo "1. OVERALL DATASET SIZE:"
aws s3 ls s3://${S3_BUCKET}/${S3_PREFIX}/ --recursive --human-readable --summarize

echo -e "\n2. SNAPSHOTS AVAILABLE:"
aws s3 ls s3://${S3_BUCKET}/${S3_PREFIX}/documents/ | grep PRE

echo -e "\n3. DETAILED BREAKDOWN BY SNAPSHOT:"
for snapshot in $(aws s3 ls s3://${S3_BUCKET}/${S3_PREFIX}/documents/ | grep PRE | awk '{print $2}' | sed 's/\///'); do
    echo "Snapshot: $snapshot"
    file_count=$(aws s3 ls s3://${S3_BUCKET}/${S3_PREFIX}/documents/$snapshot/ --recursive | wc -l)
    size=$(aws s3 ls s3://${S3_BUCKET}/${S3_PREFIX}/documents/$snapshot/ --recursive --summarize | grep "Total Size" | awk '{print $3}')
    echo "  Files: $file_count"
    echo "  Size: $(numfmt --to=iec-i --suffix=B $size)"
    echo "  Sample files:"
    aws s3 ls s3://${S3_BUCKET}/${S3_PREFIX}/documents/$snapshot/ --recursive | head -3 | awk '{print "    " $4}'
    echo
done

echo "4. QUALITY SIGNALS (if available):"
aws s3 ls s3://${S3_BUCKET}/${S3_PREFIX}/quality_signals/ --recursive --summarize 2>/dev/null || echo "No quality signals found"

echo -e "\n5. DUPLICATES AND MINHASH (if available):"
aws s3 ls s3://${S3_BUCKET}/${S3_PREFIX}/duplicates/ --recursive --summarize 2>/dev/null || echo "No duplicates data found"
aws s3 ls s3://${S3_BUCKET}/${S3_PREFIX}/minhash/ --recursive --summarize 2>/dev/null || echo "No minhash data found"

# Save analysis
aws s3 ls s3://${S3_BUCKET}/${S3_PREFIX}/ --recursive > s3_file_listing.txt
echo "Complete file listing saved to: s3_file_listing.txt"
