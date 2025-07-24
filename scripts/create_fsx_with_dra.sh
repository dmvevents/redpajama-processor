#!/bin/bash
# create_fsx_with_dra.sh

# Load configuration
source fsx-config.env

echo "=== Creating FSx for Lustre with Data Repository Association ==="
echo "VPC: $VPC_ID"
echo "Subnet: $SUBNET_ID"
echo "Security Group: $FSX_SECURITY_GROUP_ID"
echo "S3 Bucket: $S3_BUCKET"
echo "S3 Prefix: $S3_PREFIX"

# Calculate storage size based on your data
# Check current S3 size first
S3_SIZE_BYTES=$(aws s3 ls s3://${S3_BUCKET}/${S3_PREFIX}/ --recursive --summarize | grep "Total Size" | awk '{print $3}')
S3_SIZE_TB=$((S3_SIZE_BYTES / 1024 / 1024 / 1024 / 1024))

echo "Current S3 data size: ~${S3_SIZE_TB} TB"

# Recommend FSx size (minimum 1200 GB, but should be larger than your data)
if [ $S3_SIZE_TB -lt 1 ]; then
    FSX_SIZE=1200
elif [ $S3_SIZE_TB -lt 10 ]; then
    FSX_SIZE=$((S3_SIZE_TB * 1200 + 1200))  # Add buffer
else
    FSX_SIZE=$((S3_SIZE_TB * 1200))
fi

echo "Recommended FSx size: ${FSX_SIZE} GB"

# Create FSx filesystem
echo "Creating FSx for Lustre filesystem..."

FSX_CREATE_OUTPUT=$(aws fsx create-file-system \
    --file-system-type LUSTRE \
    --storage-capacity $FSX_SIZE \
    --subnet-ids $SUBNET_ID \
    --security-group-ids $FSX_SECURITY_GROUP_ID \
    --lustre-configuration '{
        "DeploymentType": "PERSISTENT_2",
        "PerUnitStorageThroughput": 250,
        "DataRepositoryAssociations": [
            {
                "FileSystemPath": "/redpajama",
                "DataRepositoryPath": "s3://'$S3_BUCKET'/'$S3_PREFIX'",
                "ImportedFileChunkSize": 1024,
                "AutoImportPolicy": {
                    "Events": ["NEW", "CHANGED", "DELETED"]
                },
                "AutoExportPolicy": {
                    "Events": ["NEW", "CHANGED", "DELETED"]
                }
            }
        ]
    }' \
    --tags Key=Name,Value=redpajama-fsx-lustre Key=Project,Value=redpajama-processing)

FSX_ID=$(echo $FSX_CREATE_OUTPUT | jq -r '.FileSystem.FileSystemId')
echo "FSx filesystem creation initiated: $FSX_ID"

# Add to config
echo "export FSX_ID=\"$FSX_ID\"" >> fsx-config.env

echo "Waiting for filesystem to become available (this may take 10-15 minutes)..."
aws fsx wait file-system-available --file-system-ids $FSX_ID

# Get DNS name
FSX_DNS=$(aws fsx describe-file-systems --file-system-ids $FSX_ID \
    --query 'FileSystems[0].DNSName' --output text)

echo "export FSX_DNS=\"$FSX_DNS\"" >> fsx-config.env

echo "=== FSx Filesystem Ready! ==="
echo "Filesystem ID: $FSX_ID"
echo "DNS Name: $FSX_DNS"
echo "Mount command: mount -t lustre $FSX_DNS@tcp:/fsx /mnt/fsx"
echo "Configuration saved to fsx-config.env"
