#!/bin/bash
# get_aws_config_for_fsx.sh

echo "=== Getting AWS Configuration for FSx Setup ==="

# Get current region and account
REGION=$(aws configure get region)
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

echo "Region: $REGION"
echo "Account: $ACCOUNT_ID"

# Get VPC info
DEFAULT_VPC=$(aws ec2 describe-vpcs --filters "Name=is-default,Values=true" --query 'Vpcs[0].VpcId' --output text)
echo "Default VPC: $DEFAULT_VPC"

# Get subnets
echo -e "\nAvailable Subnets:"
aws ec2 describe-subnets --filters "Name=vpc-id,Values=$DEFAULT_VPC" \
    --query 'Subnets[*].[SubnetId,AvailabilityZone,CidrBlock,MapPublicIpOnLaunch]' \
    --output table

# Get private subnets (recommended for FSx)
PRIVATE_SUBNETS=($(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$DEFAULT_VPC" "Name=map-public-ip-on-launch,Values=false" \
    --query 'Subnets[*].SubnetId' --output text))

if [ ${#PRIVATE_SUBNETS[@]} -eq 0 ]; then
    echo "No private subnets found, using public subnets"
    SUBNETS=($(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$DEFAULT_VPC" \
        --query 'Subnets[*].SubnetId' --output text))
else
    SUBNETS=("${PRIVATE_SUBNETS[@]}")
fi

echo -e "\nRecommended subnet for FSx: ${SUBNETS[0]}"

# Create or get security group for FSx
SG_NAME="redpajama-fsx-sg"
FSX_SG_ID=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=$SG_NAME" "Name=vpc-id,Values=$DEFAULT_VPC" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)

if [ "$FSX_SG_ID" = "null" ] || [ -z "$FSX_SG_ID" ]; then
    echo "Creating security group for FSx..."
    FSX_SG_ID=$(aws ec2 create-security-group \
        --group-name $SG_NAME \
        --description "Security group for RedPajama FSx Lustre" \
        --vpc-id $DEFAULT_VPC \
        --query 'GroupId' --output text)
    
    # Add Lustre rules
    aws ec2 authorize-security-group-ingress --group-id $FSX_SG_ID --protocol tcp --port 988 --source-group $FSX_SG_ID
    aws ec2 authorize-security-group-ingress --group-id $FSX_SG_ID --protocol tcp --port 1021-1023 --source-group $FSX_SG_ID
    echo "Created security group: $FSX_SG_ID"
else
    echo "Using existing security group: $FSX_SG_ID"
fi

# Save configuration
cat > fsx-config.env << EOF
export REGION="$REGION"
export ACCOUNT_ID="$ACCOUNT_ID"
export VPC_ID="$DEFAULT_VPC"
export SUBNET_ID="${SUBNETS[0]}"
export FSX_SECURITY_GROUP_ID="$FSX_SG_ID"
export S3_BUCKET="redpajama-v2-bucket"
export S3_PREFIX="redpajama-v2/en"
EOF

echo -e "\n=== Configuration saved to fsx-config.env ==="
cat fsx-config.env
echo -e "\nRun: source fsx-config.env"
