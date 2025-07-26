#!/bin/bash

# Configuration
AWS_REGION=${AWS_REGION:-us-west-2}
REPOSITORY_NAME=${REPOSITORY_NAME:-sagemaker-nemo-curator}
IMAGE_TAG=${IMAGE_TAG:-latest}
DOCKERFILE=${DOCKERFILE:-Dockerfile}


aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin 763104351884.dkr.ecr.$AWS_REGION.amazonaws.com

# Get AWS Account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
if [ -z "$AWS_ACCOUNT_ID" ]; then
    echo "Error: Unable to get AWS Account ID. Make sure AWS CLI is configured."
    exit 1
fi

ECR_URI=$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$REPOSITORY_NAME

echo "Building Docker image..."
docker build \
  --build-arg CUDA_VER=12.5.1 \
  --build-arg REPO_URL=https://github.com/NVIDIA/NeMo-Curator.git \
  --build-arg CURATOR_COMMIT=main \
  --build-arg IMAGE_LABEL=sagemaker-nemo-curator \
  -t $REPOSITORY_NAME:$IMAGE_TAG \
  -f $DOCKERFILE .

if [ $? -ne 0 ]; then
    echo "Error: Docker build failed"
    exit 1
fi

echo "Creating ECR repository if it doesn't exist..."
aws ecr create-repository \
  --repository-name $REPOSITORY_NAME \
  --region $AWS_REGION \
  --image-scanning-configuration scanOnPush=true || true

echo "Tagging image for ECR..."
docker tag $REPOSITORY_NAME:$IMAGE_TAG $ECR_URI:$IMAGE_TAG

echo "Logging in to ECR..."
aws ecr get-login-password --region $AWS_REGION | \
  docker login --username AWS --password-stdin $ECR_URI

echo "Pushing image to ECR..."
docker push $ECR_URI:$IMAGE_TAG

if [ $? -eq 0 ]; then
    echo "Success! Image pushed to: $ECR_URI:$IMAGE_TAG"
    echo ""
    echo "To use this image in SageMaker:"
    echo "image_uri = '$ECR_URI:$IMAGE_TAG'"
else
    echo "Error: Failed to push image"
    exit 1
fi
