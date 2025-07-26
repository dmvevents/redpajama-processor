# RedPajama V2 Processor

A comprehensive toolkit for downloading, fixing, analyzing, and processing RedPajama V2 dataset with multi-core CPU and GPU acceleration.

## Features

- **Data Download & Recovery**: Download RedPajama V2 data with retry logic and corruption fixing
- **Data Analysis**: Memory-efficient analysis of JSONL datasets with statistical reporting
- **Full Processing Pipeline**: Complete NeMo Curator-based processing pipeline including:
  - Data resharding for balanced partitions
  - Document ID assignment
  - Language identification and separation
  - Text cleaning and Unicode normalization
  - Exact and fuzzy deduplication
  - Quality filtering with configurable heuristics
- **Multi-core & GPU Processing**: Efficient processing using Dask distributed and RAPIDS
- **Robust Error Handling**: Comprehensive logging and recovery mechanisms
- **Resumable Pipeline**: Start from any step or skip completed steps

## Requirements

### Core Dependencies
- Python 3.8+
- Dask distributed
- requests
- tqdm
- numpy
- pandas

### Optional Dependencies
- **GPU Acceleration**: RAPIDS ecosystem (cuDF, cuPy)
- **NeMo Curator**: For advanced text processing pipeline
- **FastText**: For language identification

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/redpajama-processor.git
cd redpajama-processor

# Install core dependencies
pip install -r requirements.txt

# Optional: Install GPU support
pip install cudf-cu11 cupy-cuda11x

# Optional: Install NeMo Curator
pip install nemo-curator

# Optional: Install FastText
pip install fasttext
```

## Quick Start

### 1. Download and Fix Data

```bash
# Download damaged files based on processing logs
python fix_damaged_files.py

# The script will:
# - Parse your processing logs for corrupted files
# - Download fresh copies with rate limiting
# - Verify file integrity
# - Retry failed downloads with exponential backoff
```

### 2. Analyze Your Dataset

```bash
# Analyze converted JSONL files
python analyze_redpajama.py --input-dir /path/to/jsonl/files --output-dir ./analysis

# Generates:
# - dataset_analysis_report.json: Comprehensive statistics
# - sample_documents.json: Sample documents for inspection
```

### 3. Run Processing Pipeline

```bash
# Run complete pipeline
python run_full_pipeline.py --input-dir /path/to/jsonl/files --base-dir /processing

# Or run individual steps
python step1_load_and_reshard.py --input-dir /path/to/jsonl/files --base-dir /processing
python step2_add_ids.py --base-dir /processing
python step3_language_separation.py --base-dir /processing
python step4_text_cleaning.py --base-dir /processing
python step5_exact_deduplication.py --base-dir /processing
python step6_fuzzy_deduplication.py --base-dir /processing
python step7_quality_filtering.py --base-dir /processing
```

## Scripts Overview

### Data Download & Recovery

| Script | Purpose | Usage |
|--------|---------|-------|
| `fix_damaged_files.py` | Download and fix corrupted files | `python fix_damaged_files.py` |

**Features:**
- Parses processing logs to identify corrupted files
- Downloads with rate limiting to avoid 429 errors
- Exponential backoff retry mechanism
- File integrity verification
- Comprehensive logging

### Data Analysis

| Script | Purpose | Usage |
|--------|---------|-------|
| `analyze_redpajama.py` | Analyze JSONL datasets | `python analyze_redpajama.py --input-dir /path/to/data` |

**Features:**
- Memory-efficient processing of large datasets
- Statistical analysis (document counts, text lengths, field analysis)
- Sample document extraction
- Error detection and reporting
- Parallel processing with configurable workers

### Processing Pipeline

| Step | Script | Purpose |
|------|--------|---------|
| 1 | `step1_load_and_reshard.py` | Load and reshard data into balanced partitions |
| 2 | `step2_add_ids.py` | Add unique IDs to documents |
| 3 | `step3_language_separation.py` | Identify and separate by language |
| 4 | `step4_text_cleaning.py` | Clean and normalize Unicode text |
| 5 | `step5_exact_deduplication.py` | Remove exact duplicates |
| 6 | `step6_fuzzy_deduplication.py` | Remove near-duplicates using MinHash |
| 7 | `step7_quality_filtering.py` | Apply quality filters |

| Utility | Script | Purpose |
|---------|--------|---------|
| Runner | `run_full_pipeline.py` | Execute complete pipeline |

## Detailed Usage

### Data Download & Recovery

If you encounter corrupted files during processing:

```bash
# Basic usage - will prompt for log file
python fix_damaged_files.py

# The script will:
# 1. Parse your processing logs
# 2. Identify corrupted files
# 3. Download fresh copies with proper rate limiting
# 4. Verify file integrity
# 5. Retry failed downloads with exponential backoff
```

### Data Analysis

```bash
# Basic analysis
python analyze_redpajama.py --input-dir /data/redpajama/jsonl

# Custom settings
python analyze_redpajama.py \
    --input-dir /data/redpajama/jsonl \
    --output-dir ./analysis \
    --workers 8 \
    --sample-size 2000

# Output files:
# - dataset_analysis_report.json: Complete statistics
# - sample_documents.json: Sample documents
```

### Processing Pipeline

#### Run Complete Pipeline

```bash
# Full pipeline with default settings
python run_full_pipeline.py --input-dir /data/jsonl --base-dir /processing

# Skip certain steps
python run_full_pipeline.py \
    --input-dir /data/jsonl \
    --base-dir /processing \
    --skip-steps 5,6

# Start from specific step
python run_full_pipeline.py \
    --input-dir /data/jsonl \
    --base-dir /processing \
    --start-from 4
```

#### Run Individual Steps

```bash
# Step 1: Reshard data
python step1_load_and_reshard.py \
    --input-dir /data/jsonl \
    --base-dir /data/processing \
    --file-size 100M

# Step 2: Add IDs
python step2_add_ids.py \
    --base-dir /data/processing \
    --id-prefix rpv2-2023-06

# Step 3: Language separation
python step3_language_separation.py \
    --base-dir /processing \
    --text-field raw_content

# Step 4: Text cleaning
python step4_text_cleaning.py \
    --base-dir /processing \
    --language EN

# Step 5: Exact deduplication
python step5_exact_deduplication.py \
    --base-dir /processing \
    --hash-method md5

# Step 6: Fuzzy deduplication
python step6_fuzzy_deduplication.py \
    --base-dir /processing

# Step 7: Quality filtering
python step7_quality_filtering.py \
    --base-dir /processing \
    --config-file ./config/filters.yaml
```

## Configuration

### Quality Filtering

Create a custom filter configuration:

```yaml
# config/heuristic_filter_en.yaml
filters:
  - type: WordCountFilter
    config:
      min_words: 10
      max_words: 500000
  - type: CharacterCountFilter
    config:
      min_characters: 50
      max_characters: 2000000
  - type: AlphaRatioFilter
    config:
      min_alpha_ratio: 0.6
  - type: SymbolToWordFilter
    config:
      max_symbol_word_ratio: 0.1
```

### GPU Configuration

```bash
# Enable GPU processing (default)
python step5_exact_deduplication.py --base-dir /processing

# Disable GPU processing
python step5_exact_deduplication.py --base-dir /processing --no-gpu
```

## Output Structure

```
/processing/
├── rpv2-2023-06-resharded/          # Step 1: Resharded data
├── rpv2-2023-06-id/                 # Step 2: Data with IDs
├── rpv2-2023-06-language/           # Step 3: Language-separated data
│   └── data/
│       ├── EN/                      # English documents
│       ├── ES/                      # Spanish documents
│       └── ...
├── rpv2-2023-06-en-cleaned/         # Step 4: Cleaned English data
├── rpv2-2023-06-exact-dedup/        # Step 5: Exact dedup results
├── rpv2-2023-06-exact-dup-removed/  # Step 5: After exact dedup
├── rpv2-2023-06-minhash/            # Step 6: MinHash signatures
├── fuzzy-dedup-output-2023-06/      # Step 6: Fuzzy dedup results
├── rpv2-2023-06-deduped/            # Step 6: After fuzzy dedup
├── rpv2-2023-06-quality-filtered/   # Step 7: Final filtered data
├── logs/                            # Processing logs
└── config/                          # Configuration files
```

## Performance Tips

### Memory Management
- Use appropriate `--workers` based on available RAM
- For large datasets, use smaller batch sizes
- Monitor memory usage during fuzzy deduplication

### GPU Usage
- Ensure sufficient GPU memory for deduplication steps
- Use GPU for steps 5 and 6 (deduplication)
- CPU is used for quality filtering (step 7)

### Disk Space
- Plan for 3-5x original dataset size during processing
- Remove intermediate files after each step if space is limited
- Use fast storage for better I/O performance

## Troubleshooting

### Common Issues

**Rate Limiting (429 errors)**
```bash
# The fix_damaged_files.py script handles this automatically
# with exponential backoff and jitter
```

**Memory Issues**
```bash
# Reduce workers and batch size
python analyze_redpajama.py --workers 2
python run_full_pipeline.py --base-dir /processing
```

**GPU Out of Memory**
```bash
# Fall back to CPU processing
python step5_exact_deduplication.py --base-dir /processing --no-gpu
```

**Corrupted Files**
```bash
# Use the recovery script
python fix_damaged_files.py
```

## Logging

All scripts generate comprehensive logs:
- Individual step logs: `stepX_*.log`
- Pipeline log: `full_pipeline.log`
- Analysis log: `redpajama_analysis.log`
- Recovery log: `download_recovery.log`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request



# Complete NeMo Curator Setup Guide for Amazon EKS

This comprehensive guide walks you through setting up NeMo Curator on Amazon EKS from cluster creation to running data processing workloads, based on real-world implementation experience.

## Prerequisites

- AWS CLI configured with appropriate permissions  
- kubectl installed
- Docker installed (for custom image building)
- Helm 3.x installed
- An NGC API key from [ngc.nvidia.com](https://ngc.nvidia.com)

## 1. Environment Setup

First, create and configure your environment variables:

```bash
#!/bin/bash
# setup-environment.sh

echo "=== NeMo Curator Environment Setup ==="

# Get AWS account and region
export AWS_REGION=$(aws configure get region || echo "us-east-2")
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Cluster configuration
read -p "Enter your EKS cluster name (default: nemo-curator-cluster): " cluster_input
export CLUSTER_NAME=${cluster_input:-nemo-curator-cluster}

# Storage configuration  
read -p "Enter your S3 bucket name (default: nemo-curator-data-${AWS_ACCOUNT_ID}): " bucket_input
export S3_BUCKET_NAME=${bucket_input:-nemo-curator-data-${AWS_ACCOUNT_ID}}

# NGC API Key
read -p "Enter your NGC API key: " ngc_key
export NGC_API_KEY=${ngc_key}

# ECR configuration
export ECR_REPOSITORY="nemo-curator"
export ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# Storage sizing
export FSX_STORAGE_SIZE="2400"  # GB
export PVC_STORAGE_SIZE="2400Gi"

# Save to file for later use
cat > nemo-env.sh << EOF
#!/bin/bash
# Generated environment file
export AWS_REGION="${AWS_REGION}"
export AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID}"
export CLUSTER_NAME="${CLUSTER_NAME}"
export S3_BUCKET_NAME="${S3_BUCKET_NAME}"
export NGC_API_KEY="${NGC_API_KEY}"
export ECR_REPOSITORY="${ECR_REPOSITORY}"
export ECR_URI="${ECR_URI}"
export FSX_STORAGE_SIZE="${FSX_STORAGE_SIZE}"
export PVC_STORAGE_SIZE="${PVC_STORAGE_SIZE}"
EOF

echo "Environment configured! Run 'source nemo-env.sh' to load variables."
echo "AWS Region: ${AWS_REGION}"
echo "Cluster Name: ${CLUSTER_NAME}"
echo "S3 Bucket: ${S3_BUCKET_NAME}"
```

Run the setup:
```bash
chmod +x setup-environment.sh
./setup-environment.sh
source nemo-env.sh
```

## 2. EKS Cluster Setup

### Option A: Using eksctl (Recommended for new clusters)

```bash
#!/bin/bash
# create-eks-cluster.sh

source nemo-env.sh

echo "Creating EKS cluster: ${CLUSTER_NAME}"

# Create cluster with GPU nodes
eksctl create cluster \
  --name ${CLUSTER_NAME} \
  --region ${AWS_REGION} \
  --nodegroup-name gpu-nodes \
  --node-type p3.8xlarge \
  --nodes 2 \
  --nodes-min 1 \
  --nodes-max 4 \
  --managed \
  --enable-ssm

# Update kubeconfig
aws eks update-kubeconfig --region ${AWS_REGION} --name ${CLUSTER_NAME}

echo "EKS cluster ${CLUSTER_NAME} created successfully"
```

### Option B: Using existing HyperPod EKS cluster

If you already have a HyperPod EKS cluster:

```bash
# Update kubeconfig for existing cluster
aws eks update-kubeconfig --region ${AWS_REGION} --name ${CLUSTER_NAME}

# Verify connection
kubectl get nodes
```

## 3. Install Required Components

```bash
#!/bin/bash
# install-components.sh

source nemo-env.sh

echo "Installing required Kubernetes components..."

# 1. Install NVIDIA GPU Operator
helm repo add nvidia https://helm.ngc.nvidia.com/nvidia
helm repo update

kubectl create namespace gpu-operator

helm install --wait gpu-operator \
    -n gpu-operator \
    --create-namespace \
    nvidia/gpu-operator \
    --set driver.enabled=false

# 2. Install FSx CSI Driver
helm repo add aws-fsx-csi-driver https://kubernetes-sigs.github.io/aws-fsx-csi-driver
helm repo update

helm upgrade --install aws-fsx-csi-driver \
    aws-fsx-csi-driver/aws-fsx-csi-driver \
    --namespace kube-system \
    --set controller.serviceAccount.create=false

# 3. Install Dask Operator
helm repo add dask https://helm.dask.org
helm repo update

helm install --create-namespace \
    -n dask-operator \
    --generate-name \
    dask/dask-kubernetes-operator

echo "Components installed successfully"
```

## 4. S3 and FSx Storage Setup

```bash
#!/bin/bash
# setup-storage.sh

source nemo-env.sh

echo "Setting up S3 bucket and FSx filesystem..."

# Create S3 bucket
echo "Creating S3 bucket: ${S3_BUCKET_NAME}"
aws s3 mb s3://${S3_BUCKET_NAME} --region ${AWS_REGION} || echo "Bucket may already exist"

# Get subnet and security group info
export PRIVATE_SUBNET_ID=$(aws ec2 describe-subnets \
    --filters "Name=tag:Name,Values=*private*" \
    --query 'Subnets[0].SubnetId' --output text)

export SECURITY_GROUP_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=*node*" \
    --query 'SecurityGroups[0].GroupId' --output text)

echo "Private Subnet: ${PRIVATE_SUBNET_ID}"
echo "Security Group: ${SECURITY_GROUP_ID}"

# Create FSx Lustre filesystem
echo "Creating FSx Lustre filesystem..."
FSX_RESPONSE=$(aws fsx create-file-system \
    --file-system-type LUSTRE \
    --lustre-configuration "AutoImportPolicy=NEW_CHANGED,DataRepositoryConfiguration={ImportPath=s3://${S3_BUCKET_NAME},ExportPath=s3://${S3_BUCKET_NAME}/processed}" \
    --storage-capacity ${FSX_STORAGE_SIZE} \
    --subnet-ids ${PRIVATE_SUBNET_ID} \
    --tags Key=Name,Value=${CLUSTER_NAME}-fsx \
    --query 'FileSystem.FileSystemId' --output text)

export FSX_ID=${FSX_RESPONSE}
echo "FSx ID: ${FSX_ID}"

# Wait for FSx to be available
echo "Waiting for FSx filesystem to be available..."
aws fsx wait file-system-available --file-system-ids ${FSX_ID} --region ${AWS_REGION}

# Get FSx details
export FSX_DNS_NAME=$(aws fsx describe-file-systems \
    --file-system-id ${FSX_ID} \
    --region ${AWS_REGION} \
    --query 'FileSystems[0].DNSName' --output text)

export FSX_MOUNT_NAME=$(aws fsx describe-file-systems \
    --file-system-id ${FSX_ID} \
    --region ${AWS_REGION} \
    --query 'FileSystems[0].LustreConfiguration.MountName' --output text)

# Save FSx info to env file
cat >> nemo-env.sh << EOF
export FSX_ID="${FSX_ID}"
export FSX_DNS_NAME="${FSX_DNS_NAME}"
export FSX_MOUNT_NAME="${FSX_MOUNT_NAME}"
export PRIVATE_SUBNET_ID="${PRIVATE_SUBNET_ID}"
export SECURITY_GROUP_ID="${SECURITY_GROUP_ID}"
EOF

echo "FSx filesystem created successfully!"
echo "FSx ID: ${FSX_ID}"
echo "DNS Name: ${FSX_DNS_NAME}"
```

## 5. Configure Data Repository Association (DRA)

```bash
#!/bin/bash
# setup-dra.sh

source nemo-env.sh

echo "Setting up Data Repository Association..."

# Create DRA for automatic S3/FSx sync
aws fsx create-data-repository-association \
    --file-system-id ${FSX_ID} \
    --file-system-path /data \
    --data-repository-path s3://${S3_BUCKET_NAME}/ \
    --s3 "AutoImportPolicy={Events=[NEW,CHANGED,DELETED]},AutoExportPolicy={Events=[NEW,CHANGED,DELETED]}" \
    --region ${AWS_REGION}

echo "DRA created - S3 and FSx will sync automatically"

# Trigger initial import
aws fsx create-data-repository-task \
    --file-system-id ${FSX_ID} \
    --type IMPORT_METADATA_FROM_REPOSITORY \
    --paths "/data/" \
    --region ${AWS_REGION} \
    --report Enabled=false

echo "Initial data import triggered"
```

## 6. Setup Kubernetes Storage

```bash
#!/bin/bash
# setup-k8s-storage.sh

source nemo-env.sh

echo "Setting up Kubernetes storage resources..."

# Create storage class
cat <<EOF | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: fsx-lustre-sc
provisioner: fsx.csi.aws.com
parameters:
  fileSystemId: ${FSX_ID}
  subnetId: ${PRIVATE_SUBNET_ID}
  securityGroupIds: ${SECURITY_GROUP_ID}
mountOptions:
  - flock
EOF

# Create PV
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolume
metadata:
  name: fsx-lustre-pv
spec:
  capacity:
    storage: ${PVC_STORAGE_SIZE}
  volumeMode: Filesystem
  accessModes:
    - ReadWriteMany
  persistentVolumeReclaimPolicy: Retain
  storageClassName: fsx-lustre-sc
  csi:
    driver: fsx.csi.aws.com
    volumeHandle: ${FSX_ID}
    volumeAttributes:
      dnsname: ${FSX_DNS_NAME}
      mountname: ${FSX_MOUNT_NAME}
EOF

# Create PVC
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: nemo-workspace
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: fsx-lustre-sc
  resources:
    requests:
      storage: ${PVC_STORAGE_SIZE}
EOF

# Wait for PVC to be bound
echo "Waiting for PVC to be bound..."
kubectl wait --for=condition=Bound pvc/nemo-workspace --timeout=300s

echo "Kubernetes storage configured successfully"
```

## 7. Container Registry and Secrets Setup

```bash
#!/bin/bash
# setup-registry.sh

source nemo-env.sh

echo "Setting up container registry and secrets..."

# Create ECR repository
aws ecr create-repository \
    --repository-name ${ECR_REPOSITORY} \
    --region ${AWS_REGION} || echo "Repository may already exist"

# ECR login
aws ecr get-login-password --region ${AWS_REGION} | \
    docker login --username AWS --password-stdin ${ECR_URI}

# Create NGC registry secret
kubectl delete secret nvcrimagepullsecret --ignore-not-found

kubectl create secret docker-registry nvcrimagepullsecret \
    --docker-server=nvcr.io \
    --docker-username='$oauthtoken' \
    --docker-password="${NGC_API_KEY}"

echo "Registry and secrets configured successfully"
```

## 8. Create Helper Pods

```bash
#!/bin/bash
# create-helper-pods.sh

source nemo-env.sh

echo "Creating helper pods..."

# BusyBox for file management
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: nemo-workspace-busybox
spec:
  containers:
  - name: busybox
    image: busybox
    command: ["sleep", "infinity"]
    volumeMounts:
    - name: workspace
      mountPath: /nemo-workspace
  volumes:
  - name: workspace
    persistentVolumeClaim:
      claimName: nemo-workspace
EOF

# Wait for busybox to be ready
kubectl wait --for=condition=Ready pod/nemo-workspace-busybox --timeout=300s

echo "Helper pods created successfully"
```

## 9. Setup Dask Cluster Manager

Create the Dask cluster creation script:

```python
# create_dask_cluster.py
import argparse
import logging
from dask_kubernetes.operator import KubeCluster
from dask_kubernetes.operator.kubecluster import make_pod_spec

def create_cluster(name, n_workers, n_gpus_per_worker, image, image_pull_secret, pvcs):
    """Create a Dask cluster with GPU support"""
    
    # Parse PVC info
    pvc_name, mount_path = pvcs.split(':')
    
    # Resource specifications
    worker_resources = {
        "limits": {
            "nvidia.com/gpu": str(n_gpus_per_worker),
            "memory": "128Gi",
            "cpu": "16"
        },
        "requests": {
            "nvidia.com/gpu": str(n_gpus_per_worker), 
            "memory": "96Gi",
            "cpu": "12"
        }
    }
    
    scheduler_resources = {
        "limits": {
            "memory": "8Gi",
            "cpu": "2"
        },
        "requests": {
            "memory": "4Gi",
            "cpu": "1"
        }
    }
    
    # Create cluster
    cluster = KubeCluster(
        name=name,
        image=image,
        n_workers=n_workers,
        resources=worker_resources,
        scheduler_resources=scheduler_resources,
        image_pull_secrets=[image_pull_secret],
        env={"CUDA_VISIBLE_DEVICES": "0,1,2,3,4,5,6,7"},
        extra_pod_config={
            "spec": {
                "volumes": [
                    {
                        "name": "workspace",
                        "persistentVolumeClaim": {"claimName": pvc_name}
                    }
                ],
                "containers": [
                    {
                        "name": "dask-worker",
                        "volumeMounts": [
                            {
                                "name": "workspace", 
                                "mountPath": mount_path
                            }
                        ]
                    }
                ]
            }
        }
    )
    
    return cluster

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create Dask cluster for NeMo Curator')
    parser.add_argument("--name", required=True, help="Cluster name")
    parser.add_argument("--n_workers", type=int, default=2, help="Number of workers")
    parser.add_argument("--n_gpus_per_worker", type=int, default=8, help="GPUs per worker")
    parser.add_argument("--image", required=True, help="Container image")
    parser.add_argument("--image_pull_secret", required=True, help="Image pull secret name")
    parser.add_argument("--pvcs", required=True, help="PVC mapping (name:path)")
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    
    try:
        cluster = create_cluster(
            args.name,
            args.n_workers, 
            args.n_gpus_per_worker,
            args.image,
            args.image_pull_secret,
            args.pvcs
        )
        
        print(f"✅ Cluster '{args.name}' created successfully!")
        print(f"📊 Scheduler address: {cluster.scheduler_address}")
        print(f"👥 Workers: {args.n_workers}")
        print(f"🎮 GPUs per worker: {args.n_gpus_per_worker}")
        
    except Exception as e:
        print(f"❌ Error creating cluster: {e}")
        raise
```

Install dependencies and download the script:

```bash
# Install Python dependencies
python3 -m venv nemo-curator-env
source nemo-curator-env/bin/activate
pip install 'dask_kubernetes>=2024.4.1'

# Save the script above or download from source
```

## 10. Launch NeMo Curator Processing

```bash
#!/bin/bash
# launch-nemo-curator.sh

source nemo-env.sh
source nemo-curator-env/bin/activate

echo "Launching NeMo Curator processing..."

# Create Dask cluster
python create_dask_cluster.py \
    --name nemo-curator-cluster \
    --n_workers 2 \
    --n_gpus_per_worker 8 \
    --image nvcr.io/nvidia/nemo:24.03.framework \
    --image_pull_secret nvcrimagepullsecret \
    --pvcs nemo-workspace:/data

# Wait for cluster to be ready
echo "Waiting for Dask cluster to be ready..."
sleep 30

# Get scheduler pod
DASK_CLUSTER_NAME="nemo-curator-cluster"
SCHEDULER_POD=$(kubectl get pods -l "dask.org/cluster-name=$DASK_CLUSTER_NAME,dask.org/component=scheduler" -o name)

echo "Scheduler pod: ${SCHEDULER_POD}"

# Check cluster status
kubectl get pods -l "dask.org/cluster-name=$DASK_CLUSTER_NAME"

echo "✅ NeMo Curator cluster is ready!"
echo "🔗 Connect to scheduler: kubectl exec -it ${SCHEDULER_POD} -- bash"
```

## 11. Run Data Processing

Connect to the scheduler pod and run your processing:

```bash
# Connect to scheduler
DASK_CLUSTER_NAME="nemo-curator-cluster"
SCHEDULER_POD=$(kubectl get pods -l "dask.org/cluster-name=$DASK_CLUSTER_NAME,dask.org/component=scheduler" -o name)
kubectl exec -it ${SCHEDULER_POD} -- bash
```

Inside the scheduler pod:

```bash
# Set AWS credentials (use your actual temporary credentials)
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key" 
export AWS_SESSION_TOKEN="your-session-token"

# Create output directories
mkdir -p /data/curator/{output,logs,profiles}

# Run NeMo Curator processing
nemo_curator \
    --input-data-dirs /data/raw \
    --output-dir /data/curator/output \
    --log-dir /data/curator/logs \
    --scheduler-address localhost:8786 \
    --num-workers 2 \
    --gpu-per-worker 8
```

## 12. Monitor and Manage

```bash
#!/bin/bash
# monitor-cluster.sh

source nemo-env.sh

echo "=== Cluster Monitoring ==="

# Check cluster status
echo "🔍 Cluster Status:"
kubectl get nodes
kubectl get pods --all-namespaces

echo -e "\n📊 Dask Cluster:"
kubectl get daskclusters
kubectl get pods -l app.kubernetes.io/name=dask

echo -e "\n💾 Storage Status:"
kubectl get pv,pvc

echo -e "\n🎮 GPU Status:"
kubectl describe nodes | grep -A 5 "nvidia.com/gpu"

# Check FSx sync status
echo -e "\n📁 FSx Sync Status:"
aws fsx describe-data-repository-tasks \
    --filters Name=file-system-id,Values=${FSX_ID} \
    --region ${AWS_REGION} \
    --query 'DataRepositoryTasks[*].[TaskId,Lifecycle,Type]' \
    --output table

echo -e "\n📈 Resource Usage:"
kubectl top nodes
kubectl top pods
```

## 13. Cleanup

```bash
#!/bin/bash  
# cleanup.sh

source nemo-env.sh

echo "Cleaning up NeMo Curator resources..."

# Delete Dask cluster
kubectl delete daskcluster nemo-curator-cluster --ignore-not-found

# Delete helper pods
kubectl delete pod nemo-workspace-busybox --ignore-not-found

# Delete storage resources (optional - this will delete data!)
read -p "Delete storage resources? This will DELETE ALL DATA (y/N): " confirm
if [[ $confirm == [yY] ]]; then
    kubectl delete pvc nemo-workspace
    kubectl delete pv fsx-lustre-pv
    kubectl delete sc fsx-lustre-sc
    
    # Delete FSx filesystem (this deletes all data!)
    aws fsx delete-file-system --file-system-id ${FSX_ID} --region ${AWS_REGION}
    
    # Delete S3 bucket (this deletes all data!)
    aws s3 rb s3://${S3_BUCKET_NAME} --force
fi

# Delete secrets
kubectl delete secret nvcrimagepullsecret --ignore-not-found

# Uninstall Helm charts
helm uninstall gpu-operator -n gpu-operator
helm uninstall aws-fsx-csi-driver -n kube-system

echo "Cleanup completed"
```

## Complete Setup Script

Run everything in sequence:

```bash
#!/bin/bash
# complete-setup.sh

set -e  # Exit on any error

echo "🚀 Starting complete NeMo Curator setup..."

# 1. Environment setup
./setup-environment.sh
source nemo-env.sh

# 2. Create EKS cluster (if needed)
# ./create-eks-cluster.sh

# 3. Install components
./install-components.sh

# 4. Setup storage
./setup-storage.sh

# 5. Setup DRA
./setup-dra.sh

# 6. Setup K8s storage
./setup-k8s-storage.sh  

# 7. Setup registry
./setup-registry.sh

# 8. Create helper pods
./create-helper-pods.sh

# 9. Launch NeMo Curator
./launch-nemo-curator.sh

echo "✅ NeMo Curator setup complete!"
echo "📖 Next steps:"
echo "   1. Upload your data to S3 bucket: ${S3_BUCKET_NAME}"
echo "   2. Connect to scheduler pod to run processing"
echo "   3. Monitor with: ./monitor-cluster.sh"
```

## Key Success Factors

Based on real implementation experience:

1. **Storage Subnet Matching**: Ensure FSx is in the same subnet as EKS nodes
2. **Security Groups**: Configure proper ingress/egress rules between FSx and EKS
3. **Image Selection**: Use `nvcr.io/nvidia/nemo:24.03.framework` for reliability
4. **Resource Allocation**: 8 GPUs per worker works well on p3.8xlarge instances
5. **Data Repository Association**: Essential for automatic S3/FSx synchronization

This guide provides a complete, tested path from cluster creation to running NeMo Curator workloads.