# RedPajama Processor

A high-performance tool for processing RedPajama V2 dataset using multi-core CPU and GPU acceleration.

## Features

- Multi-core processing using Dask distributed
- Optional GPU acceleration with RAPIDS ecosystem
- Efficient conversion of compressed JSON to JSONL format
- NeMo Curator integration for document processing
- Kubernetes deployment ready

## Requirements

- Python 3.8+
- Dask
- RAPIDS (optional, for GPU acceleration)
- NeMo Curator

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/redpajama-processor.git
cd redpajama-processor

# Install dependencies
./setup.sh
Usage
# Basic usage
python redpajama_processor.py --input-dir /path/to/input --output-dir /path/to/output

# Specify number of workers
python redpajama_processor.py --workers 16

# Disable GPU acceleration
python redpajama_processor.py --no-gpu
Kubernetes Deployment
kubectl apply -f redpajama-k8s-deployment.yaml
License
MIT
