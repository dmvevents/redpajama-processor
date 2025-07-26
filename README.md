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