#!/usr/bin/env python3
# analyze_redpajama.py
import os
import json
import time
import logging
import multiprocessing
import random
from pathlib import Path
from collections import defaultdict
import gc
import traceback

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('redpajama_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def analyze_jsonl_file(file_path):
    """Analyze a single jsonl file with memory-efficient approach"""
    try:
        stats = {
            'file_path': file_path,
            'doc_count': 0,
            'total_chars': 0,
            'min_length': float('inf'),
            'max_length': 0,
            'length_sum': 0,
            'length_sum_sq': 0,
            'fields': set(),
            'sample_docs': [],
            'errors': 0
        }
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    doc = json.loads(line.strip())
                    stats['doc_count'] += 1
                    
                    # Track fields
                    if isinstance(doc, dict):
                        stats['fields'].update(doc.keys())
                        
                        # Analyze text field if present
                        if 'text' in doc and doc['text']:
                            text_len = len(doc['text'])
                        elif 'raw_content' in doc and doc['raw_content']:
                            text_len = len(doc['raw_content'])
                        else:
                            text_len = 0
                            
                        if text_len > 0:
                            stats['total_chars'] += text_len
                            stats['min_length'] = min(stats['min_length'], text_len)
                            stats['max_length'] = max(stats['max_length'], text_len)
                            stats['length_sum'] += text_len
                            stats['length_sum_sq'] += text_len * text_len
                        
                        # Collect sample documents (first 3 only)
                        if len(stats['sample_docs']) < 3:
                            sample_doc = {}
                            for key in ['id', 'text', 'raw_content']:
                                if key in doc:
                                    if key in ['text', 'raw_content'] and len(doc[key]) > 500:
                                        sample_doc[key] = doc[key][:500] + "..."
                                    else:
                                        sample_doc[key] = doc[key]
                            stats['sample_docs'].append(sample_doc)
                    
                except json.JSONDecodeError:
                    stats['errors'] += 1
                    if stats['errors'] <= 3:  # Log first few errors only
                        logger.warning(f"JSON decode error in {file_path}:line {line_num}")
                
                # Memory management - process in chunks
                if line_num % 100000 == 0:
                    gc.collect()
        
        # Convert set to list for JSON serialization
        stats['fields'] = list(stats['fields'])
        
        # Fix infinite min_length if no valid documents
        if stats['min_length'] == float('inf'):
            stats['min_length'] = 0
            
        return stats
        
    except Exception as e:
        logger.error(f"Error analyzing {file_path}: {str(e)}")
        return None

def find_jsonl_files(input_dir):
    """Find all jsonl files in the directory"""
    jsonl_files = []
    for root, dirs, files in os.walk(input_dir):
        for filename in files:
            if filename.endswith('.jsonl'):
                file_path = os.path.join(root, filename)
                # Check if file is not empty
                if os.path.getsize(file_path) > 0:
                    jsonl_files.append(file_path)
    return jsonl_files

def analyze_redpajama_data(input_dir, output_dir, max_workers=4, sample_size=100):
    """Main analysis function with reduced memory footprint"""
    
    logger.info(f"Starting RedPajama data analysis using {max_workers} workers")
    logger.info(f"Input directory: {input_dir}")
    logger.info(f"Output directory: {output_dir}")
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Find all jsonl files
    logger.info("Scanning for jsonl files...")
    jsonl_files = find_jsonl_files(input_dir)
    logger.info(f"Found {len(jsonl_files)} jsonl files")
    
    if not jsonl_files:
        logger.error("No jsonl files found!")
        return
    
    # Process files in smaller batches to manage memory
    batch_size = max_workers * 4  # Process 4 batches per worker at a time
    all_stats = []
    
    for i in range(0, len(jsonl_files), batch_size):
        batch_files = jsonl_files[i:i + batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}/{(len(jsonl_files) + batch_size - 1)//batch_size}")
        
        # Analyze files in parallel
        start_time = time.time()
        
        from multiprocessing import Pool
        with Pool(max_workers) as pool:
            batch_stats = pool.map(analyze_jsonl_file, batch_files)
        
        # Filter out failed analyses and add to overall stats
        batch_stats = [stat for stat in batch_stats if stat is not None]
        all_stats.extend(batch_stats)
        
        # Force garbage collection between batches
        gc.collect()
        
        batch_time = time.time() - start_time
        logger.info(f"Batch processed in {batch_time:.2f} seconds")
    
    logger.info("Calculating aggregate statistics...")
    
    # Aggregate statistics efficiently
    total_docs = sum(stat['doc_count'] for stat in all_stats)
    total_chars = sum(stat['total_chars'] for stat in all_stats)
    total_errors = sum(stat['errors'] for stat in all_stats)
    
    # Calculate text statistics
    if total_docs > 0:
        # Calculate mean and std dev without storing all lengths
        total_length_sum = sum(stat['length_sum'] for stat in all_stats)
        total_length_sum_sq = sum(stat['length_sum_sq'] for stat in all_stats)
        
        mean_length = total_length_sum / total_docs
        variance = (total_length_sum_sq / total_docs) - (mean_length ** 2)
        std_dev = variance ** 0.5 if variance > 0 else 0
        
        text_stats = {
            'count': total_docs,
            'min_length': min(stat['min_length'] for stat in all_stats if stat['doc_count'] > 0),
            'max_length': max(stat['max_length'] for stat in all_stats),
            'mean_length': mean_length,
            'std_dev': std_dev,
            'total_characters': total_chars
        }
    else:
        text_stats = {}
    
    # Collect all unique fields
    all_fields = set()
    for stat in all_stats:
        all_fields.update(stat['fields'])
    
    # Create comprehensive report
    report = {
        'analysis_metadata': {
            'analysis_date': time.strftime('%Y-%m-%d %H:%M:%S'),
            'input_directory': input_dir,
            'files_analyzed': len(all_stats),
            'workers_used': max_workers
        },
        'dataset_summary': {
            'total_documents': total_docs,
            'total_characters': total_chars,
            'total_files': len(all_stats),
            'total_errors': total_errors,
            'unique_fields': sorted(list(all_fields))
        },
        'text_statistics': text_stats,
        'file_statistics': {
            'files_processed': len(all_stats),
            'avg_docs_per_file': total_docs / len(all_stats) if all_stats else 0,
            'files_with_errors': sum(1 for stat in all_stats if stat['errors'] > 0)
        }
    }
    
    # Save comprehensive report
    report_path = os.path.join(output_dir, 'dataset_analysis_report.json')
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Comprehensive report saved to: {report_path}")
    
    # Create sample documents from collected samples
    logger.info(f"Creating sample documents...")
    sample_docs = []
    
    # Collect samples from different files
    for stat in all_stats:
        sample_docs.extend(stat['sample_docs'])
        if len(sample_docs) >= sample_size:
            break
    
    # Randomly sample if we have too many
    if len(sample_docs) > sample_size:
        sample_docs = random.sample(sample_docs, sample_size)
    
    sample_path = os.path.join(output_dir, 'sample_documents.json')
    with open(sample_path, 'w', encoding='utf-8') as f:
        json.dump(sample_docs, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Sample documents saved to: {sample_path}")
    
    # Print summary
    logger.info("\n" + "="*60)
    logger.info("REDPAJAMA DATASET ANALYSIS SUMMARY")
    logger.info("="*60)
    logger.info(f"📁 Files analyzed: {len(all_stats):,}")
    logger.info(f"📄 Total documents: {total_docs:,}")
    logger.info(f"📝 Total characters: {total_chars:,}")
    logger.info(f"⚠️  Total errors: {total_errors:,}")
    
    if text_stats:
        logger.info(f"📊 Average document length: {text_stats['mean_length']:.0f} characters")
        logger.info(f"📏 Document length range: {text_stats['min_length']:,} - {text_stats['max_length']:,}")
    
    logger.info("="*60)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze RedPajama jsonl files with memory management')
    parser.add_argument('--input-dir', type=str, required=True,
                        help='Directory containing jsonl files')
    parser.add_argument('--output-dir', type=str, default='./analysis_output',
                        help='Output directory for analysis reports')
    parser.add_argument('--workers', type=int, default=4,
                        help='Number of worker processes (default: 4)')
    parser.add_argument('--sample-size', type=int, default=100,
                        help='Number of sample documents to save')
    
    args = parser.parse_args()
    
    analyze_redpajama_data(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        max_workers=args.workers,
        sample_size=args.sample_size
    )

    '''
    # Basic usage
python3 analyze_redpajama.py --input-dir /path/to/your/jsonl/files

# With custom output directory
python3 analyze_redpajama.py \
    --input-dir /data/rpv2-processed \
    --output-dir /data/analysis \
    --workers 8 \
    --sample-size 2000
    '''