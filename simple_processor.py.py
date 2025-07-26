#!/usr/bin/env python3
import os
import json
import gzip
import time
import logging
from pathlib import Path
import traceback
import concurrent.futures

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/processing/redpajama_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def process_file(input_path, output_path):
    """Process a single json.gz file with robust error handling"""
    try:
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Skip if output exists and is newer than input
        if os.path.exists(output_path) and os.path.getmtime(output_path) > os.path.getmtime(input_path):
            logger.debug(f"Skipping {input_path}: output file already exists and is newer")
            return True, 0, "skipped"
        
        # Try to open and process
        try:
            doc_count = 0
            with gzip.open(input_path, 'rt', encoding='utf-8', errors='replace') as f_in, \
                 open(output_path, 'w', encoding='utf-8') as f_out:
                for line in f_in:
                    try:
                        # Validate JSON
                        doc = json.loads(line)
                        f_out.write(json.dumps(doc) + '\n')
                        doc_count += 1
                    except json.JSONDecodeError:
                        continue
            
            return True, doc_count, "processed"
            
        except Exception as e:
            error_msg = f"Error processing {input_path}: {str(e)}"
            logger.error(error_msg)
            if os.path.exists(output_path):
                os.remove(output_path)
            return False, 0, error_msg
    
    except Exception as e:
        error_msg = f"Unexpected error with {input_path}: {str(e)}"
        logger.error(error_msg)
        return False, 0, error_msg

def find_files(input_dir):
    """Find all json.gz files in the directory"""
    file_paths = []
    for root, dirs, files in os.walk(input_dir):
        for filename in files:
            if filename.endswith('.json.gz'):
                input_path = os.path.join(root, filename)
                rel_path = os.path.relpath(input_path, input_dir)
                output_filename = os.path.splitext(os.path.splitext(rel_path)[0])[0] + '.jsonl'
                file_paths.append((input_path, output_filename))
                
    return file_paths

def process_directory(input_dir, output_dir, workers=8, batch_size=50):
    """Process all json.gz files in the directory using multiprocessing"""
    # Track statistics
    start_time = time.time()
    files_processed = 0
    files_skipped = 0
    files_failed = 0
    docs_processed = 0
    failed_files = []
    
    # Find all files
    logger.info(f"Scanning for files in {input_dir}...")
    file_paths = find_files(input_dir)
    logger.info(f"Found {len(file_paths)} json.gz files")
    
    # Create directory structure in output dir
    existing_dirs = set()
    for _, rel_path in file_paths:
        output_path = os.path.join(output_dir, rel_path)
        output_dir_path = os.path.dirname(output_path)
        if output_dir_path not in existing_dirs:
            os.makedirs(output_dir_path, exist_ok=True)
            existing_dirs.add(output_dir_path)
    
    # Process files in batches to manage memory
    num_batches = (len(file_paths) + batch_size - 1) // batch_size
    
    for batch_idx in range(num_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(file_paths))
        batch = file_paths[start_idx:end_idx]
        
        logger.info(f"Processing batch {batch_idx+1}/{num_batches} with {len(batch)} files")
        batch_start_time = time.time()
        
        # Prepare arguments for multiprocessing
        file_args = []
        for input_path, rel_path in batch:
            output_path = os.path.join(output_dir, rel_path)
            file_args.append((input_path, output_path))
        
        # Process files in parallel
        batch_processed = 0
        batch_skipped = 0
        batch_failed = 0
        batch_docs = 0
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            future_to_path = {executor.submit(process_file, arg[0], arg[1]): arg[0] for arg in file_args}
            
            for future in concurrent.futures.as_completed(future_to_path):
                input_path = future_to_path[future]
                try:
                    success, doc_count, status = future.result()
                    if success:
                        if status == "processed":
                            batch_processed += 1
                            batch_docs += doc_count
                            logger.debug(f"Processed {input_path}: {doc_count} documents")
                        else:  # skipped
                            batch_skipped += 1
                    else:
                        batch_failed += 1
                        failed_files.append((input_path, status))
                        logger.warning(f"Failed to process {input_path}: {status}")
                except Exception as e:
                    batch_failed += 1
                    failed_files.append((input_path, str(e)))
                    logger.error(f"Exception processing {input_path}: {e}")
        
        # Update stats
        files_processed += batch_processed
        files_skipped += batch_skipped
        files_failed += batch_failed
        docs_processed += batch_docs
        
        batch_time = time.time() - batch_start_time
        logger.info(f"Batch {batch_idx+1} complete: {batch_processed} processed, {batch_skipped} skipped, " 
                   f"{batch_failed} failed, {batch_docs} docs in {batch_time:.1f}s")
    
    # Write failed files to disk
    if failed_files:
        with open(os.path.join(output_dir, "failed_files.txt"), "w") as f:
            for path, reason in failed_files:
                f.write(f"{path}\t{reason}\n")
    
    # Final report
    elapsed = time.time() - start_time
    logger.info(f"\nPROCESSING SUMMARY:")
    logger.info(f"Files processed: {files_processed}")
    logger.info(f"Files skipped: {files_skipped}")
    logger.info(f"Files failed: {files_failed}")
    logger.info(f"Documents processed: {docs_processed}")
    logger.info(f"Total time: {elapsed:.1f}s")
    
    if files_failed > 0:
        logger.info(f"Failed files written to {os.path.join(output_dir, 'failed_files.txt')}")
    
    return files_processed, files_skipped, files_failed, docs_processed

def analyze_documents(input_dir, sample_size=1000):
    """Analyze and sample documents from processed files"""
    import random
    
    logger.info(f"Analyzing documents in {input_dir}")
    
    # Find all jsonl files
    jsonl_files = []
    for root, dirs, files in os.walk(input_dir):
        for filename in files:
            if filename.endswith('.jsonl'):
                jsonl_files.append(os.path.join(root, filename))
    
    if not jsonl_files:
        logger.error("No JSONL files found!")
        return
    
    # Sample some files for analysis
    sampled_files = random.sample(jsonl_files, min(len(jsonl_files), 20))
    
    # Statistics
    stats = {
        'files_examined': len(sampled_files),
        'total_docs': 0,
        'total_tokens': 0,
        'total_chars': 0,
        'min_length': float('inf'),
        'max_length': 0
    }
    
    # Sample documents
    sampled_docs = []
    
    # Process files to gather statistics
    for file_path in sampled_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        doc = json.loads(line)
                        stats['total_docs'] += 1
                        
                        if 'text' in doc:
                            text = doc['text']
                            text_len = len(text)
                            stats['total_chars'] += text_len
                            stats['total_tokens'] += len(text.split())
                            stats['min_length'] = min(stats['min_length'], text_len)
                            stats['max_length'] = max(stats['max_length'], text_len)
                        
                        # Add to sample if needed
                        if len(sampled_docs) < sample_size and random.random() < 0.01:
                            sampled_docs.append(doc)
                    
                    except json.JSONDecodeError:
                        continue
                
        except Exception as e:
            logger.error(f"Error analyzing {file_path}: {e}")
    
    # Calculate averages
    if stats['total_docs'] > 0:
        stats['avg_tokens'] = stats['total_tokens'] / stats['total_docs']
        stats['avg_chars'] = stats['total_chars'] / stats['total_docs']
    
        # Write sample to file
        if sampled_docs:
            sample_path = os.path.join(input_dir, "sample_documents.jsonl")
            with open(sample_path, 'w', encoding='utf-8') as f:
                for doc in sampled_docs:
                    f.write(json.dumps(doc) + '\n')
            logger.info(f"Wrote {len(sampled_docs)} sample documents to {sample_path}")
        
        # Log statistics
        logger.info("\nANALYSIS SUMMARY:")
        logger.info(f"Files examined: {stats['files_examined']}")
        logger.info(f"Documents analyzed: {stats['total_docs']}")
        logger.info(f"Average tokens per document: {stats['avg_tokens']:.1f}")
        logger.info(f"Average characters per document: {stats['avg_chars']:.1f}")
        logger.info(f"Min document length: {stats['min_length']} chars")
        logger.info(f"Max document length: {stats['max_length']} chars")
        
        # Write stats to json
        stats_path = os.path.join(input_dir, "document_stats.json")
        with open(stats_path, 'w') as f:
            json.dump(stats, f, indent=2)
        logger.info(f"Statistics written to {stats_path}")
        
    else:
        logger.info("No documents found for analysis")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Process RedPajama data without GPU dependencies')
    parser.add_argument('--input-dir', type=str, default="/data/redpajama-v2/en/documents/2023-06",
                        help='Input directory with json.gz files')
    parser.add_argument('--output-dir', type=str, default="/processing/rpv2-processed",
                        help='Output directory for processed files')
    parser.add_argument('--workers', type=int, default=8,
                        help='Number of worker processes (default: 8)')
    parser.add_argument('--batch-size', type=int, default=50,
                        help='Number of files to process in each batch (default: 50)')
    
    args = parser.parse_args()
    
    # Make sure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Process files
    logger.info(f"Starting processing with {args.workers} workers")
    files_processed, files_skipped, files_failed, docs_processed = process_directory(
        args.input_dir, 
        args.output_dir,
        workers=args.workers,
        batch_size=args.batch_size
    )
    
    # Analyze documents if any were processed
    if files_processed > 0:
        analyze_documents(args.output_dir)

''''
python /data/simple_processor.py \
  --input-dir /data/redpajama/documents/2023-06 \
  --output-dir /data/rpv2-processed \
  --workers 40 \
  --batch-size 100
'''