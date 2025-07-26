#!/usr/bin/env python3
# step1_manual_reshard_fast.py
import os
import json
import time
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('manual_resharding.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def process_file_batch(batch_info):
    """Process a batch of input files"""
    input_files, output_dir, target_size_bytes, batch_id, start_file_idx = batch_info
    
    current_output_file = None
    current_size = 0
    file_idx = start_file_idx
    docs_processed = 0
    
    try:
        for input_file in input_files:
            with open(input_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Check if we need a new output file
                    if current_output_file is None or current_size >= target_size_bytes:
                        if current_output_file:
                            current_output_file.close()
                        
                        output_path = os.path.join(output_dir, f"rpv2-2023-06-{file_idx:06d}.jsonl")
                        current_output_file = open(output_path, 'w', encoding='utf-8')
                        current_size = 0
                        file_idx += 1
                    
                    # Write line
                    current_output_file.write(line + '\n')
                    current_size += len(line.encode('utf-8')) + 1
                    docs_processed += 1
        
        if current_output_file:
            current_output_file.close()
        
        return docs_processed, file_idx - start_file_idx, None
        
    except Exception as e:
        if current_output_file:
            current_output_file.close()
        return 0, 0, str(e)

def fast_manual_reshard(input_dir, output_dir, target_size_mb=100, max_workers=None):
    """Fast parallel manual resharding"""
    
    logger.info("="*60)
    logger.info("FAST MANUAL RESHARDING")
    logger.info("="*60)
    
    if max_workers is None:
        max_workers = max(1, multiprocessing.cpu_count() - 1)
    
    target_size_bytes = target_size_mb * 1024 * 1024
    os.makedirs(output_dir, exist_ok=True)
    
    logger.info(f"Input: {input_dir}")
    logger.info(f"Output: {output_dir}")
    logger.info(f"Target size: {target_size_mb}MB per file")
    logger.info(f"Workers: {max_workers}")
    
    # Find all JSONL files
    logger.info("Scanning for JSONL files...")
    jsonl_files = []
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith('.jsonl'):
                jsonl_files.append(os.path.join(root, file))
    
    jsonl_files.sort()  # Process in consistent order
    logger.info(f"Found {len(jsonl_files)} JSONL files")
    
    if not jsonl_files:
        logger.error("No JSONL files found!")
        return False
    
    # Show some sample files
    logger.info("Sample files:")
    for i, file_path in enumerate(jsonl_files[:5]):
        rel_path = os.path.relpath(file_path, input_dir)
        file_size = os.path.getsize(file_path) / (1024*1024)  # MB
        logger.info(f"  {i+1}. {rel_path} ({file_size:.1f}MB)")
    
    # Estimate total size
    total_size = sum(os.path.getsize(f) for f in jsonl_files) / (1024*1024*1024)  # GB
    logger.info(f"Total data size: ~{total_size:.2f}GB")
    
    # Create batches for parallel processing
    batch_size = max(1, len(jsonl_files) // max_workers)
    batches = []
    
    for i in range(0, len(jsonl_files), batch_size):
        batch_files = jsonl_files[i:i + batch_size]
        batch_info = (batch_files, output_dir, target_size_bytes, i // batch_size, i * 100)
        batches.append(batch_info)
    
    logger.info(f"Created {len(batches)} processing batches")
    
    # Process batches in parallel
    start_time = time.time()
    total_docs = 0
    total_output_files = 0
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {
            executor.submit(process_file_batch, batch): i 
            for i, batch in enumerate(batches)
        }
        
        for future in as_completed(future_to_batch):
            batch_idx = future_to_batch[future]
            try:
                docs, files, error = future.result()
                if error:
                    logger.error(f"Batch {batch_idx} failed: {error}")
                else:
                    total_docs += docs
                    total_output_files += files
                    logger.info(f"Batch {batch_idx + 1}/{len(batches)} complete: {docs:,} docs, {files} files")
            except Exception as e:
                logger.error(f"Batch {batch_idx} exception: {str(e)}")
    
    processing_time = time.time() - start_time
    
    # Count actual output files
    actual_output_files = len([f for f in os.listdir(output_dir) if f.endswith('.jsonl')])
    
    logger.info("="*60)
    logger.info("RESHARDING COMPLETE")
    logger.info("="*60)
    logger.info(f"Total documents processed: {total_docs:,}")
    logger.info(f"Output files created: {actual_output_files}")
    logger.info(f"Processing time: {processing_time:.2f} seconds")
    logger.info(f"Speed: {total_docs / processing_time:,.0f} docs/second")
    
    # Save metadata
    metadata = {
        'processing_method': 'fast_manual_parallel',
        'input_dir': input_dir,
        'output_dir': output_dir,
        'input_file_count': len(jsonl_files),
        'output_file_count': actual_output_files,
        'total_documents': total_docs,
        'target_size_mb': target_size_mb,
        'max_workers': max_workers,
        'processing_time': processing_time,
        'docs_per_second': total_docs / processing_time if processing_time > 0 else 0,
        'total_size_gb': total_size
    }
    
    metadata_path = os.path.join(output_dir, 'resharding_metadata.json')
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    logger.info(f"Metadata saved: {metadata_path}")
    logger.info("✅ Manual resharding completed successfully!")
    
    return True

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Fast manual resharding for RedPajama data')
    parser.add_argument('--input-dir', type=str, required=True)
    parser.add_argument('--output-dir', type=str, required=True)
    parser.add_argument('--target-size', type=int, default=100, help='Target size in MB')
    parser.add_argument('--workers', type=int, default=None, help='Number of workers')
    
    args = parser.parse_args()
    
    success = fast_manual_reshard(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        target_size_mb=args.target_size,
        max_workers=args.workers
    )
    
    if success:
        print("✅ Fast manual resharding completed!")
    else:
        print("❌ Resharding failed!")
        exit(1)


    '''
# Run the fast manual resharding
python step1_manual_reshard_fast.py \
    --input-dir /data/rpv2-processed/ \
    --output-dir /data/processing/rpv2-2023-06-resharded \
    --target-size 100 \
    --workers 8

# Monitor progress
tail -f manual_resharding.log
    '''