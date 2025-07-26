#!/usr/bin/env python3
# step2_manual_add_ids.py
import os
import json
import time
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def add_ids_to_file(file_info):
    """Add IDs to a single file"""
    input_file, output_file, id_prefix, start_id = file_info
    
    docs_processed = 0
    current_id = start_id
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f_in, \
             open(output_file, 'w', encoding='utf-8') as f_out:
            
            for line in f_in:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    doc = json.loads(line)
                    # Add ID if not present
                    if 'id' not in doc:
                        doc['id'] = f"{id_prefix}-{current_id:010d}"
                        current_id += 1
                    
                    f_out.write(json.dumps(doc) + '\n')
                    docs_processed += 1
                    
                except json.JSONDecodeError:
                    continue
        
        return docs_processed, current_id, None
        
    except Exception as e:
        return 0, start_id, str(e)

def manual_add_ids(input_dir, output_dir, id_prefix="rpv2-2023-06", max_workers=None):
    """Manually add IDs using parallel processing"""
    
    logger.info("="*60)
    logger.info("MANUAL ID ADDITION")
    logger.info("="*60)
    
    if max_workers is None:
        max_workers = max(1, multiprocessing.cpu_count() - 1)
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Find JSONL files
    jsonl_files = []
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith('.jsonl'):
                jsonl_files.append(os.path.join(root, file))
    
    jsonl_files.sort()
    logger.info(f"Found {len(jsonl_files)} files to process")
    
    # Prepare file processing tasks
    file_tasks = []
    current_id = 0
    
    for input_file in jsonl_files:
        output_file = os.path.join(output_dir, os.path.basename(input_file))
        file_tasks.append((input_file, output_file, id_prefix, current_id))
        current_id += 1000000  # Reserve 1M IDs per file
    
    # Process files in parallel
    start_time = time.time()
    total_docs = 0
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(add_ids_to_file, task): os.path.basename(task[0])
            for task in file_tasks
        }
        
        for future in as_completed(future_to_file):
            filename = future_to_file[future]
            try:
                docs, _, error = future.result()
                if error:
                    logger.error(f"Failed {filename}: {error}")
                else:
                    total_docs += docs
                    logger.info(f"Processed {filename}: {docs:,} docs")
            except Exception as e:
                logger.error(f"Exception {filename}: {str(e)}")
    
    processing_time = time.time() - start_time
    
    logger.info(f"✅ Manual ID addition completed!")
    logger.info(f"Total documents: {total_docs:,}")
    logger.info(f"Processing time: {processing_time:.2f} seconds")
    
    # Save metadata
    metadata = {
        'step': 'manual_add_ids',
        'input_dir': input_dir,
        'output_dir': output_dir,
        'total_documents': total_docs,
        'id_prefix': id_prefix,
        'processing_time': processing_time
    }
    
    with open(os.path.join(output_dir, 'add_ids_metadata.json'), 'w') as f:
        json.dump(metadata, f, indent=2)

if __name__ == "__main__":
    manual_add_ids(
        "/data/processing/rpv2-2023-06-resharded",
        "/data/processing/rpv2-2023-06-id",
        "rpv2-2023-06"
    )