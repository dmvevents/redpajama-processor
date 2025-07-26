#!/usr/bin/env python3
import os
import json
import gzip
import time
import logging
import multiprocessing
import numpy as np
from pathlib import Path
from tqdm import tqdm
import gc
import traceback

# Set up Dask with proper resource utilization
import dask
from dask.distributed import Client, LocalCluster
import dask.bag as db

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('redpajama_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def convert_single_file(file_tuple):
    """Convert a single json.gz file to jsonl with robust error handling"""
    input_path, output_path = file_tuple
    
    try:
        # Skip zero-byte files
        if os.path.getsize(input_path) == 0:
            return False, input_path, f"Skipping zero-byte file: {input_path}"

        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Try to read the compressed file
        try:
            # First, check if file is actually gzipped
            with open(input_path, 'rb') as f:
                magic = f.read(2)
                if magic != b'\x1f\x8b':  # gzip magic number
                    return False, input_path, f"Not a valid gzip file: {input_path}"
            
            # Try to process the file
            doc_count = 0
            with gzip.open(input_path, 'rt', encoding='utf-8', errors='replace') as f_in, \
                 open(output_path, 'w', encoding='utf-8') as f_out:
                for i, line in enumerate(f_in):
                    try:
                        # Basic validation that it's valid JSON
                        json.loads(line)
                        f_out.write(line)
                        doc_count += 1
                    except json.JSONDecodeError:
                        # Skip individual malformed JSON lines
                        continue
            
            # If no documents were written, consider it failed
            if doc_count == 0:
                if os.path.exists(output_path):
                    os.remove(output_path)
                return False, input_path, f"No valid documents found in {input_path}"
            
            return True, input_path, f"Converted {input_path} with {doc_count} documents"
            
        except (gzip.BadGzipFile, EOFError, zlib.error) as e:
            # Handle gzip-specific errors
            return False, input_path, f"Gzip error in {input_path}: {str(e)}"
            
        except UnicodeDecodeError as e:
            # Handle encoding errors
            return False, input_path, f"Unicode decode error in {input_path}: {str(e)}"
    
    except Exception as e:
        error_msg = f"Error processing {input_path}: {str(e)}"
        logger.error(f"{error_msg}\n{traceback.format_exc()}")
        # Clean up partial output file if it exists
        if os.path.exists(output_path):
            os.remove(output_path)
        return False, input_path, error_msg

def convert_json_gz_to_jsonl(input_dir: str, output_dir: str, num_workers: int = None, 
                            memory_limit: str = '4GB', batch_size: int = 100) -> None:
    """
    Convert all json.gz files in input_dir to jsonl files in output_dir using all available cores.
    Process in batches to manage memory usage.
    """
    # Automatically determine optimal number of workers if not specified
    if num_workers is None:
        num_workers = max(1, multiprocessing.cpu_count() - 1)
    
    logger.info(f"Using {num_workers} workers for parallel processing")
    
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Find all json.gz files recursively
    file_paths = []
    for root, dirs, files in os.walk(input_dir):
        for filename in files:
            if filename.endswith(".json.gz"):
                input_path = os.path.join(root, filename)
                
                # Preserve directory structure in output
                rel_path = os.path.relpath(input_path, input_dir)
                output_path = os.path.join(
                    output_dir, 
                    os.path.splitext(os.path.splitext(rel_path)[0])[0] + ".jsonl"
                )
                
                # Only add if output doesn't exist or input is newer than output
                if not os.path.exists(output_path) or \
                   os.path.getmtime(input_path) > os.path.getmtime(output_path):
                    file_paths.append((input_path, output_path))
    
    logger.info(f"Found {len(file_paths)} json.gz files to convert")
    
    # Process in batches to limit memory usage
    total_success = 0
    total_failed = 0
    failed_files = []
    
    # Calculate number of batches
    num_batches = (len(file_paths) + batch_size - 1) // batch_size
    
    for batch_idx in range(num_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(file_paths))
        batch = file_paths[start_idx:end_idx]
        
        logger.info(f"Processing batch {batch_idx + 1}/{num_batches} with {len(batch)} files")
        
        # Set up distributed processing with proper resource allocation
        try:
            cluster = LocalCluster(
                n_workers=num_workers,
                threads_per_worker=1,  # One thread per worker for CPU-bound tasks
                memory_limit=memory_limit,
                silence_logs=logging.WARNING
            )
            client = Client(cluster)
            logger.info(f"Batch {batch_idx + 1}: Dask dashboard at {client.dashboard_link}")
            
            # Create a Dask bag from the file paths and apply the function in parallel
            bag = db.from_sequence(batch, npartitions=num_workers)
            results = bag.map(convert_single_file).compute()
            
            # Collect batch results
            batch_success = 0
            batch_failed = 0
            for success, path, message in results:
                if success:
                    batch_success += 1
                    logger.debug(message)
                else:
                    batch_failed += 1
                    failed_files.append(path)
                    logger.warning(message)
            
            logger.info(f"Batch {batch_idx + 1}: {batch_success} succeeded, {batch_failed} failed")
            total_success += batch_success
            total_failed += batch_failed
            
        except Exception as e:
            logger.error(f"Batch {batch_idx + 1} failed: {str(e)}")
            total_failed += len(batch)
            failed_files.extend([path for path, _ in batch])
        
        finally:
            # Clean up the client and cluster
            client.close()
            cluster.close()
            
            # Force garbage collection
            gc.collect()
    
    # Write failed files to disk
    if failed_files:
        with open(os.path.join(output_dir, "failed_files.txt"), "w") as f:
            for path in failed_files:
                f.write(f"{path}\n")
    
    logger.info(f"Conversion complete: {total_success}/{len(file_paths)} files converted successfully")
    logger.info(f"Failed files: {total_failed} (see failed_files.txt for details)")
    
    return total_success

def process_redpajama(input_data_dir, output_base_dir, max_workers=None, sample_size=1000, 
                     use_gpu=True, memory_limit='4GB', batch_size=100):
    """Main processing function with multiprocessing and GPU acceleration"""
    # Define directories
    raw_output_dir = os.path.join(output_base_dir, "rpv2-2023-06-raw")
    output_data_dir = os.path.join(output_base_dir, "rpv2-2023-06")
    
    # Auto-detect cores if not specified
    if max_workers is None:
        max_workers = max(1, multiprocessing.cpu_count() - 1)
        logger.info(f"Auto-detected {max_workers} CPU cores")
    
    # Check GPU availability
    if use_gpu:
        try:
            import cudf
            import cupy
            gpu_available = True
            logger.info("GPU support enabled with RAPIDS (cuDF)")
        except ImportError:
            gpu_available = False
            logger.warning("GPU libraries not found. Falling back to CPU processing.")
            logger.info("To enable GPU: pip install cudf-cu11 cupy-cuda11x")
    else:
        gpu_available = False
        logger.info("GPU processing disabled by configuration")
    
    # Step 1: Create symbolic links to raw data for simplified paths
    logger.info("Creating symbolic links to raw data...")
    os.makedirs(raw_output_dir, exist_ok=True)
    
    for root, dirs, files in os.walk(input_data_dir):
        for filename in files:
            if filename.endswith(".json.gz"):
                src_path = os.path.join(root, filename)
                segment = os.path.basename(os.path.dirname(src_path))
                dst_path = os.path.join(raw_output_dir, f"{segment}_{filename}")
                
                # Create symlink if it doesn't exist
                if not os.path.exists(dst_path):
                    os.symlink(src_path, dst_path)
    
    # Step 2: Convert json.gz to jsonl using all cores
    logger.info(f"Converting json.gz to jsonl using {max_workers} workers...")
    t0 = time.time()
    success_count = convert_json_gz_to_jsonl(
        raw_output_dir, 
        output_data_dir, 
        num_workers=max_workers,
        memory_limit=memory_limit,
        batch_size=batch_size
    )
    convert_time = time.time() - t0
    logger.info(f"Uncompressing {success_count} files took {convert_time:.2f} seconds "
               f"({convert_time/max(1, success_count):.2f} s/file)")
    
    # Step 3: Load and process with DocumentDataset (with GPU if available)
    try:
        from nemo_curator.datasets import DocumentDataset
        from nemo_curator.utils.distributed_utils import setup_for_distributed_mode
        
        # Configure Dask for GPU if available
        if gpu_available:
            dask.config.set({"dataframe.backend": "cudf"})
            logger.info("Using cuDF as Dask dataframe backend for GPU acceleration")
        
        # Setup distributed processing
        if gpu_available:
            # RAPIDS setup for GPU acceleration
            setup_for_distributed_mode(gpu_mode=True)
            logger.info("Set up distributed mode with GPU acceleration")
        else:
            setup_for_distributed_mode(gpu_mode=False)
            logger.info("Set up distributed mode with CPU processing")
        
        # Process in batches to manage memory
        json_files = []
        for root, dirs, files in os.walk(output_data_dir):
            for filename in files:
                if filename.endswith(".jsonl"):
                    json_files.append(os.path.join(root, filename))
        
        logger.info(f"Found {len(json_files)} jsonl files to load into DocumentDataset")
        
        # Process files in smaller batches if there are many
        if len(json_files) > 100:
            # Sample a subset of files for initial testing
            import random
            sampled_files = random.sample(json_files, min(100, len(json_files)))
            logger.info(f"Sampling {len(sampled_files)} files for initial testing")
        else:
            sampled_files = json_files
        
        logger.info("Loading data into DocumentDataset...")
        t0 = time.time()
        input_dataset = DocumentDataset.read_json(sampled_files, add_filename=True)
        load_time = time.time() - t0
        logger.info(f"Loading into DocumentDataset took {load_time:.2f} seconds")
        
        # Get statistics
        t0 = time.time()
        doc_count = len(input_dataset.df)
        logger.info(f"Total documents: {doc_count:,}")
        
        # Calculate basic stats using Dask/RAPIDS for parallelism
        logger.info("Calculating document statistics...")
        
        if 'text' in input_dataset.df.columns:
            # Calculate text length stats with memory-efficient approach
            def get_text_lengths(partition):
                return partition['text'].str.len()
            
            # Instead of computing all at once, extract statistics iteratively
            text_lengths = input_dataset.df.map_partitions(get_text_lengths)
            
            length_stats = {
                'min_length': int(text_lengths.min().compute()),
                'max_length': int(text_lengths.max().compute()),
                'mean_length': float(text_lengths.mean().compute()),
                'median_length': float(text_lengths.quantile(0.5).compute()),
                'total_characters': int(text_lengths.sum().compute())
            }
            logger.info(f"Text statistics: {json.dumps(length_stats, indent=2)}")
        
        # Save a sample for inspection (using efficient sampling)
        logger.info(f"Saving {min(sample_size, doc_count)} sample documents...")
        if doc_count > 0:
            sample = input_dataset.df.sample(n=min(sample_size, doc_count)).compute()
            sample.to_json(os.path.join(output_base_dir, "sample_documents.json"), 
                          orient="records", lines=True)
        
        stats_time = time.time() - t0
        logger.info(f"Stats calculation took {stats_time:.2f} seconds")
        
        # Performance summary
        logger.info("\n===== PERFORMANCE SUMMARY =====")
        logger.info(f"Files processed: {success_count} files")
        logger.info(f"Documents processed: {doc_count:,} documents")
        logger.info(f"Total processing time: {convert_time + load_time + stats_time:.2f} seconds")
        if doc_count > 0:
            logger.info(f"Processing speed: {doc_count/(convert_time + load_time + stats_time):,.2f} docs/second")
        logger.info("==============================\n")
        
    except ImportError:
        logger.warning("NeMo Curator not installed. Skipping DocumentDataset processing.")
        logger.info("To install NeMo Curator: pip install nemo-curator")
    except Exception as e:
        logger.error(f"Error during DocumentDataset processing: {str(e)}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    import argparse
    import zlib  # Add missing import
    
    parser = argparse.ArgumentParser(description='Process RedPajama data with multi-core and optional GPU acceleration')
    parser.add_argument('--input-dir', type=str, default="/data/redpajama-v2/en/documents/2023-06",
                        help='Input directory with json.gz files')
    parser.add_argument('--output-dir', type=str, default="/processing",
                        help='Output directory for processed files')
    parser.add_argument('--workers', type=int, default=None,
                        help='Number of worker processes (default: auto-detect)')
    parser.add_argument('--sample-size', type=int, default=1000,
                        help='Number of sample documents to save')
    parser.add_argument('--no-gpu', action='store_true',
                        help='Disable GPU acceleration even if available')
    parser.add_argument('--memory-limit', type=str, default='4GB',
                        help='Memory limit per worker (default: 4GB)')
    parser.add_argument('--batch-size', type=int, default=100,
                        help='Number of files to process in each batch (default: 100)')
    
    args = parser.parse_args()
    
    # Process with robust error handling and memory management
    process_redpajama(
        input_data_dir=args.input_dir,
        output_base_dir=args.output_dir,
        max_workers=args.workers,
        sample_size=args.sample_size,
        use_gpu=not args.no_gpu,
        memory_limit=args.memory_limit,
        batch_size=args.batch_size
    )