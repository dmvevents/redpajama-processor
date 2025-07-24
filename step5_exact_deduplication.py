#!/usr/bin/env python3
# step5_exact_deduplication.py
import os
import time
import logging
import json
from nemo_curator.datasets import DocumentDataset
from nemo_curator.modules import ExactDuplicates
from nemo_curator.utils.file_utils import expand_outdir_and_mkdir
from nemo_curator.utils.distributed_utils import get_client, get_num_workers

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('step5_exact_deduplication.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_gpu_client():
    """Setup GPU client for processing"""
    def pre_imports():
        import cudf  # noqa: F401
    
    try:
        scheduler_address = os.getenv("SCHEDULER_ADDRESS")
        gpu_client = get_client(scheduler_address=scheduler_address)
        num_workers = get_num_workers(gpu_client)
        logger.info(f"GPU client setup successful. Number of workers: {num_workers}")
        
        gpu_client.run(pre_imports)
        logger.info("GPU pre-imports completed")
        return gpu_client, num_workers
    except Exception as e:
        logger.warning(f"GPU client setup failed: {str(e)}")
        logger.info("Falling back to CPU processing")
        return None, 0

def exact_deduplication(base_dir, language="en", hash_method="md5", use_gpu=True):
    """Perform exact deduplication on cleaned dataset"""
    
    logger.info("="*60)
    logger.info("STEP 5: EXACT DEDUPLICATION")
    logger.info("="*60)
    
    # Define directories
    cleaned_dataset_path = os.path.join(base_dir, f"rpv2-2023-06-{language}-cleaned")
    log_dir = expand_outdir_and_mkdir(os.path.join(base_dir, "logs"))
    output_dir = expand_outdir_and_mkdir(os.path.join(base_dir, "rpv2-2023-06-exact-dedup"))
    
    logger.info(f"Input directory: {cleaned_dataset_path}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Hash method: {hash_method}")
    logger.info(f"Use GPU: {use_gpu}")
    
    # Check if input directory exists
    if not os.path.exists(cleaned_dataset_path):
        logger.error(f"Input directory does not exist: {cleaned_dataset_path}")
        return False
    
    # Setup GPU client if requested
    gpu_client = None
    if use_gpu:
        gpu_client, num_workers = setup_gpu_client()
        if gpu_client is None:
            use_gpu = False
    
    # Load the dataset
    logger.info("Loading cleaned dataset...")
    t0 = time.time()
    try:
        backend = "cudf" if use_gpu else "pandas"
        input_dataset = DocumentDataset.read_json(cleaned_dataset_path, backend=backend)
        load_time = time.time() - t0
        logger.info(f"Dataset loaded in {load_time:.2f} seconds")
        
        doc_count = len(input_dataset.df)
        logger.info(f"Total documents to deduplicate: {doc_count:,}")
        
    except Exception as e:
        logger.error(f"Failed to load dataset: {str(e)}")
        return False
    
    # Perform exact deduplication
    logger.info("Starting exact deduplication...")
    t0 = time.time()
    try:
        # Create exact duplicates detector
        exact_dups = ExactDuplicates(
            logger=log_dir,
            id_field="id",
            text_field="raw_content",
            hash_method=hash_method,
            cache_dir=output_dir,
        )
        
        # Find duplicates
        duplicates = exact_dups(dataset=input_dataset)
        
        dedup_time = time.time() - t0
        logger.info(f"Exact deduplication completed in {dedup_time:.2f} seconds")
        
        # Analyze results
        dup_count = len(duplicates)
        logger.info(f"Found {dup_count:,} duplicate documents")
        
        if dup_count > 0:
            # Analyze duplicate clusters
            duplicates_df = duplicates.df
            cluster_sizes = duplicates_df.groupby("_hashes").agg({"id": "count"}).rename(columns={"id": "count"})
            largest_clusters = cluster_sizes.sort_values("count", ascending=False).head(10)
            
            logger.info("Top 10 largest duplicate clusters:")
            for hash_val, row in largest_clusters.iterrows():
                logger.info(f"  Hash {hash_val}: {row['count']} duplicates")
        
        # Remove duplicates
        logger.info("Removing duplicate documents...")
        t0 = time.time()
        
        # Load duplicates and determine which ones to remove
        duplicates_df = duplicates.df
        docs_to_remove = duplicates_df.map_partitions(
            lambda x: x[x._hashes.duplicated(keep="first")],
        )
        
        # Remove duplicates from original dataset
        result = input_dataset.df[
            ~input_dataset.df["id"].isin(docs_to_remove["id"].compute())
        ]
        
        # Write deduplicated dataset
        from nemo_curator.utils.distributed_utils import write_to_disk
        dedup_output_dir = expand_outdir_and_mkdir(os.path.join(base_dir, "rpv2-2023-06-exact-dup-removed"))
        
        write_to_disk(
            result,
            dedup_output_dir,
            write_to_filename=True,
            output_type="jsonl",
        )
        
        removal_time = time.time() - t0
        logger.info(f"Duplicate removal completed in {removal_time:.2f} seconds")
        
        # Final statistics
        final_count = len(result)
        removed_count = doc_count - final_count
        removal_percentage = (removed_count / doc_count) * 100
        
        logger.info(f"Original documents: {doc_count:,}")
        logger.info(f"Removed documents: {removed_count:,}")
        logger.info(f"Remaining documents: {final_count:,}")
        logger.info(f"Removal percentage: {removal_percentage:.2f}%")
        
        # Count output files
        output_files = [f for f in os.listdir(dedup_output_dir) if f.endswith('.jsonl')]
        logger.info(f"Created {len(output_files)} deduplicated data files")
        
        # Save metadata
        metadata = {
            'step': 'exact_deduplication',
            'input_dir': cleaned_dataset_path,
            'output_dir': dedup_output_dir,
            'duplicates_dir': output_dir,
            'hash_method': hash_method,
            'use_gpu': use_gpu,
            'input_document_count': doc_count,
            'duplicate_count': dup_count,
            'removed_count': removed_count,
            'final_document_count': final_count,
            'removal_percentage': removal_percentage,
            'output_file_count': len(output_files),
            'deduplication_time': dedup_time,
            'removal_time': removal_time,
            'total_time': dedup_time + removal_time
        }
        
        metadata_path = os.path.join(dedup_output_dir, 'exact_deduplication_metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Metadata saved to: {metadata_path}")
        logger.info("Step 5 completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Exact deduplication failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    
    finally:
        # Clean up GPU client
        if gpu_client:
            gpu_client.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Perform exact deduplication on cleaned dataset')
    parser.add_argument('--base-dir', type=str, default='/processing',
                        help='Base directory containing cleaned data')
    parser.add_argument('--language', type=str, default='en',
                        help='Language code to process (default: en)')
    parser.add_argument('--hash-method', type=str, default='md5',
                        choices=['md5', 'sha256'],
                        help='Hash method to use for deduplication')
    parser.add_argument('--no-gpu', action='store_true',
                        help='Disable GPU acceleration')
    
    args = parser.parse_args()
    
    success = exact_deduplication(
        base_dir=args.base_dir,
        language=args.language,
        hash_method=args.hash_method,
        use_gpu=not args.no_gpu
    )
    
    if success:
        print("✅ Step 5 completed successfully!")
        print(f"Next step: python step6_fuzzy_deduplication.py --base-dir {args.base_dir}")
    else:
        print("❌ Step 5 failed!")
        exit(1)