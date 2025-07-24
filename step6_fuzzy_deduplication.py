#!/usr/bin/env python3
# step6_fuzzy_deduplication.py
import os
import time
import logging
import json
import dask_cudf
from nemo_curator.datasets import DocumentDataset
from nemo_curator import MinHash, LSH, BucketsToEdges, ConnectedComponents
from nemo_curator.utils.file_utils import expand_outdir_and_mkdir, get_all_files_paths_under
from nemo_curator.utils.distributed_utils import get_client, get_num_workers, read_data, write_to_disk
from dask.distributed import wait

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('step6_fuzzy_deduplication.log'),
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

def compute_minhashes(base_dir, use_gpu=True):
    """Step 1: Compute MinHash signatures"""
    logger.info("Starting MinHash computation...")
    
    # Configuration
    seed = 42
    minhash_length = 260
    char_ngram = 24
    id_field = "id"
    text_field = "raw_content"
    
    # Directories
    input_data_dir = os.path.join(base_dir, "rpv2-2023-06-en-cleaned")
    log_dir = expand_outdir_and_mkdir(os.path.join(base_dir, "logs"))
    minhash_output_dir = expand_outdir_and_mkdir(os.path.join(base_dir, "rpv2-2023-06-minhash"))
    
    logger.info(f"Input: {input_data_dir}")
    logger.info(f"Output: {minhash_output_dir}")
    
    # Load data
    files = get_all_files_paths_under(
        root=input_data_dir,
        recurse_subdirectories=False,
        keep_extensions="jsonl",
    )
    
    backend = "cudf" if use_gpu else "pandas"
    df = read_data(
        files,
        file_type="jsonl",
        backend=backend,
        files_per_partition=1,
        add_filename=False,
    )[[id_field, text_field]]
    
    logger.info(f"Loaded {len(files)} files")
    
    # Compute MinHash
    t0 = time.time()
    minhasher = MinHash(
        seed=seed,
        num_hashes=minhash_length,
        char_ngrams=char_ngram,
        use_64bit_hash=False,
        logger=log_dir,
        id_field=id_field,
        text_field=text_field,
        cache_dir=minhash_output_dir,
    )
    
    result = minhasher(DocumentDataset(df)).df
    minhash_time = time.time() - t0
    logger.info(f"MinHash computation took {minhash_time:.2f} seconds")
    
    return minhash_output_dir, minhash_time

def compute_lsh(base_dir, minhash_dir, use_gpu=True):
    """Step 2: Locality-Sensitive Hashing"""
    logger.info("Starting LSH computation...")
    
    # Configuration
    id_field = "id"
    num_bands = 20
    buckets_per_shuffle = 1
    minhash_field = "_minhash_signature"
    minhash_length = 260
    
    # Directories
    log_dir = os.path.join(base_dir, "logs")
    output_bucket_dir = expand_outdir_and_mkdir(os.path.join(base_dir, "fuzzy-dedup-output-2023-06"))
    
    logger.info(f"Input: {minhash_dir}")
    logger.info(f"Output: {output_bucket_dir}")
    
    # Load MinHash output
    backend = "cudf" if use_gpu else "pandas"
    if use_gpu:
        df = dask_cudf.read_parquet(minhash_dir, blocksize="2GB", aggregate_files=True)
    else:
        import dask.dataframe as dd
        df = dd.read_parquet(minhash_dir, blocksize="2GB")
    
    # Compute LSH
    t0 = time.time()
    lsh = LSH(
        cache_dir=output_bucket_dir,
        num_hashes=minhash_length,
        num_buckets=num_bands,
        buckets_per_shuffle=buckets_per_shuffle,
        id_fields=id_field,
        minhash_field=minhash_field,
        logger=log_dir,
    )
    
    lsh_result = lsh(DocumentDataset(df))
    lsh_time = time.time() - t0
    logger.info(f"LSH computation took {lsh_time:.2f} seconds")
    
    return output_bucket_dir, lsh_time

def compute_buckets_to_edges(base_dir, cache_dir, use_gpu=True):
    """Step 3: Convert buckets to edges"""
    logger.info("Starting buckets to edges conversion...")
    
    # Configuration
    id_field = "id"
    input_bucket_field = "_bucket_id"
    log_dir = os.path.join(base_dir, "logs")
    
    # Input
    input_bucket_path = os.path.join(cache_dir, "_buckets.parquet")
    
    logger.info(f"Input: {input_bucket_path}")
    
    # Load buckets
    backend = "cudf" if use_gpu else "pandas"
    ddf_bk = DocumentDataset.read_parquet(input_bucket_path, backend=backend)
    
    # Convert buckets to edges
    t0 = time.time()
    buckets_to_edges = BucketsToEdges(
        cache_dir=cache_dir,
        id_fields=id_field,
        bucket_field=input_bucket_field,
        logger=log_dir,
    )
    
    edgelist_df = buckets_to_edges(ddf_bk)
    buckets_time = time.time() - t0
    logger.info(f"Buckets to edges took {buckets_time:.2f} seconds")
    
    return buckets_time

def compute_connected_components(base_dir, cache_dir):
    """Step 4: Find connected components"""
    logger.info("Starting connected components computation...")
    
    # Configuration
    id_field = "id"
    cc_cache_dir = expand_outdir_and_mkdir(os.path.join(cache_dir, "cc-cache"))
    edgelist_path = os.path.join(cache_dir, "jaccard_similarity_results.parquet")
    output_path = expand_outdir_and_mkdir(os.path.join(cache_dir, "connected_components.parquet"))
    
    logger.info(f"Input: {edgelist_path}")
    logger.info(f"Output: {output_path}")
    
    # Compute connected components
    t0 = time.time()
    components_stage = ConnectedComponents(
        cache_dir=cc_cache_dir,
        jaccard_pairs_path=edgelist_path,
        id_column=id_field,
    )
    
    components_stage.cc_workflow(output_path=output_path)
    cc_time = time.time() - t0
    logger.info(f"Connected components took {cc_time:.2f} seconds")
    
    return output_path, cc_time

def remove_fuzzy_duplicates(base_dir, cc_output_path, use_gpu=True):
    """Step 5: Remove fuzzy duplicates"""
    logger.info("Starting fuzzy duplicate removal...")
    
    # Load connected components results
    if use_gpu:
        cc_result = dask_cudf.read_parquet(cc_output_path, split_row_groups=False).repartition(npartitions=1)
    else:
        import dask.dataframe as dd
        cc_result = dd.read_parquet(cc_output_path).repartition(npartitions=1)
    
    # Set 'group' as index and shuffle
    cc_result = cc_result.set_index("group", shuffle="tasks")
    
    # Find duplicates to remove
    def assign_cumcount(df):
        df["cumcount"] = df.groupby(level=0).cumcount()
        df = df[df["cumcount"] >= 1]
        return df.drop(columns=["cumcount"])
    
    docs_to_remove = cc_result.map_partitions(assign_cumcount, meta=cc_result)
    docs_to_remove = docs_to_remove.reset_index()
    docs_to_remove = docs_to_remove[["id"]]
    docs_to_remove = docs_to_remove.rename(columns={"id": "to_remove_doc_id"})
    docs_to_remove = docs_to_remove.reset_index(drop=True).persist()
    _ = wait(docs_to_remove)
    del _
    
    dup_count = len(docs_to_remove)
    logger.info(f"Found {dup_count:,} fuzzy duplicates to remove")
    
    # Load original dataset
    input_data_dir = os.path.join(base_dir, "rpv2-2023-06-en-cleaned")
    backend = "cudf" if use_gpu else "pandas"
    input_dataset = DocumentDataset.read_json(input_data_dir, backend=backend)
    input_df = input_dataset.df[["raw_content", "id"]]
    
    original_count = len(input_df)
    logger.info(f"Original dataset: {original_count:,} documents")
    
    # Remove duplicates
    t0 = time.time()
    deduped_df = input_df.merge(docs_to_remove, left_on=["id"], right_on=["to_remove_doc_id"], how="left")
    deduped_df = deduped_df[deduped_df["to_remove_doc_id"].isna()].drop(columns=["to_remove_doc_id"]).reset_index(drop=True)
    
    # Write deduplicated dataset
    dedup_output_dir = expand_outdir_and_mkdir(os.path.join(base_dir, "rpv2-2023-06-deduped"))
    deduped_df.to_parquet(dedup_output_dir)
    
    removal_time = time.time() - t0
    logger.info(f"Duplicate removal took {removal_time:.2f} seconds")
    
    final_count = len(deduped_df)
    removed_count = original_count - final_count
    removal_percentage = (removed_count / original_count) * 100
    
    logger.info(f"Removed {removed_count:,} documents ({removal_percentage:.2f}%)")
    logger.info(f"Final dataset: {final_count:,} documents")
    
    return dedup_output_dir, original_count, final_count, removed_count, removal_time

def fuzzy_deduplication(base_dir, use_gpu=True):
    """Perform complete fuzzy deduplication pipeline"""
    
    logger.info("="*60)
    logger.info("STEP 6: FUZZY DEDUPLICATION")
    logger.info("="*60)
    
    # Setup GPU client if requested
    gpu_client = None
    if use_gpu:
        gpu_client, num_workers = setup_gpu_client()
        if gpu_client is None:
            use_gpu = False
    
    total_start_time = time.time()
    
    try:
        # Step 1: Compute MinHash signatures
        minhash_dir, minhash_time = compute_minhashes(base_dir, use_gpu)
        
        # Step 2: Locality-Sensitive Hashing
        cache_dir, lsh_time = compute_lsh(base_dir, minhash_dir, use_gpu)
        
        # Step 3: Buckets to Edges
        buckets_time = compute_buckets_to_edges(base_dir, cache_dir, use_gpu)
        
        # Step 4: Connected Components
        cc_output_path, cc_time = compute_connected_components(base_dir, cache_dir)
        
        # Step 5: Remove Duplicates
        dedup_output_dir, original_count, final_count, removed_count, removal_time = remove_fuzzy_duplicates(
            base_dir, cc_output_path, use_gpu
        )
        
        total_time = time.time() - total_start_time
        
        # Save metadata
        metadata = {
            'step': 'fuzzy_deduplication',
            'input_dir': os.path.join(base_dir, "rpv2-2023-06-en-cleaned"),
            'output_dir': dedup_output_dir,
            'use_gpu': use_gpu,
            'original_document_count': original_count,
            'removed_count': removed_count,
            'final_document_count': final_count,
            'removal_percentage': (removed_count / original_count) * 100,
            'processing_times': {
                'minhash': minhash_time,
                'lsh': lsh_time,
                'buckets_to_edges': buckets_time,
                'connected_components': cc_time,
                'duplicate_removal': removal_time,
                'total': total_time
            }
        }
        
        metadata_path = os.path.join(dedup_output_dir, 'fuzzy_deduplication_metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Metadata saved to: {metadata_path}")
        logger.info(f"Total fuzzy deduplication time: {total_time:.2f} seconds")
        logger.info("Step 6 completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Fuzzy deduplication failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    
    finally:
        # Clean up GPU client
        if gpu_client:
            gpu_client.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Perform fuzzy deduplication on cleaned dataset')
    parser.add_argument('--base-dir', type=str, default='/processing',
                        help='Base directory containing cleaned data')
    parser.add_argument('--no-gpu', action='store_true',
                        help='Disable GPU acceleration')
    
    args = parser.parse_args()
    
    success = fuzzy_deduplication(
        base_dir=args.base_dir,
        use_gpu=not args.no_gpu
    )
    
    if success:
        print("✅ Step 6 completed successfully!")
        print(f"Next step: python step7_quality_filtering.py --base-dir {args.base_dir}")
    else:
        print("❌ Step 6 failed!")
        exit(1)