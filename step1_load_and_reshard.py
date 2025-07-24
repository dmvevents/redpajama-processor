#!/usr/bin/env python3
# step1_load_and_reshard.py
import os
import time
import logging
from nemo_curator.datasets import DocumentDataset
from nemo_curator.utils.file_utils import expand_outdir_and_mkdir, reshard_jsonl

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('step1_resharding.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_and_reshard_data(input_dir, base_dir, output_file_size="100M"):
    """Load data and reshard into balanced partitions"""
    
    logger.info("="*60)
    logger.info("STEP 1: LOADING AND RESHARDING DATA")
    logger.info("="*60)
    
    # Define output directory
    output_resharded_dir = expand_outdir_and_mkdir(os.path.join(base_dir, "rpv2-2023-06-resharded"))
    
    logger.info(f"Input directory: {input_dir}")
    logger.info(f"Output directory: {output_resharded_dir}")
    logger.info(f"Target file size: {output_file_size}")
    
    # First, load the dataset to get basic stats
    logger.info("Loading dataset...")
    t0 = time.time()
    try:
        input_dataset = DocumentDataset.read_json(input_dir, add_filename=True)
        load_time = time.time() - t0
        logger.info(f"Dataset loaded in {load_time:.2f} seconds")
        
        doc_count = len(input_dataset.df)
        logger.info(f"Total documents: {doc_count:,}")
        
    except Exception as e:
        logger.error(f"Failed to load dataset: {str(e)}")
        return False
    
    # Reshard the data
    logger.info("Starting data resharding...")
    t0 = time.time()
    try:
        reshard_jsonl(
            input_dir,
            output_resharded_dir,
            output_file_size=output_file_size,
            start_index=0,
            file_prefix="rpv2-2023-06",
        )
        reshard_time = time.time() - t0
        logger.info(f"Data resharding completed in {reshard_time:.2f} seconds")
        
        # Count output files
        output_files = [f for f in os.listdir(output_resharded_dir) if f.endswith('.jsonl')]
        logger.info(f"Created {len(output_files)} balanced partition files")
        
        # Save metadata
        metadata = {
            'step': 'resharding',
            'input_dir': input_dir,
            'output_dir': output_resharded_dir,
            'original_doc_count': doc_count,
            'output_file_count': len(output_files),
            'target_file_size': output_file_size,
            'processing_time': reshard_time
        }
        
        metadata_path = os.path.join(output_resharded_dir, 'resharding_metadata.json')
        import json
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Metadata saved to: {metadata_path}")
        logger.info("Step 1 completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Resharding failed: {str(e)}")
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Load and reshard RedPajama data')
    parser.add_argument('--input-dir', type=str, required=True,
                        help='Input directory with jsonl files')
    parser.add_argument('--base-dir', type=str, default='/processing',
                        help='Base directory for output')
    parser.add_argument('--file-size', type=str, default='100M',
                        help='Target file size for resharding')
    
    args = parser.parse_args()
    
    success = load_and_reshard_data(
        input_dir=args.input_dir,
        base_dir=args.base_dir,
        output_file_size=args.file_size
    )
    
    if success:
        print("✅ Step 1 completed successfully!")
        print(f"Next step: python step2_add_ids.py --base-dir {args.base_dir}")
    else:
        print("❌ Step 1 failed!")
        exit(1)