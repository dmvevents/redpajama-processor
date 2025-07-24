#!/usr/bin/env python3
# step2_add_ids.py
import os
import time
import logging
import json
from nemo_curator import AddId
from nemo_curator.datasets import DocumentDataset
from nemo_curator.utils.file_utils import expand_outdir_and_mkdir

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('step2_add_ids.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def add_document_ids(base_dir, id_prefix="rpv2-2023-06"):
    """Add unique IDs to all documents"""
    
    logger.info("="*60)
    logger.info("STEP 2: ADDING DOCUMENT IDS")
    logger.info("="*60)
    
    # Define input and output directories
    input_data_dir = os.path.join(base_dir, "rpv2-2023-06-resharded")
    id_data_dir = expand_outdir_and_mkdir(os.path.join(base_dir, "rpv2-2023-06-id"))
    
    logger.info(f"Input directory: {input_data_dir}")
    logger.info(f"Output directory: {id_data_dir}")
    logger.info(f"ID prefix: {id_prefix}")
    
    # Check if input directory exists
    if not os.path.exists(input_data_dir):
        logger.error(f"Input directory does not exist: {input_data_dir}")
        return False
    
    # Load the dataset
    logger.info("Loading resharded dataset...")
    t0 = time.time()
    try:
        input_dataset = DocumentDataset.read_json(input_data_dir, add_filename=True)
        load_time = time.time() - t0
        logger.info(f"Dataset loaded in {load_time:.2f} seconds")
        
        doc_count = len(input_dataset.df)
        logger.info(f"Total documents to process: {doc_count:,}")
        
    except Exception as e:
        logger.error(f"Failed to load dataset: {str(e)}")
        return False
    
    # Add IDs
    logger.info("Adding unique IDs to documents...")
    t0 = time.time()
    try:
        # Create AddId instance
        add_id = AddId(
            id_field="id",
            id_prefix=id_prefix,
        )
        
        # Process the dataset
        id_dataset = add_id(input_dataset)
        
        # Write to disk
        id_dataset.to_json(id_data_dir, write_to_filename=True)
        
        id_time = time.time() - t0
        logger.info(f"ID addition completed in {id_time:.2f} seconds")
        
        # Verify the results
        final_count = len(id_dataset.df)
        logger.info(f"Final document count: {final_count:,}")
        
        # Check a few samples to verify IDs were added
        sample = id_dataset.df.head(3)
        logger.info("Sample documents with IDs:")
        for i, row in enumerate(sample.iterrows()):
            logger.info(f"  Sample {i+1}: ID = {row[1].get('id', 'NOT_FOUND')}")
        
        # Save metadata
        metadata = {
            'step': 'add_ids',
            'input_dir': input_data_dir,
            'output_dir': id_data_dir,
            'document_count': final_count,
            'id_prefix': id_prefix,
            'processing_time': id_time
        }
        
        metadata_path = os.path.join(id_data_dir, 'add_ids_metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Metadata saved to: {metadata_path}")
        logger.info("Step 2 completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"ID addition failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Add unique IDs to RedPajama documents')
    parser.add_argument('--base-dir', type=str, default='/processing',
                        help='Base directory containing resharded data')
    parser.add_argument('--id-prefix', type=str, default='rpv2-2023-06',
                        help='Prefix for document IDs')
    
    args = parser.parse_args()
    
    success = add_document_ids(
        base_dir=args.base_dir,
        id_prefix=args.id_prefix
    )
    
    if success:
        print("✅ Step 2 completed successfully!")
        print(f"Next step: python step3_language_separation.py --base-dir {args.base_dir}")
    else:
        print("❌ Step 2 failed!")
        exit(1)