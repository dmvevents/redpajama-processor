#!/usr/bin/env python3
# step4_text_cleaning.py
import os
import time
import logging
import json
import nemo_curator
from nemo_curator.datasets import DocumentDataset
from nemo_curator.modifiers import UnicodeReformatter
from nemo_curator.utils.file_utils import expand_outdir_and_mkdir

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('step4_text_cleaning.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def clean_text_data(base_dir, language="EN", text_field="raw_content"):
    """Clean text data using Unicode reformatter"""
    
    logger.info("="*60)
    logger.info("STEP 4: TEXT CLEANING")
    logger.info("="*60)
    
    # Define directories
    en_dataset_path = os.path.join(base_dir, "rpv2-2023-06-language", "data", language)
    output_clean_dir = expand_outdir_and_mkdir(os.path.join(base_dir, f"rpv2-2023-06-{language.lower()}-cleaned"))
    
    logger.info(f"Input directory: {en_dataset_path}")
    logger.info(f"Output directory: {output_clean_dir}")
    logger.info(f"Text field: {text_field}")
    logger.info(f"Language: {language}")
    
    # Check if input directory exists
    if not os.path.exists(en_dataset_path):
        logger.error(f"Input directory does not exist: {en_dataset_path}")
        return False
    
    # Load the dataset
    logger.info("Loading language-separated dataset...")
    t0 = time.time()
    try:
        en_dataset = DocumentDataset.read_json(en_dataset_path, add_filename=True)
        load_time = time.time() - t0
        logger.info(f"Dataset loaded in {load_time:.2f} seconds")
        
        doc_count = len(en_dataset.df)
        logger.info(f"Total documents to clean: {doc_count:,}")
        
        # Check if text field exists
        if text_field not in en_dataset.df.columns:
            logger.error(f"Text field '{text_field}' not found in dataset")
            available_fields = list(en_dataset.df.columns)
            logger.error(f"Available fields: {available_fields}")
            return False
        
    except Exception as e:
        logger.error(f"Failed to load dataset: {str(e)}")
        return False
    
    # Clean the text
    logger.info("Starting text cleaning...")
    t0 = time.time()
    try:
        # Create the cleaner
        cleaner = nemo_curator.Modify(
            UnicodeReformatter(),
            text_field=text_field,
        )
        
        # Apply cleaning
        cleaned_dataset = cleaner(en_dataset)
        
        # Write to disk
        cleaned_dataset.to_json(output_clean_dir, write_to_filename=True)
        
        cleaning_time = time.time() - t0
        logger.info(f"Text cleaning completed in {cleaning_time:.2f} seconds")
        
        # Verify the results
        final_count = len(cleaned_dataset.df)
        logger.info(f"Final document count: {final_count:,}")
        
        # Check for any data loss
        if final_count != doc_count:
            logger.warning(f"Document count changed during cleaning: {doc_count} -> {final_count}")
        
        # Show sample of cleaned text
        sample = cleaned_dataset.df.head(2)
        logger.info("Sample cleaned documents:")
        for i, row in enumerate(sample.iterrows()):
            text_sample = str(row[1].get(text_field, ""))[:200] + "..."
            logger.info(f"  Sample {i+1}: {text_sample}")
        
        # Count output files
        output_files = [f for f in os.listdir(output_clean_dir) if f.endswith('.jsonl')]
        logger.info(f"Created {len(output_files)} cleaned data files")
        
        # Save metadata
        metadata = {
            'step': 'text_cleaning',
            'input_dir': en_dataset_path,
            'output_dir': output_clean_dir,
            'language': language,
            'text_field': text_field,
            'input_document_count': doc_count,
            'output_document_count': final_count,
            'output_file_count': len(output_files),
            'processing_time': cleaning_time
        }
        
        metadata_path = os.path.join(output_clean_dir, 'text_cleaning_metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Metadata saved to: {metadata_path}")
        logger.info("Step 4 completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Text cleaning failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Clean text data using Unicode reformatter')
    parser.add_argument('--base-dir', type=str, default='/processing',
                        help='Base directory containing language-separated data')
    parser.add_argument('--language', type=str, default='EN',
                        help='Language code to process (default: EN)')
    parser.add_argument('--text-field', type=str, default='raw_content',
                        help='Field name containing the text to clean')
    
    args = parser.parse_args()
    
    success = clean_text_data(
        base_dir=args.base_dir,
        language=args.language,
        text_field=args.text_field
    )
    
    if success:
        print("✅ Step 4 completed successfully!")
        print(f"Next step: python step5_exact_deduplication.py --base-dir {args.base_dir}")
    else:
        print("❌ Step 4 failed!")
        exit(1)