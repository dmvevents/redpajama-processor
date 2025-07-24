#!/usr/bin/env python3
# step7_quality_filtering.py
import os
import time
import logging
import json
import yaml
from nemo_curator.datasets import DocumentDataset
from nemo_curator.utils.config_utils import build_filter_pipeline
from nemo_curator.utils.file_utils import expand_outdir_and_mkdir
from nemo_curator.utils.distributed_utils import get_client, get_num_workers

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('step7_quality_filtering.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def create_default_filter_config(config_path):
    """Create a default heuristic filter configuration"""
    
    # Define a basic set of quality filters
    default_config = {
        'filters': [
            {
                'type': 'WordCountFilter',
                'config': {
                    'min_words': 10,
                    'max_words': 500000
                }
            },
            {
                'type': 'CharacterCountFilter',
                'config': {
                    'min_characters': 50,
                    'max_characters': 2000000
                }
            },
            {
                'type': 'MeanWordLengthFilter',
                'config': {
                    'min_word_length': 2,
                    'max_word_length': 20
                }
            },
            {
                'type': 'AlphaRatioFilter',
                'config': {
                    'min_alpha_ratio': 0.6
                }
            },
            {
                'type': 'SymbolToWordFilter',
                'config': {
                    'max_symbol_word_ratio': 0.1
                }
            },
            {
                'type': 'NumberToWordFilter',
                'config': {
                    'max_number_word_ratio': 0.3
                }
            },
            {
                'type': 'WhitespaceFilter',
                'config': {
                    'max_whitespace_ratio': 0.25
                }
            },
            {
                'type': 'ParenthesesFilter',
                'config': {
                    'max_parentheses_ratio': 0.1
                }
            },
            {
                'type': 'EllipsisFilter',
                'config': {
                    'max_ellipsis_ratio': 0.3
                }
            },
            {
                'type': 'DuplicateLineFilter',
                'config': {
                    'max_duplicate_line_ratio': 0.3
                }
            }
        ]
    }
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    
    # Write config file
    with open(config_path, 'w') as f:
        yaml.dump(default_config, f, default_flow_style=False)
    
    logger.info(f"Created default filter configuration: {config_path}")
    return config_path

def setup_cpu_client():
    """Setup CPU client for processing"""
    try:
        scheduler_address = os.getenv("SCHEDULER_ADDRESS")
        cpu_client = get_client(scheduler_address=scheduler_address)
        num_workers = get_num_workers(cpu_client)
        logger.info(f"CPU client setup successful. Number of workers: {num_workers}")
        return cpu_client, num_workers
    except Exception as e:
        logger.warning(f"CPU client setup failed: {str(e)}")
        logger.info("Using default processing")
        return None, 0

def quality_filtering(base_dir, config_file=None, text_field="raw_content"):
    """Perform quality filtering on deduplicated dataset"""
    
    logger.info("="*60)
    logger.info("STEP 7: QUALITY FILTERING")
    logger.info("="*60)
    
    # Define directories
    input_data_dir = os.path.join(base_dir, "rpv2-2023-06-deduped")
    output_dir = expand_outdir_and_mkdir(os.path.join(base_dir, "rpv2-2023-06-quality-filtered"))
    
    logger.info(f"Input directory: {input_data_dir}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Text field: {text_field}")
    
    # Check if input directory exists
    if not os.path.exists(input_data_dir):
        logger.error(f"Input directory does not exist: {input_data_dir}")
        return False
    
    # Setup filter configuration
    if config_file is None:
        config_dir = expand_outdir_and_mkdir(os.path.join(base_dir, "config"))
        config_file = os.path.join(config_dir, "heuristic_filter_en.yaml")
        
        if not os.path.exists(config_file):
            config_file = create_default_filter_config(config_file)
    
    if not os.path.exists(config_file):
        logger.error(f"Filter configuration file not found: {config_file}")
        return False
    
    logger.info(f"Using filter configuration: {config_file}")
    
    # Setup CPU client (quality filtering is CPU-intensive)
    cpu_client, num_workers = setup_cpu_client()
    
    # Load the dataset
    logger.info("Loading deduplicated dataset...")
    t0 = time.time()
    try:
        dataset = DocumentDataset.read_parquet(input_data_dir)
        load_time = time.time() - t0
        logger.info(f"Dataset loaded in {load_time:.2f} seconds")
        
        doc_count = len(dataset.df)
        logger.info(f"Total documents to filter: {doc_count:,}")
        
        # Check if text field exists
        if text_field not in dataset.df.columns:
            logger.error(f"Text field '{text_field}' not found in dataset")
            available_fields = list(dataset.df.columns)
            logger.error(f"Available fields: {available_fields}")
            return False
        
    except Exception as e:
        logger.error(f"Failed to load dataset: {str(e)}")
        return False
    
    # Build filter pipeline
    logger.info("Building filter pipeline...")
    try:
        filter_pipeline = build_filter_pipeline(config_file)
        logger.info("Filter pipeline built successfully")
        
        # Display filters being applied
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
            logger.info("Applying filters:")
            for i, filter_config in enumerate(config.get('filters', []), 1):
                logger.info(f"  {i}. {filter_config['type']}: {filter_config.get('config', {})}")
        
    except Exception as e:
        logger.error(f"Failed to build filter pipeline: {str(e)}")
        return False
    
    # Apply filters
    logger.info("Starting quality filtering...")
    t0 = time.time()
    try:
        # Apply the filter pipeline
        filtered_dataset = filter_pipeline(dataset)
        
        # Write filtered dataset to disk
        filtered_dataset.to_parquet(output_dir)
        
        filtering_time = time.time() - t0
        logger.info(f"Quality filtering completed in {filtering_time:.2f} seconds")
        
        # Analyze results
        final_count = len(filtered_dataset.df)
        removed_count = doc_count - final_count
        removal_percentage = (removed_count / doc_count) * 100
        
        logger.info(f"Original documents: {doc_count:,}")
        logger.info(f"Filtered documents: {final_count:,}")
        logger.info(f"Removed documents: {removed_count:,}")
        logger.info(f"Removal percentage: {removal_percentage:.2f}%")
        
        # Show sample of filtered documents
        sample = filtered_dataset.df.head(2)
        logger.info("Sample filtered documents:")
        for i, row in enumerate(sample.iterrows()):
            text_sample = str(row[1].get(text_field, ""))[:200] + "..."
            logger.info(f"  Sample {i+1}: {text_sample}")
        
        # Count output files
        output_files = []
        for root, dirs, files in os.walk(output_dir):
            for file in files:
                if file.endswith('.parquet'):
                    output_files.append(file)
        
        logger.info(f"Created {len(output_files)} filtered data files")
        
        # Save metadata
        metadata = {
            'step': 'quality_filtering',
            'input_dir': input_data_dir,
            'output_dir': output_dir,
            'config_file': config_file,
            'text_field': text_field,
            'input_document_count': doc_count,
            'filtered_document_count': final_count,
            'removed_count': removed_count,
            'removal_percentage': removal_percentage,
            'output_file_count': len(output_files),
            'processing_time': filtering_time,
            'filters_applied': config.get('filters', []) if 'config' in locals() else []
        }
        
        metadata_path = os.path.join(output_dir, 'quality_filtering_metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Metadata saved to: {metadata_path}")
        logger.info("Step 7 completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Quality filtering failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    
    finally:
        # Clean up CPU client
        if cpu_client:
            cpu_client.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Perform quality filtering on deduplicated dataset')
    parser.add_argument('--base-dir', type=str, default='/processing',
                        help='Base directory containing deduplicated data')
    parser.add_argument('--config-file', type=str, default=None,
                        help='Path to filter configuration file (YAML)')
    parser.add_argument('--text-field', type=str, default='raw_content',
                        help='Field name containing the text to filter')
    
    args = parser.parse_args()
    
    success = quality_filtering(
        base_dir=args.base_dir,
        config_file=args.config_file,
        text_field=args.text_field
    )
    
    if success:
        print("✅ Step 7 completed successfully!")
        print("🎉 Full RedPajama processing pipeline completed!")
        print(f"Final processed data available at: {args.base_dir}/rpv2-2023-06-quality-filtered")
    else:
        print("❌ Step 7 failed!")
        exit(1)