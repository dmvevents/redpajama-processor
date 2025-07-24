#!/usr/bin/env python3
# step3_language_separation.py
import os
import time
import logging
import json
import subprocess
from nemo_curator import ScoreFilter
from nemo_curator.filters import FastTextLangId
from nemo_curator.datasets import DocumentDataset
from nemo_curator.utils.file_utils import expand_outdir_and_mkdir, separate_by_metadata

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('step3_language_separation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def download_fasttext_model(model_path):
    """Download FastText language identification model"""
    model_file = os.path.join(model_path, "lid.176.bin")
    
    if os.path.exists(model_file):
        logger.info(f"FastText model already exists: {model_file}")
        return model_file
    
    logger.info("Downloading FastText language identification model...")
    try:
        subprocess.run([
            "wget", 
            "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin",
            "-P", model_path
        ], check=True)
        logger.info(f"Model downloaded successfully to: {model_file}")
        return model_file
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to download FastText model: {str(e)}")
        return None

def separate_by_language(base_dir, text_field="raw_content"):
    """Identify and separate documents by language"""
    
    logger.info("="*60)
    logger.info("STEP 3: LANGUAGE IDENTIFICATION AND SEPARATION")
    logger.info("="*60)
    
    # Define directories
    id_data_dir = os.path.join(base_dir, "rpv2-2023-06-id")
    language_output_path = expand_outdir_and_mkdir(os.path.join(base_dir, "rpv2-2023-06-language"))
    language_data_output_path = expand_outdir_and_mkdir(os.path.join(language_output_path, "data"))
    
    logger.info(f"Input directory: {id_data_dir}")
    logger.info(f"Output directory: {language_output_path}")
    logger.info(f"Text field: {text_field}")
    
    # Check if input directory exists
    if not os.path.exists(id_data_dir):
        logger.error(f"Input directory does not exist: {id_data_dir}")
        return False
    
    # Download FastText model
    model_file = download_fasttext_model(language_output_path)
    if not model_file:
        return False
    
    # Load dataset
    logger.info("Loading dataset with IDs...")
    t0 = time.time()
    try:
        input_dataset = DocumentDataset.read_json(id_data_dir, add_filename=True)
        load_time = time.time() - t0
        logger.info(f"Dataset loaded in {load_time:.2f} seconds")
        
        doc_count = len(input_dataset.df)
        logger.info(f"Total documents to process: {doc_count:,}")
        
    except Exception as e:
        logger.error(f"Failed to load dataset: {str(e)}")
        return False
    
    # Perform language identification and separation
    logger.info("Starting language identification...")
    t0 = time.time()
    try:
        # Define language field
        language_field = "language"
        
        # Create language filter
        lang_filter = FastTextLangId(model_file)
        language_id_pipeline = ScoreFilter(
            lang_filter,
            score_field=language_field,
            text_field=text_field,
            score_type="object",
        )
        
        # Apply language identification
        filtered_dataset = language_id_pipeline(input_dataset)
        
        # Extract language code from the score object
        filtered_dataset.df[language_field] = filtered_dataset.df[language_field].apply(
            lambda score: score[1] if isinstance(score, tuple) else score,
            meta=(language_field, "object"),
        )
        
        # Split dataset by language
        logger.info("Separating documents by language...")
        language_stats = separate_by_metadata(
            filtered_dataset.df,
            language_data_output_path,
            metadata_field=language_field,
        ).compute()
        
        processing_time = time.time() - t0
        logger.info(f"Language separation completed in {processing_time:.2f} seconds")
        
        # Log language statistics
        logger.info("Language distribution:")
        total_docs = sum(language_stats.values())
        for lang, count in sorted(language_stats.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total_docs) * 100
            logger.info(f"  {lang}: {count:,} documents ({percentage:.2f}%)")
        
        # Check English dataset specifically
        en_dataset_path = os.path.join(language_data_output_path, "EN")
        if os.path.exists(en_dataset_path):
            en_dataset = DocumentDataset.read_json(en_dataset_path, add_filename=True)
            en_count = len(en_dataset)
            logger.info(f"English dataset: {en_count:,} documents")
        else:
            logger.warning("No English documents found!")
        
        # Save metadata
        metadata = {
            'step': 'language_separation',
            'input_dir': id_data_dir,
            'output_dir': language_output_path,
            'total_documents': total_docs,
            'language_statistics': dict(language_stats),
            'text_field': text_field,
            'model_used': model_file,
            'processing_time': processing_time
        }
        
        metadata_path = os.path.join(language_output_path, 'language_separation_metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Metadata saved to: {metadata_path}")
        logger.info("Step 3 completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Language separation failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Identify and separate documents by language')
    parser.add_argument('--base-dir', type=str, default='/processing',
                        help='Base directory containing data with IDs')
    parser.add_argument('--text-field', type=str, default='raw_content',
                        help='Field name containing the text to analyze')
    
    args = parser.parse_args()
    
    success = separate_by_language(
        base_dir=args.base_dir,
        text_field=args.text_field
    )
    
    if success:
        print("✅ Step 3 completed successfully!")
        print(f"Next step: python step4_text_cleaning.py --base-dir {args.base_dir}")
    else:
        print("❌ Step 3 failed!")
        exit(1)