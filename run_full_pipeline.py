#!/usr/bin/env python3
# run_full_pipeline.py
import os
import sys
import time
import logging
import subprocess
import argparse

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('full_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_step(script_name, args_dict):
    """Run a single pipeline step"""
    logger.info(f"Starting {script_name}...")
    
    # Build command
    cmd = [sys.executable, script_name]
    for key, value in args_dict.items():
        cmd.extend([f"--{key}", str(value)])
    
    logger.info(f"Running: {' '.join(cmd)}")
    
    # Execute
    start_time = time.time()
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        end_time = time.time()
        
        logger.info(f"{script_name} completed successfully in {end_time - start_time:.2f} seconds")
        logger.info(f"Output: {result.stdout}")
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"{script_name} failed with exit code {e.returncode}")
        logger.error(f"Error output: {e.stderr}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Run full RedPajama processing pipeline')
    parser.add_argument('--input-dir', type=str, required=True,
                        help='Input directory with original jsonl files')
    parser.add_argument('--base-dir', type=str, default='/processing',
                        help='Base directory for processing')
    parser.add_argument('--skip-steps', type=str, default='',
                        help='Comma-separated list of steps to skip (1-7)')
    parser.add_argument('--start-from', type=int, default=1,
                        help='Start from step number (1-7)')
    
    args = parser.parse_args()
    
    # Parse skip steps
    skip_steps = set()
    if args.skip_steps:
        skip_steps = set(int(x.strip()) for x in args.skip_steps.split(','))
    
    # Define pipeline steps
    steps = [
        (1, 'step1_load_and_reshard.py', {'input-dir': args.input_dir, 'base-dir': args.base_dir}),
        (2, 'step2_add_ids.py', {'base-dir': args.base_dir}),
        (3, 'step3_language_separation.py', {'base-dir': args.base_dir}),
        (4, 'step4_text_cleaning.py', {'base-dir': args.base_dir}),
        (5, 'step5_exact_deduplication.py', {'base-dir': args.base_dir}),
        (6, 'step6_fuzzy_deduplication.py', {'base-dir': args.base_dir}),
        (7, 'step7_quality_filtering.py', {'base-dir': args.base_dir}),
    ]
    
    # Run pipeline
    logger.info("="*60)
    logger.info("STARTING FULL REDPAJAMA PROCESSING PIPELINE")
    logger.info("="*60)
    logger.info(f"Input directory: {args.input_dir}")
    logger.info(f"Base directory: {args.base_dir}")
    logger.info(f"Skip steps: {skip_steps}")
    logger.info(f"Start from step: {args.start_from}")
    
    total_start_time = time.time()
    
    for step_num, script_name, step_args in steps:
        if step_num < args.start_from:
            logger.info(f"Skipping step {step_num} (starting from {args.start_from})")
            continue
            
        if step_num in skip_steps:
            logger.info(f"Skipping step {step_num} (user requested)")
            continue
        
        logger.info(f"\n{'='*60}")
        logger.info(f"STEP {step_num}: {script_name}")
        logger.info(f"{'='*60}")
        
        success = run_step(script_name, step_args)
        
        if not success:
            logger.error(f"Pipeline failed at step {step_num}")
            logger.error("Stopping pipeline execution")
            sys.exit(1)
    
    total_time = time.time() - total_start_time
    logger.info(f"\n{'='*60}")
    logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
    logger.info(f"Total processing time: {total_time:.2f} seconds")
    logger.info(f"Final output: {args.base_dir}/rpv2-2023-06-quality-filtered")
    logger.info(f"{'='*60}")

if __name__ == "__main__":
    main()