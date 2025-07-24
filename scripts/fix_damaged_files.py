#!/usr/bin/env python3
import re
import os
import gzip
import requests
import time
import random
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/data/redpajama/download_recovery.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

def extract_errors(log_content):
    """Extract failed files and categorize errors from log content."""
    failed_files = []
    error_counts = defaultdict(int)
    
    for line in log_content.splitlines():
        if "WARNING - Failed to process" in line:
            match = re.search(r'Failed to process ([^:]+): (.+)', line)
            if match:
                file_path = match.group(1)
                error_msg = match.group(2)
                failed_files.append({
                    'timestamp': line.split(' - ')[0],
                    'file_path': file_path,
                    'error': error_msg
                })
                
                # Count error types
                if "Compressed file ended before the end-of-stream marker was reached" in error_msg:
                    error_counts['compression_error'] += 1
                elif "JSONDecodeError" in error_msg:
                    error_counts['json_error'] += 1
                elif "Not a gzipped file" in error_msg:
                    error_counts['not_gzipped'] += 1
                elif "invalid code lengths set" in error_msg:
                    error_counts['invalid_code_lengths'] += 1
                elif "invalid block type" in error_msg:
                    error_counts['invalid_block_type'] += 1
                elif "invalid stored block lengths" in error_msg:
                    error_counts['invalid_stored_block_lengths'] += 1
                elif "invalid distance code" in error_msg:
                    error_counts['invalid_distance_code'] += 1
                elif "CRC check failed" in error_msg:
                    error_counts['crc_check_failed'] += 1
                else:
                    error_counts['other_error'] += 1
    
    return failed_files, error_counts

def is_valid_gzip(file_path):
    """Check if a file is a valid gzip file."""
    try:
        with gzip.open(file_path, 'rb') as f:
            # Read a small amount to verify it's a valid gzip
            f.read(1)
        return True
    except:
        return False

def download_file(file_info, base_url, max_retries=5, initial_backoff=1):
    """Download a file with exponential backoff retry."""
    file_path = file_info['file_path']
    
    # Extract the relative path (everything after /data/redpajama/)
    rel_path = file_path.replace('/data/redpajama/', '')
    url = f"{base_url}/{rel_path}"
    dest = file_path
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    
    # Remove existing damaged file if it exists
    if os.path.exists(dest):
        os.remove(dest)
    
    # Add some jitter to avoid synchronized retries
    time.sleep(random.uniform(0.1, 0.5))
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Downloading {rel_path} (attempt {attempt}/{max_retries})")
            
            # Download the file
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            # Save to file
            with open(dest, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Verify the downloaded file
            if is_valid_gzip(dest):
                logger.info(f"Successfully downloaded and verified: {rel_path}")
                return True
            else:
                logger.error(f"Downloaded file is corrupted: {rel_path}")
                os.remove(dest)
                # If corrupted, wait and retry
                backoff_time = initial_backoff * (2 ** (attempt - 1))
                time.sleep(backoff_time)
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Too Many Requests
                logger.warning(f"Rate limited while downloading {rel_path} (attempt {attempt})")
                # Calculate backoff time with exponential increase
                backoff_time = initial_backoff * (2 ** (attempt - 1))
                # Add jitter to avoid thundering herd problem
                backoff_time = backoff_time * (0.5 + random.random())
                logger.info(f"Backing off for {backoff_time:.1f} seconds")
                time.sleep(backoff_time)
            else:
                logger.error(f"HTTP error downloading {rel_path}: {str(e)}")
                if os.path.exists(dest):
                    os.remove(dest)
                return False
                
        except Exception as e:
            logger.error(f"Failed to download {rel_path}: {str(e)}")
            if os.path.exists(dest):
                os.remove(dest)
            # For general errors, retry with backoff
            backoff_time = initial_backoff * (2 ** (attempt - 1))
            time.sleep(backoff_time)
    
    logger.error(f"Failed to download {rel_path} after {max_retries} attempts")
    return False

def main(log_content):
    # Configuration
    base_url = "https://data.together.xyz/redpajama-data-v2/v1.0.0"
    
    # Extract failed files from the log content
    failed_files, error_counts = extract_errors(log_content)
    
    logger.info(f"Found {len(failed_files)} failed files")
    logger.info("\nError type breakdown:")
    for error_type, count in sorted(error_counts.items()):
        logger.info(f"  {error_type}: {count}")
    
    # Write detailed list to file
    with open('/data/redpajama/failed_files_detailed.txt', 'w') as f:
        f.write("FAILED FILES REPORT\n")
        f.write("==================\n\n")
        
        for item in failed_files:
            f.write(f"Timestamp: {item['timestamp']}\n")
            f.write(f"File: {item['file_path']}\n")
            f.write(f"Error: {item['error']}\n")
            f.write("-" * 80 + "\n")
    
    # Ask for confirmation before proceeding with downloads
    logger.info(f"\nReady to download and replace {len(failed_files)} damaged files.")
    confirm = input("Proceed with downloads? (y/n): ")
    
    if confirm.lower() != 'y':
        logger.info("Download cancelled.")
        return
    
    # Use batch processing instead of ThreadPoolExecutor to better control rate limiting
    max_workers = 3  # Significantly reduced from 10
    batch_size = 10
    successful = 0
    failed = 0
    
    # Process files in smaller batches
    for i in range(0, len(failed_files), batch_size):
        batch = failed_files[i:i + batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}/{(len(failed_files) + batch_size - 1)//batch_size}")
        
        # Process batch with limited parallelism
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(
                lambda file_info: download_file(file_info, base_url), 
                batch
            ))
        
        # Update counts
        for result in results:
            if result:
                successful += 1
            else:
                failed += 1
                
        # Add delay between batches to reduce rate limiting
        if i + batch_size < len(failed_files):
            delay = random.uniform(1.5, 3.0)
            logger.info(f"Waiting {delay:.1f}s before next batch...")
            time.sleep(delay)
    
    logger.info("\nDownload Summary:")
    logger.info(f"  Successfully downloaded and verified: {successful}")
    logger.info(f"  Failed downloads: {failed}")
    logger.info(f"  Total processed: {len(failed_files)}")
    
    # If there are still failed downloads, offer to retry them with even more conservative settings
    if failed > 0:
        retry = input("\nThere were failed downloads. Retry with more conservative settings? (y/n): ")
        if retry.lower() == 'y':
            # Get list of files that failed
            retry_files = []
            for file_info in failed_files:
                file_path = file_info['file_path']
                if not os.path.exists(file_path) or not is_valid_gzip(file_path):
                    retry_files.append(file_info)
            
            logger.info(f"Retrying {len(retry_files)} files with more conservative settings...")
            
            # Even more conservative settings for retry
            max_workers = 1
            batch_size = 5
            retry_successful = 0
            retry_failed = 0
            
            # Process retry files
            for i in range(0, len(retry_files), batch_size):
                batch = retry_files[i:i + batch_size]
                logger.info(f"Processing retry batch {i//batch_size + 1}/{(len(retry_files) + batch_size - 1)//batch_size}")
                
                for file_info in batch:
                    result = download_file(file_info, base_url, max_retries=8, initial_backoff=2)
                    if result:
                        retry_successful += 1
                    else:
                        retry_failed += 1
                    
                    # Add significant delay between individual files
                    if i + batch_size < len(retry_files):
                        delay = random.uniform(3.0, 5.0)
                        logger.info(f"Waiting {delay:.1f}s before next file...")
                        time.sleep(delay)
                
                # Add delay between batches
                if i + batch_size < len(retry_files):
                    delay = random.uniform(5.0, 10.0)
                    logger.info(f"Waiting {delay:.1f}s before next batch...")
                    time.sleep(delay)
            
            logger.info("\nRetry Summary:")
            logger.info(f"  Successfully downloaded and verified: {retry_successful}")
            logger.info(f"  Failed downloads: {retry_failed}")
            logger.info(f"  Total retried: {len(retry_files)}")
            
            # Update overall stats
            successful += retry_successful
            failed = retry_failed

if __name__ == "__main__":
    # Get log content from file
    log_file = input("Enter log file path: ")
    
    # Check if log file exists
    if not os.path.exists(log_file):
        print(f"Log file not found: {log_file}")
        log_file = input("Enter log file path again: ")
    
    with open(log_file, 'r') as f:
        log_content = f.read()
    
    main(log_content)