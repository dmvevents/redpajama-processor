# data_loader.py
import os
import glob
from nemo_curator.datasets import DocumentDataset
from nemo_curator.utils.file_utils import expand_outdir_and_mkdir

# Your data paths
base_data_dir = "/data/rpv2-processed"
base_output_dir = "/output"  # Adjust to your output location

def load_rpv2_data(data_dir):
    """Load data from your year-based directory structure"""
    all_files = []
    
    # Find all jsonl files in year subdirectories
    pattern = os.path.join(data_dir, "*/*.jsonl")
    files = glob.glob(pattern)
    
    print(f"Found {len(files)} files:")
    for f in sorted(files)[:10]:  # Show first 10
        print(f)
    if len(files) > 10:
        print("...")
    
    return files

# Load your data
input_files = load_rpv2_data(base_data_dir)
input_dataset = DocumentDataset.read_json(input_files, add_filename=True)
print(f"Total documents: {len(input_dataset.df)}")