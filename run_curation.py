# run_curation.py
from nemo_curator_p5_pipeline import setup_dask_cluster, load_rpv2_data
from nemo_curator.datasets import DocumentDataset

# Setup
gpu_client, gpu_cluster = setup_dask_cluster()

# Load your data
input_files = load_rpv2_data("/data/rpv2-processed")
print(f"Found {len(input_files)} files")

# Load as dataset
dataset = DocumentDataset.read_json(input_files, add_filename=True)
print(f"Total documents: {len(dataset.df)}")

# Show sample
print("\nSample data:")
print(dataset.df.head(2))

# Now you can run individual steps or the full pipeline