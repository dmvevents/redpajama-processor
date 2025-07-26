# dask_setup_p5.py
import os
import warnings
import cudf
import dask.dataframe as dd
import dask_cudf
from dask_cuda import LocalCUDACluster
from dask.distributed import Client, wait
from nemo_curator import get_client
from nemo_curator.utils.distributed_utils import get_num_workers, read_data, write_to_disk

warnings.filterwarnings("ignore")

def setup_dask_cluster():
    """Setup Dask cluster for P5 instance with 8 H100s"""
    # Set CUDA devices
    os.environ['CUDA_VISIBLE_DEVICES'] = '0,1,2,3,4,5,6,7'
    
    # GPU cluster for deduplication and GPU-based operations
    gpu_cluster = LocalCUDACluster(
        n_workers=8,  # One per H100
        threads_per_worker=2,
        memory_limit='60GB',  # Per worker, adjust based on your needs
        device_memory_limit='70GB',  # H100 has 80GB, leave buffer
        dashboard_address=':8787',
        protocol='tcp',
        spill_compression='lz4',
        jit_unspill=True,
    )
    
    gpu_client = Client(gpu_cluster)
    print(f"GPU Cluster Dashboard: {gpu_client.dashboard_link}")
    print(f"GPU Workers: {get_num_workers(gpu_client)}")
    
    return gpu_client, gpu_cluster

# Initialize
gpu_client, gpu_cluster = setup_dask_cluster()