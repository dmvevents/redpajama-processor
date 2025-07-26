# Dockerfile.sagemaker-nemo-curator
ARG CUDA_VER=12.5.1
ARG REPO_URL=https://github.com/NVIDIA/NeMo-Curator.git
ARG CURATOR_COMMIT=main
ARG IMAGE_LABEL=sagemaker-nemo-curator

# Stage 1: Clone NeMo Curator (equivalent to curator-update stage)
FROM 763104351884.dkr.ecr.us-west-2.amazonaws.com/pytorch-training:2.7.1-gpu-py312-cu128-ubuntu22.04-sagemaker-v1.0 AS curator-update

ARG REPO_URL
ARG CURATOR_COMMIT

# Install git if not present and clone repository exactly like the reference
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

RUN bash -exu <<EOF
  mkdir -p /opt/NeMo-Curator
  cd /opt/NeMo-Curator
  git init
  git remote add origin $REPO_URL
  git fetch --all
  git fetch origin '+refs/pull/*/merge:refs/remotes/pull/*/merge'
  git checkout $CURATOR_COMMIT
EOF

# Stage 2: Build dependencies (equivalent to deps stage)
FROM 763104351884.dkr.ecr.us-west-2.amazonaws.com/pytorch-training:2.7.1-gpu-py312-cu128-ubuntu22.04-sagemaker-v1.0 AS deps

LABEL "nemo.library"=${IMAGE_LABEL}
WORKDIR /opt

ARG CUDA_VER

# Install system dependencies needed for conda
RUN apt-get update && apt-get install -y \
    curl wget git build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Miniconda to match the conda approach from reference
RUN wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh && \
    bash /tmp/miniconda.sh -b -p /opt/conda && \
    rm /tmp/miniconda.sh

ENV PATH="/opt/conda/bin:$PATH"

# Create curator environment exactly like the reference
# Create environment with explicit channels only
RUN conda create -y --name curator \
    --override-channels \
    -c nvidia/label/cuda-${CUDA_VER} \
    -c conda-forge \
    python=3.12 \
    cuda-cudart \
    libcufft \
    libcublas \
    libcurand \
    libcusparse \
    libcusolver \
    cuda-nvvm && \
    conda clean -ay

# Activate curator environment and install pip packages
RUN /opt/conda/envs/curator/bin/pip install --upgrade pytest pip pytest-coverage

# Install RAPIDS packages in the curator environment (this was missing!)
RUN /opt/conda/envs/curator/bin/pip install --no-cache-dir --extra-index-url https://pypi.nvidia.com \
    cudf-cu12==24.12.* \
    dask-cudf-cu12==24.12.* \
    cupy-cuda12x \
    rmm-cu12==24.12.*

# Install Dask and distributed computing packages
RUN /opt/conda/envs/curator/bin/pip install --no-cache-dir \
    dask[complete]==2024.11.* \
    dask-cuda \
    distributed

# Copy NeMo Curator source from first stage
COPY --from=curator-update /opt/NeMo-Curator /opt/NeMo-Curator

# Install NeMo Curator exactly like the reference
RUN cd /opt/NeMo-Curator && \
    /opt/conda/envs/curator/bin/pip install --extra-index-url https://pypi.nvidia.com -e ".[all]"

# Stage 3: Final image (equivalent to final stage)
FROM 763104351884.dkr.ecr.us-west-2.amazonaws.com/pytorch-training:2.7.1-gpu-py312-cu128-ubuntu22.04-sagemaker-v1.0 AS final

# Set SageMaker environment variables
ENV PYTHONPATH=/opt/ml/code:$PYTHONPATH
ENV CUDA_VISIBLE_DEVICES=0,1,2,3,4,5,6,7
ENV NCCL_DEBUG=INFO
ENV NCCL_TREE_THRESHOLD=0

# Set PATH to use curator environment (exactly like reference)
ENV PATH=/opt/conda/envs/curator/bin:$PATH

LABEL "nemo.library"=${IMAGE_LABEL}
WORKDIR /opt

# Install system dependencies for SageMaker
RUN apt-get update && apt-get install -y \
    s3fs \
    fuse \
    awscli \
    git \
    curl \
    wget \
    build-essential \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

# Install base conda (needed for the curator environment to work)
RUN wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh && \
    bash /tmp/miniconda.sh -b -p /opt/conda && \
    rm /tmp/miniconda.sh

# Copy the curator environment from deps stage (exactly like reference)
COPY --from=deps /opt/conda/envs/curator /opt/conda/envs/curator

# Install additional packages you need in the curator environment
RUN /opt/conda/envs/curator/bin/pip install --no-cache-dir \
    fsspec[s3] \
    s3fs \
    boto3 \
    pyarrow \
    fastparquet \
    tqdm \
    rich \
    psutil \
    GPUtil

# Set up conda initialization
RUN /opt/conda/bin/conda init bash && \
    echo "conda activate curator" >> ~/.bashrc && \
    echo "conda activate curator" >> /etc/skel/.bashrc

# Create SageMaker directories
RUN mkdir -p /mnt/fsx /mnt/s3-data /opt/ml/processing /opt/ml/input /opt/ml/output /opt/ml/code

# Set working directory for SageMaker
WORKDIR /opt/ml/code

# Copy processing scripts
COPY . /opt/ml/code/

# Set permissions
RUN chmod +x /opt/ml/code/*.sh 2>/dev/null || true

# # Test installations using the curator environment - now includes RAPIDS
# RUN /opt/conda/envs/curator/bin/python -c "import torch; print(f'PyTorch CUDA available: {torch.cuda.is_available()}'); print(f'CUDA devices: {torch.cuda.device_count()}')" && \
#     /opt/conda/envs/curator/bin/python -c "import cudf; print('cuDF imported successfully')" && \
#     /opt/conda/envs/curator/bin/python -c "import dask_cudf; print('Dask-cuDF imported successfully')" && \
#     /opt/conda/envs/curator/bin/python -c "import nemo_curator; print('NeMo Curator imported successfully')"

# Ensure the curator environment is the default
ENV CONDA_DEFAULT_ENV=curator
