# nemo_curator_p5_pipeline.py
import os
import time
import warnings
from nemo_curator import AddId, Modify, ScoreFilter, ExactDuplicates, MinHash, LSH, BucketsToEdges, ConnectedComponents
from nemo_curator.datasets import DocumentDataset
from nemo_curator.modifiers import UnicodeReformatter
from nemo_curator.utils.file_utils import expand_outdir_and_mkdir, reshard_jsonl
from nemo_curator.utils.config_utils import build_filter_pipeline

warnings.filterwarnings("ignore")

class NeMoCuratorP5Pipeline:
    def __init__(self, base_data_dir="/data/rpv2-processed", base_output_dir="/output"):
        self.base_data_dir = base_data_dir
        self.base_output_dir = base_output_dir
        self.gpu_client, self.gpu_cluster = setup_dask_cluster()
        
    def step1_load_and_reshard(self):
        """Load data and reshard for balanced processing"""
        print("Step 1: Loading and resharding data...")
        
        # Load your data
        input_files = load_rpv2_data(self.base_data_dir)
        
        # Create resharded directory
        output_resharded_dir = expand_outdir_and_mkdir(
            os.path.join(self.base_output_dir, "rpv2-resharded")
        )
        
        t0 = time.time()
        
        # Since your files might already be reasonably sized, you can skip resharding
        # or reshard if needed for better balance across 8 workers
        print(f"Data loading took: {time.time() - t0} seconds")
        return input_files
    
    def step2_add_ids(self, input_files):
        """Add unique IDs to documents"""
        print("Step 2: Adding IDs...")
        
        input_dataset = DocumentDataset.read_json(input_files, add_filename=True)
        id_data_dir = expand_outdir_and_mkdir(os.path.join(self.base_output_dir, "rpv2-with-ids"))
        
        t0 = time.time()
        add_id = AddId(
            id_field="id",
            id_prefix="rpv2-processed",
        )
        id_dataset = add_id(input_dataset)
        id_dataset.to_json(id_data_dir, write_to_filename=True)
        print(f"Adding IDs took: {time.time() - t0} seconds")
        
        return id_data_dir
    
    def step3_text_cleaning(self, input_dir):
        """Clean text using Unicode reformatter"""
        print("Step 3: Text cleaning...")
        
        input_dataset = DocumentDataset.read_json(input_dir, add_filename=True)
        output_clean_dir = expand_outdir_and_mkdir(os.path.join(self.base_output_dir, "rpv2-cleaned"))
        
        t0 = time.time()
        cleaner = Modify(
            UnicodeReformatter(),
            text_field="raw_content",  # Adjust field name if different
        )
        
        cleaned_dataset = cleaner(input_dataset)
        cleaned_dataset.to_json(output_clean_dir, write_to_filename=True)
        print(f"Text cleaning took: {time.time() - t0} seconds")
        
        return output_clean_dir
    
    def step4_exact_dedup(self, input_dir):
        """Exact deduplication"""
        print("Step 4: Exact deduplication...")
        
        input_dataset = DocumentDataset.read_json(input_dir, backend="cudf")
        log_dir = expand_outdir_and_mkdir(os.path.join(self.base_output_dir, "logs"))
        cache_dir = expand_outdir_and_mkdir(os.path.join(self.base_output_dir, "exact-dedup-cache"))
        
        t0 = time.time()
        exact_dups = ExactDuplicates(
            logger=log_dir,
            id_field="id",
            text_field="raw_content",
            hash_method="md5",
            cache_dir=cache_dir,
        )
        duplicates = exact_dups(dataset=input_dataset)
        print(f"Exact dedup took: {time.time() - t0} seconds")
        print(f"Found {len(duplicates)} exact duplicates")
        
        # Remove duplicates
        output_dir = expand_outdir_and_mkdir(os.path.join(self.base_output_dir, "rpv2-exact-dedup-removed"))
        duplicates_df = duplicates.df
        
        docs_to_remove = duplicates_df.map_partitions(
            lambda x: x[x._hashes.duplicated(keep="first")]
        )
        
        result = input_dataset.df[
            ~input_dataset.df["id"].isin(docs_to_remove["id"].compute())
        ]
        
        from nemo_curator.utils.distributed_utils import write_to_disk
        write_to_disk(result, output_dir, write_to_filename=True, output_type="jsonl")
        print(f"Removed {len(docs_to_remove)} exact duplicates")
        
        return output_dir
    
    def step5_fuzzy_dedup(self, input_dir):
        """Fuzzy deduplication"""
        print("Step 5: Fuzzy deduplication...")
        
        # Parameters for fuzzy dedup
        seed = 42
        minhash_length = 260
        char_ngram = 24
        num_bands = 20
        
        # Step 5.1: Compute MinHash
        print("  5.1: Computing MinHash...")
        minhash_dir = expand_outdir_and_mkdir(os.path.join(self.base_output_dir, "minhash"))
        log_dir = os.path.join(self.base_output_dir, "logs")
        
        files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.jsonl')]
        df = read_data(files, file_type="jsonl", backend="cudf", files_per_partition=2)
        df = df[["id", "raw_content"]]
        
        t0 = time.time()
        minhasher = MinHash(
            seed=seed,
            num_hashes=minhash_length,
            char_ngrams=char_ngram,
            use_64bit_hash=False,
            logger=log_dir,
            id_field="id",
            text_field="raw_content",
            cache_dir=minhash_dir,
        )
        minhash_result = minhasher(DocumentDataset(df)).df
        print(f"  MinHash took: {time.time() - t0} seconds")
        
        # Step 5.2: LSH
        print("  5.2: Running LSH...")
        lsh_cache_dir = expand_outdir_and_mkdir(os.path.join(self.base_output_dir, "lsh-cache"))
        
        t0 = time.time()
        df_minhash = dask_cudf.read_parquet(minhash_dir, blocksize="2GB", aggregate_files=True)
        
        lsh = LSH(
            cache_dir=lsh_cache_dir,
            num_hashes=minhash_length,
            num_buckets=num_bands,
            buckets_per_shuffle=1,
            id_fields="id",
            minhash_field="_minhash_signature",
            logger=log_dir,
        )
        lsh_result = lsh(DocumentDataset(df_minhash))
        print(f"  LSH took: {time.time() - t0} seconds")
        
        # Step 5.3: Buckets to Edges
        print("  5.3: Converting buckets to edges...")
        buckets_path = os.path.join(lsh_cache_dir, "_buckets.parquet")
        
        t0 = time.time()
        ddf_bk = DocumentDataset.read_parquet(buckets_path, backend="cudf")
        
        buckets_to_edges = BucketsToEdges(
            cache_dir=lsh_cache_dir,
            id_fields="id",
            bucket_field="_bucket_id",
            logger=log_dir,
        )
        edgelist_df = buckets_to_edges(ddf_bk)
        print(f"  Buckets to edges took: {time.time() - t0} seconds")
        
        # Step 5.4: Connected Components
        print("  5.4: Finding connected components...")
        cc_cache_dir = expand_outdir_and_mkdir(os.path.join(self.base_output_dir, "cc-cache"))
        cc_output_path = os.path.join(self.base_output_dir, "connected_components.parquet")
        edgelist_path = os.path.join(lsh_cache_dir, "jaccard_similarity_results.parquet")
        
        t0 = time.time()
        components_stage = ConnectedComponents(
            cache_dir=cc_cache_dir,
            jaccard_pairs_path=edgelist_path,
            id_column="id",
        )
        components_stage.cc_workflow(output_path=cc_output_path)
        print(f"  Connected components took: {time.time() - t0} seconds")
        
        # Step 5.5: Remove duplicates
        print("  5.5: Removing fuzzy duplicates...")
        input_dataset = DocumentDataset.read_json(input_dir, backend="cudf")
        
        cc_result = dask_cudf.read_parquet(cc_output_path, split_row_groups=False).repartition(npartitions=1)
        cc_result = cc_result.set_index("group", shuffle="tasks")
        
        def assign_cumcount(df):
            df["cumcount"] = df.groupby(level=0).cumcount()
            df = df[df["cumcount"] >= 1]
            return df.drop(columns=["cumcount"])
        
        docs_to_remove = cc_result.map_partitions(assign_cumcount, meta=cc_result)
        docs_to_remove = docs_to_remove.reset_index()[["id"]]
        docs_to_remove = docs_to_remove.rename(columns={"id": "to_remove_doc_id"})
        docs_to_remove = docs_to_remove.reset_index(drop=True).persist()
        
        print(f"  Found {len(docs_to_remove)} fuzzy duplicates to remove")
        
        # Remove duplicates
        dedup_output_dir = expand_outdir_and_mkdir(os.path.join(self.base_output_dir, "rpv2-fuzzy-deduped"))
        input_df = input_dataset.df[["raw_content", "id"]]
        
        deduped_df = input_df.merge(docs_to_remove, left_on=["id"], right_on=["to_remove_doc_id"], how="left")
        deduped_df = deduped_df[deduped_df["to_remove_doc_id"].isna()].drop(columns=["to_remove_doc_id"]).reset_index(drop=True)
        
        t0 = time.time()
        deduped_df.to_parquet(dedup_output_dir)
        print(f"  Writing deduped dataset took: {time.time() - t0} seconds")
        print(f"  Final dataset size: {len(deduped_df)} documents")
        
        return dedup_output_dir
    
    def step6_quality_filtering(self, input_dir, filter_config_path=None):
        """Quality filtering using heuristics"""
        print("Step 6: Quality filtering...")
        
        if filter_config_path is None:
            # You'll need to create or provide this config file
            filter_config_path = "/path/to/heuristic_filter_en.yaml"
        
        output_dir = expand_outdir_and_mkdir(os.path.join(self.base_output_dir, "rpv2-filtered"))
        
        t0 = time.time()
        dataset = DocumentDataset.read_parquet(input_dir)
        filter_pipeline = build_filter_pipeline(filter_config_path)
        filtered_dataset = filter_pipeline(dataset)
        filtered_dataset.to_parquet(output_dir)
        
        print(f"Quality filtering took: {time.time() - t0} seconds")
        print(f"Filtered dataset size: {len(filtered_dataset)} documents")
        
        return output_dir
    
    def run_full_pipeline(self):
        """Run the complete curation pipeline"""
        print("Starting NeMo Curator Pipeline on P5 instance...")
        
        # Step 1: Load and optionally reshard
        input_files = self.step1_load_and_reshard()
        
        # Step 2: Add IDs
        id_dir = self.step2_add_ids(input_files)
        
        # Step 3: Text cleaning (skip language ID since you have English data)
        clean_dir = self.step3_text_cleaning(id_dir)
        
        # Step 4: Exact deduplication
        exact_dedup_dir = self.step4_exact_dedup(clean_dir)
        
        # Step 5: Fuzzy deduplication
        fuzzy_dedup_dir = self.step5_fuzzy_dedup(exact_dedup_dir)
        
        # Step 6: Quality filtering (optional, requires config file)
        # final_dir = self.step6_quality_filtering(fuzzy_dedup_dir)
        
        print("Pipeline complete!")
        print(f"Final output: {fuzzy_dedup_dir}")
        
        # Clean up
        self.gpu_client.close()
        self.gpu_cluster.close()

# Run the pipeline
if __name__ == "__main__":
    pipeline = NeMoCuratorP5Pipeline(
        base_data_dir="/data/rpv2-processed",
        base_output_dir="/output/curated"
    )
    pipeline.run_full_pipeline()