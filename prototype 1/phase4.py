import pandas as pd
import logging
import os

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatasetAssembler:
    def __init__(self, history_file: str, features_file: str):
        self.history_file = history_file
        self.features_file = features_file

    def load_data(self):
        """Loads and verifies existence of input CSVs."""
        if not os.path.exists(self.history_file):
            raise FileNotFoundError(f"Missing Phase 2 output: {self.history_file}")
        if not os.path.exists(self.features_file):
            raise FileNotFoundError(f"Missing Phase 3 output: {self.features_file}")

        self.df_history = pd.read_csv(self.history_file)
        self.df_features = pd.read_csv(self.features_file)
        
        logger.info(f"Loaded History: {len(self.df_history)} job records")
        logger.info(f"Loaded Features: {len(self.df_features)} job config records")

    def aggregate_history(self) -> pd.DataFrame:
        """
        Aggregates granular job costs into a single Total Run Cost.
        """
        # Group by the unique execution (Run ID)
        # We take 'first' for metadata like sha/repo, and 'sum' for metrics
        agg_rules = {
            'repo_name': 'first',
            'head_sha': 'first',
            'workflow_name': 'first',
            'financial_cost_usd': 'sum', # Target Variable
            'duration_seconds': 'sum',
            'step_count': 'sum' # Actual steps executed (for comparison)
        }
        
        df_agg = self.df_history.groupby('run_id').agg(agg_rules).reset_index()
        df_agg.rename(columns={
            'financial_cost_usd': 'actual_total_cost',
            'duration_seconds': 'actual_total_duration',
            'step_count': 'actual_steps_executed'
        }, inplace=True)
        
        return df_agg

    def aggregate_features(self) -> pd.DataFrame:
        """
        Aggregates static YAML features to the Workflow File level.
        """
        # We need to sum up the complexity of the entire file to match the run
        agg_rules = {
            'matrix_size': 'sum', # Total jobs expected
            'step_count': 'sum',  # Total static steps defined
            'uses_action_count': 'sum',
            'has_services': 'max', # If any job has a service, the run has services
            'runner_os': lambda x: x.mode()[0] if not x.mode().empty else 'linux' # Dominant OS
        }
        
        # Group by Commit SHA and Repo (Unique version of the code)
        df_agg = self.df_features.groupby(['repo_name', 'commit_sha']).agg(agg_rules).reset_index()
        
        df_agg.rename(columns={
            'matrix_size': 'predicted_job_count',
            'step_count': 'static_step_count'
        }, inplace=True)
        
        return df_agg

    def build_master_dataset(self, output_file: str = "final_training_data.csv"):
        """
        Joins History and Features to create the ML input.
        """
        run_data = self.aggregate_history()
        feature_data = self.aggregate_features()

        logger.info("Merging datasets based on Repository and Commit SHA...")
        
        # Inner Join: We only want runs where we have BOTH the cost AND the features
        # Phase 2 'head_sha' maps to Phase 3 'commit_sha'
        master_df = pd.merge(
            run_data,
            feature_data,
            left_on=['repo_name', 'head_sha'],
            right_on=['repo_name', 'commit_sha'],
            how='inner'
        )

        # Cleanup duplicate columns from join keys
        if 'commit_sha' in master_df.columns:
            master_df.drop(columns=['commit_sha'], inplace=True)

        # Feature Engineering: Complexity Score
        # A simple interaction feature: Jobs * Steps
        master_df['complexity_score'] = master_df['predicted_job_count'] * master_df['static_step_count']

        logger.info(f"Merged Data Shape: {master_df.shape}")
        
        if not master_df.empty:
            master_df.to_csv(output_file, index=False)
            logger.info(f"Phase 4 Complete. Master dataset saved to: {output_file}")
            
            # Preview for the user
            print("\n--- Master Dataset Preview ---")
            print(master_df[['repo_name', 'predicted_job_count', 'static_step_count', 'actual_total_cost']].head())
        else:
            logger.error("Merge resulted in empty dataset! Check if Commit SHAs match between Phase 2 and 3.")

# --- Execution ---
if __name__ == "__main__":
    # Ensure you have run Phase 2 and Phase 3 first to generate these files
    PHASE_2_OUTPUT = "master_dataset.csv"  # From Phase 2 script
    PHASE_3_OUTPUT = "features_dataset.csv" # From Phase 3 script
    
    try:
        assembler = DatasetAssembler(PHASE_2_OUTPUT, PHASE_3_OUTPUT)
        assembler.load_data()
        assembler.build_master_dataset()
    except Exception as e:
        logger.error(f"Failed to assemble dataset: {e}")