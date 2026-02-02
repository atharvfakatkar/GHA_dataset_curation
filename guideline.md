# Project Guideline: GitHub Action Cost Prediction System (Zero-Run)

**Version:** 1.1.0 (Research Edition)
**Type:** Technical Specification & Development Roadmap
**Target Model:** XGBoost (Regression) vs. Baselines

---

## 1. Executive Summary
The objective is to build a Machine Learning system capable of predicting the financial cost (USD) of a GitHub Action workflow *before* it is executed ("Zero-Run Prediction").

Unlike traditional observability tools that report costs *after* the fact, this system analyzes the static definition of the workflow (`.yaml`) and the state of the codebase to forecast resource consumption. A key innovation of this version is the **"Broad-Spectrum" feature extraction**, followed by rigorous Exploratory Data Analysis (EDA) to scientifically determine which static attributes correlate most strongly with cost.

---

## 2. System Architecture
The system is architected as a sequential 4-stage pipeline.

### Component A: The Historic Miner (Ground Truth)
* **Purpose:** Builds the "Target Variable" (Actual Cost) by mining historical run data.
* **Output:** `raw_run_history.csv`

### Component B: The Vacuum Extractor (Broad-Spectrum Features)
* **Purpose:** Extracts a comprehensive "Raw Feature Vector" from historical YAML files without prior assumptions.
* **Responsibility:**
    * **Time Travel:** Checkout repository to specific `commit_sha`.
    * **Broad Extraction:** Parse ~25+ syntactic and structural attributes from the YAML (e.g., nesting depth, matrix size, distinct actions used).
* **Output:** `comprehensive_features.csv`

### Component C: The Analyst (EDA & Selection)
* **Purpose:** Statistical validation and feature reduction.
* **Responsibility:** Perform Correlation Analysis and Mutual Information regression to select the top 8-10 predictive features.
* **Output:** `selected_features.csv` (The filtered training set).

### Component D: The Predictor (ML Core)
* **Purpose:** Model training on the scientifically selected features.
* **Output:** `cost_predictor_model.json` + `performance_report.md`

---

## 3. Data Gathering Strategy (The Pipeline)

### Step 1: Repository Selection
**Rule:** Select 10-20 diverse open-source repositories to ensure the model generalizes.
* **Criteria:** High Activity (>50 runs/week), Language Diversity (Python/JS/Rust/C++), and Matrix Strategy usage.

### Step 2: The Mining Logic (Ground Truth)
**Algorithm:**
1.  Iterate through target repositories.
2.  Fetch `workflow_runs` via GitHub API (filter for `success`).
3.  **Calculate Actual Cost (Target):**
    * Formula: `Duration_Minutes * OS_Multiplier * Base_Rate`
    * *Rates:* Linux ($0.008), Windows ($0.016), macOS ($0.08).

### Step 3: The "Vacuum" Extraction Logic (Zero-Run)
**Critical Update:** Instead of extracting only a few pre-selected features, we will extract a wide array of potential signals to allow for data-driven feature selection later.

#### **Category 1: Structural Complexity**
* `yaml_line_count`: Raw length of the file.
* `yaml_depth`: Maximum nesting level of the YAML tree (proxy for logic complexity).
* `total_steps`: Sum of steps across all jobs.
* `avg_steps_per_job`: `total_steps / job_count`.

#### **Category 2: The "Matrix" Multipliers**
* `uses_matrix_strategy`: Boolean.
* `matrix_dimensions`: Count of keys in `strategy.matrix`.
* `matrix_permutations`: Mathematical product of matrix options (The "Expansion Factor").
* `fail_fast`: Boolean (True/False).

#### **Category 3: Execution Environment**
* `os_label`: Categorical (`ubuntu`, `windows`, `macos`, `self-hosted`).
* `container_image`: String presence (implies Docker overhead).
* `timeout_minutes`: The explicit timeout set.

#### **Category 4: Action Ecosystem**
* `unique_actions_used`: Count of distinct `uses:` clauses.
* `is_using_setup_actions`: Boolean (detects `setup-node`, `setup-python`).
* `is_using_docker_actions`: Boolean (detects `docker/build-push-action`).
* `is_using_cache`: Boolean (detects `actions/cache`).

#### **Category 5: Logic & Flow**
* `env_var_count`: Count of `env:` keys defined.
* `if_condition_count`: Count of `if:` statements (branching logic).
* `needs_dependencies_count`: Count of `needs:` keywords (DAG complexity).

### Step 4: Exploratory Data Analysis (The Research Core)
**Goal:** Scientifically select the final features for the model.
1.  **Correlation Matrix:** Generate Pearson/Spearman heatmaps to identify features co-linear with `Actual Cost`.
2.  **Mutual Information:** Use `sklearn` to detect non-linear relationships.
3.  **Variance Analysis:** Drop features with zero variance (e.g., if 100% of repos use Linux, drop `os_label`).

---

## 4. Detailed Implementation Specification

### 4.1. Technology Stack
* **Language:** Python 3.9+
* **Libraries:** `PyGithub`, `PyYAML`, `Pandas`, `XGBoost`, `Scikit-Learn` (Feature Selection), `Seaborn` (Visualization).

### 4.2. Functional Components

#### **Module 1: `miner.py`**
* **Function:** `fetch_run_history(repo_url)`
* **Requirement:** Implement robust error handling (403/429) with exponential backoff.

#### **Module 2: `extractor.py` (The Flattener)**
* **Function:** `parse_yaml_to_vector(yaml_content)`
    * Must output a flat dictionary containing ALL 25+ features listed in Step 3.
    * Must robustly handle malformed or empty YAMLs by returning `None` (row skip).

#### **Module 3: `analysis_and_selection.py` (New)**
* **Function:** `generate_eda_report(df)`
    * Plots: Correlation Heatmap, Feature Importance Bar Chart (Random Forest quick-pass).
* **Function:** `select_best_features(df, k=10)`
    * Returns the reduced dataset with only the top `k` most predictive features.

#### **Module 4: `model_trainer.py`**
* **Input:** The *reduced* dataset from Module 3.
* **Model:** XGBoost Regressor.
* **Evaluation:** Compare MAE of the "All Features" model vs. the "Selected Features" model to prove efficiency.

---

## 5. User Stories

### Story 1: The Researcher's Validation
**As a** Researcher,
**I want** to visualize the correlation between "Matrix Permutations" and "Cost",
**So that** I can prove in my paper that structural expansion is the primary driver of cost.
* *Acceptance Criteria:* A generated PNG plot `correlation_matrix.png` showing coefficient values.

### Story 2: The Data Miner
**As a** Data Engineer,
**I want** to extract every possible attribute from the YAML, even ones I'm unsure about,
**So that** I don't have to re-run the miner later if I decide to test a new hypothesis.
* *Acceptance Criteria:* The `extractor.py` saves a "wide" CSV with 25+ columns.

### Story 3: The Zero-Run Predictor
**As a** User,
**I want** to predict cost without running the workflow,
**So that** I can save money.
* *Acceptance Criteria:* The model inference pipeline requires *only* the YAML file input, no runtime logs.

---

## 6. Deliverables Checklist
1.  [ ] `data/raw_run_history.csv`
2.  [ ] `data/comprehensive_features.csv` (The "Wide" Dataset)
3.  [ ] `data/selected_features.csv` (The "Clean" Dataset)
4.  [ ] `src/miner.py`
5.  [ ] `src/extractor.py`
6.  [ ] `src/analysis.py` (EDA & Feature Selection)
7.  [ ] `notebooks/research_eda.ipynb` (Graphs & findings for the paper)
8.  [ ] `src/train.py`