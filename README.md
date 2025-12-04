# Hackathon Project: Retail Data Pipeline

## Quick Start

### Step 1: Download Dataset
Download from Kaggle:
https://www.kaggle.com/datasets/anirudhchauhan/retail-store-inventory-forecasting-dataset

Save the CSV file as: `raw/retail_store_inventory.csv`

### Step 2: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 3: Run Task 1 Pipeline
```bash
python etl/task1_inventory.py
```

## Project Structure
```
Luck_Rising/
├── raw/                    # Raw data (Bronze layer)
├── staging/                # Validated data (Silver layer)
├── curated/                # Final analytics data (Gold layer)
├── quarantine/             # Invalid records for audit
├── config/                 # YAML configuration files
│   └── task1_config.yml
├── etl/                    # ETL pipeline scripts
│   └── task1_inventory.py
└── requirements.txt
```

## Task 1 Output
- `curated/inventory_fact/` - Final inventory fact table
- `quarantine/inventory_audit/` - Invalid records
- `curated/pipeline_report.txt` - Execution report

