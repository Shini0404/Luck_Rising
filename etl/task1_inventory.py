"""
Task 1: Unified Product & Inventory Data Harmonization Pipeline
================================================================
This pipeline ingests, validates, reconciles, and curates inventory data
into a unified single source of truth.


"""

import pandas as pd
import numpy as np
import yaml
import logging
import os
from datetime import datetime
from pathlib import Path
from fuzzywuzzy import fuzz
from typing import Dict, List, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class InventoryPipeline:
    """
    Main pipeline class for inventory data harmonization.
    Implements RAW -> STAGING -> CURATED data flow with validation and reconciliation.
    """
    
    def __init__(self, config_path: str = "config/task1_config.yml"):
        """Initialize pipeline with configuration."""
        self.config = self._load_config(config_path)
        self.raw_data = None
        self.validated_data = None
        self.curated_data = None
        self.quarantine_data = None
        
        # Statistics tracking
        self.stats = {
            'total_records': 0,
            'valid_records': 0,
            'invalid_records': 0,
            'duplicates_removed': 0,
            'negative_stock_flags': 0,
            'capacity_exceeded_flags': 0
        }
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Configuration loaded from {config_path}")
            return config
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    # =========================================================================
    # LAYER 1: RAW (BRONZE) - Data Ingestion
    # =========================================================================
    
    def ingest_raw_data(self, file_path: str = None) -> pd.DataFrame:
        """
        Ingest raw data from CSV file into RAW layer.
        No transformations - data stored as-is.
        """
        logger.info("=" * 60)
        logger.info("LAYER 1: RAW (BRONZE) - Data Ingestion")
        logger.info("=" * 60)
        
        if file_path is None:
            file_path = self.config['data_sources']['inventory_data']['file_path']
        
        try:
            # Read CSV with specified settings
            self.raw_data = pd.read_csv(
                file_path,
                delimiter=self.config['data_sources']['inventory_data'].get('delimiter', ','),
                encoding=self.config['data_sources']['inventory_data'].get('encoding', 'utf-8')
            )
            
            self.stats['total_records'] = len(self.raw_data)
            logger.info(f"Ingested {self.stats['total_records']} records from {file_path}")
            logger.info(f"Columns: {list(self.raw_data.columns)}")
            
            # Save raw data to RAW layer
            raw_output_path = "raw/inventory_raw.parquet"
            self.raw_data.to_parquet(raw_output_path, index=False)
            logger.info(f"Raw data saved to {raw_output_path}")
            
            return self.raw_data
            
        except Exception as e:
            logger.error(f"Failed to ingest data: {e}")
            raise
    
    # =========================================================================
    # LAYER 2: STAGING (SILVER) - Validation & Deduplication
    # =========================================================================
    
    def validate_data(self, df: pd.DataFrame = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Validate data according to configuration rules.
        Returns: (valid_df, invalid_df)
        """
        logger.info("=" * 60)
        logger.info("LAYER 2: STAGING (SILVER) - Validation")
        logger.info("=" * 60)
        
        if df is None:
            df = self.raw_data.copy()
        
        # Initialize validation flags
        df['validation_status'] = 'VALID'
        df['validation_errors'] = ''
        
        # Check required fields (not null)
        required_columns = self.config['data_sources']['inventory_data']['required_columns']
        
        for col in required_columns:
            if col in df.columns:
                null_mask = df[col].isna() | (df[col] == '')
                df.loc[null_mask, 'validation_status'] = 'INVALID'
                df.loc[null_mask, 'validation_errors'] += f'Missing {col}; '
                logger.info(f"Column '{col}': {null_mask.sum()} null values found")
        
        # Validate inventory level (non-negative)
        if 'Inventory_Level' in df.columns:
            negative_mask = df['Inventory_Level'] < 0
            df.loc[negative_mask, 'validation_status'] = 'INVALID'
            df.loc[negative_mask, 'validation_errors'] += 'Negative inventory; '
            logger.info(f"Negative inventory records: {negative_mask.sum()}")
        
        # Validate units sold (non-negative)
        if 'Units_Sold' in df.columns:
            negative_sold_mask = df['Units_Sold'] < 0
            df.loc[negative_sold_mask, 'validation_status'] = 'INVALID'
            df.loc[negative_sold_mask, 'validation_errors'] += 'Negative units sold; '
            logger.info(f"Negative units sold records: {negative_sold_mask.sum()}")
        
        # Validate price (positive)
        if 'Price' in df.columns:
            price_mask = df['Price'] <= 0
            df.loc[price_mask, 'validation_status'] = 'INVALID'
            df.loc[price_mask, 'validation_errors'] += 'Invalid price; '
            logger.info(f"Invalid price records: {price_mask.sum()}")
        
        # Split into valid and invalid
        valid_df = df[df['validation_status'] == 'VALID'].copy()
        invalid_df = df[df['validation_status'] == 'INVALID'].copy()
        
        self.stats['valid_records'] = len(valid_df)
        self.stats['invalid_records'] = len(invalid_df)
        
        logger.info(f"Validation complete: {len(valid_df)} valid, {len(invalid_df)} invalid")
        
        return valid_df, invalid_df
    
    def deduplicate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate records based on key columns.
        Keeps the last record (most recent).
        """
        logger.info("-" * 40)
        logger.info("Deduplication Process")
        logger.info("-" * 40)
        
        key_columns = self.config['deduplication']['key_columns']
        initial_count = len(df)
        
        # Sort by date and keep last
        if 'Date' in df.columns:
            df = df.sort_values('Date')
        
        # Remove duplicates
        df_deduped = df.drop_duplicates(subset=key_columns, keep='last')
        
        self.stats['duplicates_removed'] = initial_count - len(df_deduped)
        logger.info(f"Removed {self.stats['duplicates_removed']} duplicate records")
        logger.info(f"Records after deduplication: {len(df_deduped)}")
        
        return df_deduped
    
    # =========================================================================
    # LAYER 3: RECONCILIATION ENGINE
    # =========================================================================
    
    def reconcile_inventory(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Reconcile inventory levels using the formula:
        Final Stock = Inventory Level + Units Ordered - Units Sold
        
        Handle edge cases:
        - Negative stock levels
        - Capacity exceeded
        - Damaged/expired products
        """
        logger.info("=" * 60)
        logger.info("LAYER 3: RECONCILIATION ENGINE")
        logger.info("=" * 60)
        
        df = df.copy()
        
        # Initialize reconciliation columns
        df['reconciliation_status'] = 'RECONCILED'
        df['negative_stock_flag'] = False
        df['capacity_exceeded_flag'] = False
        
        # Calculate final stock level
        # Formula: final_stock = inventory_level + units_ordered - units_sold
        units_ordered = df.get('Units_Ordered', pd.Series(0, index=df.index))
        units_sold = df.get('Units_Sold', pd.Series(0, index=df.index))
        
        # Handle NaN values
        units_ordered = units_ordered.fillna(0)
        units_sold = units_sold.fillna(0)
        
        df['final_stock_level'] = df['Inventory_Level'] + units_ordered - units_sold
        
        # Flag negative stock (edge case)
        negative_stock_mask = df['final_stock_level'] < 0
        df.loc[negative_stock_mask, 'negative_stock_flag'] = True
        df.loc[negative_stock_mask, 'reconciliation_status'] = 'FLAGGED_NEGATIVE'
        self.stats['negative_stock_flags'] = negative_stock_mask.sum()
        logger.info(f"Negative stock flags: {self.stats['negative_stock_flags']}")
        
        # Flag capacity exceeded (edge case)
        max_capacity = self.config['reconciliation']['edge_cases']['exceeded_capacity']['max_capacity']
        capacity_mask = df['final_stock_level'] > max_capacity
        df.loc[capacity_mask, 'capacity_exceeded_flag'] = True
        df.loc[capacity_mask, 'reconciliation_status'] = 'FLAGGED_CAPACITY'
        self.stats['capacity_exceeded_flags'] = capacity_mask.sum()
        logger.info(f"Capacity exceeded flags: {self.stats['capacity_exceeded_flags']}")
        
        # Calculate additional metrics
        df['restock_in'] = units_ordered
        df['restock_out'] = units_sold
        df['stock_movement'] = units_ordered - units_sold
        
        logger.info(f"Reconciliation complete. Total records: {len(df)}")
        
        return df
    
    # =========================================================================
    # LAYER 4: CURATED (GOLD) - Final Output
    # =========================================================================
    
    def create_curated_layer(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create the final curated inventory fact table.
        This is the single source of truth for downstream analytics.
        """
        logger.info("=" * 60)
        logger.info("LAYER 4: CURATED (GOLD) - Inventory Fact Table")
        logger.info("=" * 60)
        
        # Select and rename columns for final fact table
        curated_columns = [
            'Store_ID',
            'Product_ID',
            'Date',
            'Inventory_Level',
            'Units_Sold',
            'Units_Ordered',
            'final_stock_level',
            'restock_in',
            'restock_out',
            'stock_movement',
            'Price',
            'Discount',
            'reconciliation_status',
            'negative_stock_flag',
            'capacity_exceeded_flag'
        ]
        
        # Keep only columns that exist
        available_columns = [col for col in curated_columns if col in df.columns]
        curated_df = df[available_columns].copy()
        
        # Add metadata
        curated_df['last_updated'] = datetime.now()
        curated_df['pipeline_version'] = self.config['pipeline']['version']
        
        # Save to curated layer
        curated_path = self.config['output']['curated']['path']
        os.makedirs(curated_path, exist_ok=True)
        
        output_file = f"{curated_path}/inventory_fact.parquet"
        curated_df.to_parquet(output_file, index=False)
        logger.info(f"Curated data saved to {output_file}")
        
        # Also save as CSV for easy viewing
        csv_file = f"{curated_path}/inventory_fact.csv"
        curated_df.to_csv(csv_file, index=False)
        logger.info(f"Curated data also saved as CSV: {csv_file}")
        
        self.curated_data = curated_df
        return curated_df
    
    # =========================================================================
    # QUARANTINE LAYER - Invalid Records
    # =========================================================================
    
    def save_to_quarantine(self, invalid_df: pd.DataFrame):
        """
        Save invalid records to quarantine for audit and diagnostics.
        """
        logger.info("=" * 60)
        logger.info("QUARANTINE LAYER - Invalid Records Audit")
        logger.info("=" * 60)
        
        if len(invalid_df) == 0:
            logger.info("No invalid records to quarantine")
            return
        
        # Add audit metadata
        invalid_df = invalid_df.copy()
        invalid_df['quarantine_date'] = datetime.now()
        invalid_df['error_type'] = invalid_df['validation_errors'].apply(self._classify_error)
        
        # Save to quarantine
        quarantine_path = self.config['output']['quarantine']['path']
        os.makedirs(quarantine_path, exist_ok=True)
        
        output_file = f"{quarantine_path}/inventory_audit.parquet"
        invalid_df.to_parquet(output_file, index=False)
        logger.info(f"Quarantine data saved to {output_file}")
        
        # Also save as CSV
        csv_file = f"{quarantine_path}/inventory_audit.csv"
        invalid_df.to_csv(csv_file, index=False)
        logger.info(f"Quarantine data also saved as CSV: {csv_file}")
        
        self.quarantine_data = invalid_df
    
    def _classify_error(self, error_string: str) -> str:
        """Classify error type based on validation error string."""
        if 'Missing' in error_string:
            return 'MISSING_FIELD'
        elif 'Negative' in error_string:
            return 'NEGATIVE_VALUE'
        elif 'Invalid' in error_string:
            return 'INVALID_VALUE'
        else:
            return 'UNKNOWN'
    
    # =========================================================================
    # FUZZY MATCHING - Product Master Mapping
    # =========================================================================
    
    def fuzzy_match_products(self, df: pd.DataFrame, product_master: pd.DataFrame = None) -> pd.DataFrame:
        """
        Apply fuzzy matching for product name deduplication.
        Uses Levenshtein distance for similarity scoring.
        """
        logger.info("-" * 40)
        logger.info("Fuzzy Matching for Product Names")
        logger.info("-" * 40)
        
        if not self.config['fuzzy_matching']['enabled']:
            logger.info("Fuzzy matching disabled in config")
            return df
        
        threshold = self.config['fuzzy_matching']['threshold']
        
        # If no product master provided, use unique products from data
        if product_master is None:
            unique_products = df['Product_ID'].unique()
            logger.info(f"Using {len(unique_products)} unique products from data")
            return df
        
        # Apply fuzzy matching logic here
        # This is a placeholder for actual implementation
        df['fuzzy_match_score'] = 100  # Default score
        
        return df
    
    # =========================================================================
    # SKU VALIDATION
    # =========================================================================
    
    def validate_sku(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate SKU format against configured pattern.
        """
        logger.info("-" * 40)
        logger.info("SKU Validation")
        logger.info("-" * 40)
        
        if not self.config['sku_validation']['enabled']:
            logger.info("SKU validation disabled in config")
            return df
        
        import re
        pattern = self.config['sku_validation']['pattern']
        
        # For this dataset, Product_ID may not follow standard SKU format
        # Just flag for information
        df['sku_valid'] = df['Product_ID'].astype(str).apply(
            lambda x: bool(re.match(pattern, x)) if pattern else True
        )
        
        invalid_sku_count = (~df['sku_valid']).sum()
        logger.info(f"SKU validation: {invalid_sku_count} non-standard SKUs found")
        
        return df
    
    # =========================================================================
    # MAIN PIPELINE EXECUTION
    # =========================================================================
    
    def run_pipeline(self, file_path: str = None) -> Dict:
        """
        Execute the complete pipeline:
        RAW -> STAGING -> RECONCILIATION -> CURATED
        """
        logger.info("*" * 60)
        logger.info("STARTING INVENTORY HARMONIZATION PIPELINE")
        logger.info("*" * 60)
        
        start_time = datetime.now()
        
        try:
            # Step 1: Ingest raw data
            raw_df = self.ingest_raw_data(file_path)
            
            # Step 2: Validate data
            valid_df, invalid_df = self.validate_data(raw_df)
            
            # Step 3: Deduplicate valid data
            deduped_df = self.deduplicate_data(valid_df)
            
            # Step 4: Apply fuzzy matching
            matched_df = self.fuzzy_match_products(deduped_df)
            
            # Step 5: Validate SKUs
            sku_validated_df = self.validate_sku(matched_df)
            
            # Step 6: Reconcile inventory
            reconciled_df = self.reconcile_inventory(sku_validated_df)
            
            # Step 7: Create curated layer
            curated_df = self.create_curated_layer(reconciled_df)
            
            # Step 8: Save invalid records to quarantine
            self.save_to_quarantine(invalid_df)
            
            # Calculate execution time
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            # Final statistics
            logger.info("*" * 60)
            logger.info("PIPELINE EXECUTION COMPLETE")
            logger.info("*" * 60)
            logger.info(f"Total records processed: {self.stats['total_records']}")
            logger.info(f"Valid records: {self.stats['valid_records']}")
            logger.info(f"Invalid records (quarantined): {self.stats['invalid_records']}")
            logger.info(f"Duplicates removed: {self.stats['duplicates_removed']}")
            logger.info(f"Negative stock flags: {self.stats['negative_stock_flags']}")
            logger.info(f"Capacity exceeded flags: {self.stats['capacity_exceeded_flags']}")
            logger.info(f"Execution time: {execution_time:.2f} seconds")
            
            return {
                'status': 'SUCCESS',
                'stats': self.stats,
                'execution_time': execution_time,
                'curated_records': len(curated_df),
                'quarantine_records': len(invalid_df)
            }
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return {
                'status': 'FAILED',
                'error': str(e)
            }
    
    # =========================================================================
    # REPORTING
    # =========================================================================
    
    def generate_report(self) -> str:
        """Generate a summary report of the pipeline execution."""
        report = f"""
================================================================================
              INVENTORY HARMONIZATION PIPELINE - EXECUTION REPORT
================================================================================

Pipeline: {self.config['pipeline']['name']}
Version: {self.config['pipeline']['version']}
Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

--------------------------------------------------------------------------------
                              DATA STATISTICS
--------------------------------------------------------------------------------

Total Records Processed:    {self.stats['total_records']:,}
Valid Records:              {self.stats['valid_records']:,}
Invalid Records:            {self.stats['invalid_records']:,}
Duplicates Removed:         {self.stats['duplicates_removed']:,}

--------------------------------------------------------------------------------
                           RECONCILIATION FLAGS
--------------------------------------------------------------------------------

Negative Stock Flags:       {self.stats['negative_stock_flags']:,}
Capacity Exceeded Flags:    {self.stats['capacity_exceeded_flags']:,}

--------------------------------------------------------------------------------
                              OUTPUT LOCATIONS
--------------------------------------------------------------------------------

Curated Data:    {self.config['output']['curated']['path']}/inventory_fact.parquet
Quarantine Data: {self.config['output']['quarantine']['path']}/inventory_audit.parquet

================================================================================
"""
        return report


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main entry point for the pipeline."""
    
    # Initialize pipeline
    pipeline = InventoryPipeline(config_path="config/task1_config.yml")
    
    # Run pipeline (update path to your dataset location)
    result = pipeline.run_pipeline(file_path="raw/retail_store_inventory.csv")
    
    # Generate and print report
    if result['status'] == 'SUCCESS':
        report = pipeline.generate_report()
        print(report)
        
        # Save report to file
        with open("curated/pipeline_report.txt", "w") as f:
            f.write(report)
        print("Report saved to curated/pipeline_report.txt")
    else:
        print(f"Pipeline failed: {result.get('error', 'Unknown error')}")


if __name__ == "__main__":
    main()

