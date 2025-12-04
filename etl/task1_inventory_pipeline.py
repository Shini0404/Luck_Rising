"""
================================================================================
TASK 1: UNIFIED PRODUCT & INVENTORY DATA HARMONIZATION PIPELINE
================================================================================

Goal: Create an automated pipeline that brings together inventory snapshots,
      restock logs, and product catalog data to maintain a clean, accurate
      "single source of truth" inventory model.

Pipeline Flow:
    RAW → STAGING (Validation) → RECONCILIATION → CURATED
                                       ↓
                               QUARANTINE (Invalid Records)

Author: Hackathon Team
Version: 2.0
================================================================================
"""

import pandas as pd
import numpy as np
import yaml
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field

# Fuzzy matching
try:
    from fuzzywuzzy import fuzz, process
    FUZZY_AVAILABLE = True
except ImportError:
    FUZZY_AVAILABLE = False
    print("Warning: fuzzywuzzy not installed. Fuzzy matching disabled.")


# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging(config: Dict) -> logging.Logger:
    """Configure logging based on config settings."""
    log_config = config.get('logging', {})
    
    # Create logs directory
    log_file = log_config.get('file', 'logs/task1_pipeline.log')
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Configure logger
    logger = logging.getLogger('InventoryPipeline')
    logger.setLevel(getattr(logging, log_config.get('level', 'INFO')))
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter(
        log_config.get('format', '%(asctime)s | %(levelname)-8s | %(message)s')
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    if log_config.get('console', True):
        logger.addHandler(console_handler)
    
    return logger


# =============================================================================
# DATA CLASSES FOR STATISTICS
# =============================================================================

@dataclass
class PipelineStats:
    """Track pipeline execution statistics."""
    # Inventory snapshot stats
    snapshot_total: int = 0
    snapshot_valid: int = 0
    snapshot_invalid: int = 0
    snapshot_duplicates: int = 0
    
    # Restock events stats
    restock_total: int = 0
    restock_valid: int = 0
    restock_invalid: int = 0
    
    # Validation stats
    negative_stock_count: int = 0
    mismatched_product_count: int = 0
    exceeded_capacity_count: int = 0
    exceeded_restock_max_count: int = 0
    
    # Fuzzy matching stats
    fuzzy_matches_found: int = 0
    unmatched_products: int = 0
    
    # Final stats
    curated_records: int = 0
    quarantine_records: int = 0
    
    execution_time: float = 0.0


# =============================================================================
# CONFIG-DRIVEN DATA LOADER
# =============================================================================

class ConfigDrivenLoader:
    """
    Config-driven data loader that can ingest any new file without code changes.
    Just add the file configuration to task1_config.yml and it will be loaded.
    """
    
    def __init__(self, config: Dict, logger: logging.Logger):
        self.config = config
        self.logger = logger
    
    def load_dataset(self, dataset_name: str) -> pd.DataFrame:
        """Load a dataset based on its configuration."""
        
        if dataset_name not in self.config['data_sources']:
            raise ValueError(f"Dataset '{dataset_name}' not found in configuration")
        
        ds_config = self.config['data_sources'][dataset_name]
        file_path = ds_config['file_path']
        
        self.logger.info(f"Loading dataset: {dataset_name} from {file_path}")
        
        # Check if file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Load based on file type
        file_type = ds_config.get('file_type', 'csv')
        
        if file_type == 'csv':
            df = pd.read_csv(
                file_path,
                delimiter=ds_config.get('delimiter', ','),
                encoding=ds_config.get('encoding', 'utf-8')
            )
        elif file_type == 'parquet':
            df = pd.read_parquet(file_path)
        elif file_type == 'json':
            df = pd.read_json(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
        
        # Validate required columns
        required_cols = ds_config.get('required_columns', [])
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            self.logger.warning(f"Missing columns in {dataset_name}: {missing_cols}")
        
        self.logger.info(f"Loaded {len(df)} records from {dataset_name}")
        self.logger.info(f"Columns: {list(df.columns)}")
        
        return df


# =============================================================================
# MAIN PIPELINE CLASS
# =============================================================================

class InventoryHarmonizationPipeline:
    """
    Main pipeline class implementing the complete inventory harmonization flow.
    
    Pipeline Steps:
    1. RAW Layer: Ingest inventory_snapshot.csv and restock_events.csv
    2. STAGING Layer: Validate and deduplicate
    3. RECONCILIATION: Compute effective_stock_level
    4. CURATED Layer: Final inventory fact table
    5. QUARANTINE: Invalid records for diagnostics
    """
    
    def __init__(self, config_path: str = "config/task1_config.yml"):
        """Initialize pipeline with configuration."""
        self.config = self._load_config(config_path)
        self.logger = setup_logging(self.config)
        self.loader = ConfigDrivenLoader(self.config, self.logger)
        self.stats = PipelineStats()
        
        # Data storage
        self.raw_data: Dict[str, pd.DataFrame] = {}
        self.staging_data: Dict[str, pd.DataFrame] = {}
        self.curated_data: Optional[pd.DataFrame] = None
        self.quarantine_data: Dict[str, pd.DataFrame] = {}
        
        # Reference data
        self.products_master: Optional[pd.DataFrame] = None
        self.stores_master: Optional[pd.DataFrame] = None
    
    def _load_config(self, config_path: str) -> Dict:
        """Load YAML configuration."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    # =========================================================================
    # LAYER 1: RAW (BRONZE) - Data Ingestion
    # =========================================================================
    
    def ingest_raw_layer(self) -> None:
        """
        Ingest all source data into RAW layer.
        Data is stored as-is without any transformations.
        """
        self.logger.info("=" * 70)
        self.logger.info("LAYER 1: RAW (BRONZE) - Data Ingestion")
        self.logger.info("=" * 70)
        
        # Load inventory snapshot
        self.raw_data['inventory_snapshot'] = self.loader.load_dataset('inventory_snapshot')
        self.stats.snapshot_total = len(self.raw_data['inventory_snapshot'])
        
        # Load restock events
        self.raw_data['restock_events'] = self.loader.load_dataset('restock_events')
        self.stats.restock_total = len(self.raw_data['restock_events'])
        
        # Load reference tables
        self.products_master = self.loader.load_dataset('products')
        self.stores_master = self.loader.load_dataset('stores')
        
        # Save raw data to RAW folder (for audit trail)
        for name, df in self.raw_data.items():
            output_path = f"raw/{name}_raw.parquet"
            df.to_parquet(output_path, index=False)
            self.logger.info(f"Raw data saved: {output_path}")
        
        self.logger.info(f"Total inventory snapshots: {self.stats.snapshot_total}")
        self.logger.info(f"Total restock events: {self.stats.restock_total}")
        self.logger.info(f"Products in master: {len(self.products_master)}")
        self.logger.info(f"Stores in master: {len(self.stores_master)}")
    
    # =========================================================================
    # LAYER 2: STAGING (SILVER) - Validation & Deduplication
    # =========================================================================
    
    def validate_staging_layer(self) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
        """
        Validate data and separate into valid/invalid records.
        
        Validation checks:
        1. Negative stock
        2. Mismatched product_id (not in products master)
        3. Duplicate entries
        4. Restock quantity > logical max
        5. Missing required fields
        """
        self.logger.info("=" * 70)
        self.logger.info("LAYER 2: STAGING (SILVER) - Validation & Deduplication")
        self.logger.info("=" * 70)
        
        valid_data = {}
        invalid_data = {}
        
        # Validate Inventory Snapshots
        valid_snapshot, invalid_snapshot = self._validate_inventory_snapshot(
            self.raw_data['inventory_snapshot']
        )
        valid_data['inventory_snapshot'] = valid_snapshot
        invalid_data['inventory_snapshot'] = invalid_snapshot
        
        # Validate Restock Events
        valid_restock, invalid_restock = self._validate_restock_events(
            self.raw_data['restock_events']
        )
        valid_data['restock_events'] = valid_restock
        invalid_data['restock_events'] = invalid_restock
        
        # Save to staging
        self.staging_data = valid_data
        self.quarantine_data = invalid_data
        
        return valid_data, invalid_data
    
    def _validate_inventory_snapshot(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Validate inventory snapshot data."""
        self.logger.info("-" * 50)
        self.logger.info("Validating Inventory Snapshots...")
        self.logger.info("-" * 50)
        
        df = df.copy()
        df['validation_status'] = 'VALID'
        df['validation_errors'] = ''
        
        # 1. Check missing required fields
        required_fields = ['store_id', 'product_id', 'snapshot_date', 'snapshot_level']
        for field in required_fields:
            if field in df.columns:
                mask = df[field].isna() | (df[field].astype(str).str.strip() == '')
                df.loc[mask, 'validation_status'] = 'INVALID'
                df.loc[mask, 'validation_errors'] += f'MISSING_{field.upper()}; '
                self.logger.info(f"  Missing {field}: {mask.sum()} records")
        
        # 2. Check negative stock
        if 'snapshot_level' in df.columns:
            # Convert to numeric, coercing errors to NaN
            df['snapshot_level'] = pd.to_numeric(df['snapshot_level'], errors='coerce')
            negative_mask = df['snapshot_level'] < 0
            df.loc[negative_mask, 'validation_status'] = 'INVALID'
            df.loc[negative_mask, 'validation_errors'] += 'NEGATIVE_STOCK; '
            self.stats.negative_stock_count = negative_mask.sum()
            self.logger.info(f"  Negative stock: {self.stats.negative_stock_count} records")
        
        # 3. Check mismatched product_id (not in products master)
        valid_products = set(self.products_master['product_id'].dropna().unique())
        mismatched_mask = ~df['product_id'].isin(valid_products) & df['product_id'].notna()
        df.loc[mismatched_mask, 'validation_status'] = 'INVALID'
        df.loc[mismatched_mask, 'validation_errors'] += 'MISMATCHED_PRODUCT_ID; '
        self.stats.mismatched_product_count = mismatched_mask.sum()
        self.logger.info(f"  Mismatched product_id: {self.stats.mismatched_product_count} records")
        
        # 4. Check capacity exceeded
        if 'max_capacity' in df.columns:
            df['max_capacity'] = pd.to_numeric(df['max_capacity'], errors='coerce')
            capacity_mask = df['snapshot_level'] > df['max_capacity']
            df.loc[capacity_mask, 'validation_errors'] += 'CAPACITY_EXCEEDED; '
            # This is a warning, not an error
            self.stats.exceeded_capacity_count = capacity_mask.sum()
            self.logger.info(f"  Capacity exceeded (warning): {self.stats.exceeded_capacity_count} records")
        
        # 5. Deduplicate
        df, duplicates_removed = self._deduplicate_snapshot(df)
        self.stats.snapshot_duplicates = duplicates_removed
        
        # Split valid and invalid
        valid_df = df[df['validation_status'] == 'VALID'].copy()
        invalid_df = df[df['validation_status'] == 'INVALID'].copy()
        
        self.stats.snapshot_valid = len(valid_df)
        self.stats.snapshot_invalid = len(invalid_df)
        
        self.logger.info(f"  Valid records: {self.stats.snapshot_valid}")
        self.logger.info(f"  Invalid records: {self.stats.snapshot_invalid}")
        self.logger.info(f"  Duplicates removed: {self.stats.snapshot_duplicates}")
        
        return valid_df, invalid_df
    
    def _validate_restock_events(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Validate restock events data."""
        self.logger.info("-" * 50)
        self.logger.info("Validating Restock Events...")
        self.logger.info("-" * 50)
        
        df = df.copy()
        df['validation_status'] = 'VALID'
        df['validation_errors'] = ''
        
        # 1. Check missing required fields
        required_fields = ['store_id', 'product_id', 'restock_date', 'incoming_quantity']
        for field in required_fields:
            if field in df.columns:
                mask = df[field].isna() | (df[field].astype(str).str.strip() == '')
                df.loc[mask, 'validation_status'] = 'INVALID'
                df.loc[mask, 'validation_errors'] += f'MISSING_{field.upper()}; '
                self.logger.info(f"  Missing {field}: {mask.sum()} records")
        
        # 2. Check negative/zero incoming quantity
        if 'incoming_quantity' in df.columns:
            df['incoming_quantity'] = pd.to_numeric(df['incoming_quantity'], errors='coerce')
            invalid_qty_mask = df['incoming_quantity'] <= 0
            df.loc[invalid_qty_mask, 'validation_status'] = 'INVALID'
            df.loc[invalid_qty_mask, 'validation_errors'] += 'INVALID_QUANTITY; '
            self.logger.info(f"  Invalid quantity: {invalid_qty_mask.sum()} records")
        
        # 3. Check restock quantity > logical max (from products table)
        df = df.merge(
            self.products_master[['product_id', 'max_restock_quantity']],
            on='product_id',
            how='left'
        )
        exceeded_max_mask = df['incoming_quantity'] > df['max_restock_quantity']
        df.loc[exceeded_max_mask, 'validation_status'] = 'INVALID'
        df.loc[exceeded_max_mask, 'validation_errors'] += 'EXCEEDED_RESTOCK_MAX; '
        self.stats.exceeded_restock_max_count = exceeded_max_mask.sum()
        self.logger.info(f"  Exceeded restock max: {self.stats.exceeded_restock_max_count} records")
        
        # 4. Check mismatched product_id
        valid_products = set(self.products_master['product_id'].dropna().unique())
        mismatched_mask = ~df['product_id'].isin(valid_products) & df['product_id'].notna()
        df.loc[mismatched_mask, 'validation_status'] = 'INVALID'
        df.loc[mismatched_mask, 'validation_errors'] += 'MISMATCHED_PRODUCT_ID; '
        self.logger.info(f"  Mismatched product_id: {mismatched_mask.sum()} records")
        
        # Split valid and invalid
        valid_df = df[df['validation_status'] == 'VALID'].copy()
        invalid_df = df[df['validation_status'] == 'INVALID'].copy()
        
        self.stats.restock_valid = len(valid_df)
        self.stats.restock_invalid = len(invalid_df)
        
        self.logger.info(f"  Valid records: {self.stats.restock_valid}")
        self.logger.info(f"  Invalid records: {self.stats.restock_invalid}")
        
        return valid_df, invalid_df
    
    def _deduplicate_snapshot(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
        """Remove duplicate inventory snapshot entries."""
        key_cols = ['store_id', 'product_id', 'snapshot_date']
        initial_count = len(df)
        
        # Sort by snapshot_id to keep the latest
        df = df.sort_values('snapshot_id', ascending=False)
        df = df.drop_duplicates(subset=key_cols, keep='first')
        
        duplicates_removed = initial_count - len(df)
        return df, duplicates_removed
    
    # =========================================================================
    # FUZZY MATCHING ENGINE - Auto-identify missing product_ids
    # =========================================================================
    
    def apply_fuzzy_matching(self) -> None:
        """
        Apply fuzzy matching to identify and correct mismatched product_ids.
        
        Uses:
        - Levenshtein distance for product_id similarity
        - Token sort ratio for product_name similarity
        - Regex-based SKU validation
        """
        if not FUZZY_AVAILABLE:
            self.logger.warning("Fuzzy matching skipped - fuzzywuzzy not installed")
            return
        
        if not self.config['fuzzy_matching']['enabled']:
            self.logger.info("Fuzzy matching disabled in configuration")
            return
        
        self.logger.info("-" * 50)
        self.logger.info("Applying Fuzzy Matching for Product ID Correction...")
        self.logger.info("-" * 50)
        
        # Get list of valid product_ids
        valid_product_ids = list(self.products_master['product_id'].dropna().unique())
        threshold = self.config['fuzzy_matching']['product_id_matching']['threshold']
        
        # Find invalid product_ids in quarantine
        for dataset_name, quarantine_df in self.quarantine_data.items():
            if quarantine_df.empty:
                continue
            
            # Find records with mismatched product_id
            mismatched_mask = quarantine_df['validation_errors'].str.contains('MISMATCHED_PRODUCT_ID', na=False)
            mismatched_df = quarantine_df[mismatched_mask].copy()
            
            if mismatched_df.empty:
                continue
            
            self.logger.info(f"  Attempting fuzzy match for {len(mismatched_df)} records in {dataset_name}")
            
            # Apply fuzzy matching
            for idx, row in mismatched_df.iterrows():
                invalid_product_id = row['product_id']
                
                if pd.isna(invalid_product_id):
                    continue
                
                # Find best match
                best_match, score = process.extractOne(
                    str(invalid_product_id),
                    valid_product_ids,
                    scorer=fuzz.ratio
                )
                
                if score >= threshold:
                    self.logger.info(f"    Fuzzy match: '{invalid_product_id}' → '{best_match}' (score: {score})")
                    
                    # Update the record
                    quarantine_df.loc[idx, 'suggested_product_id'] = best_match
                    quarantine_df.loc[idx, 'fuzzy_match_score'] = score
                    self.stats.fuzzy_matches_found += 1
                else:
                    quarantine_df.loc[idx, 'suggested_product_id'] = None
                    quarantine_df.loc[idx, 'fuzzy_match_score'] = score
                    self.stats.unmatched_products += 1
            
            self.quarantine_data[dataset_name] = quarantine_df
        
        self.logger.info(f"  Fuzzy matches found: {self.stats.fuzzy_matches_found}")
        self.logger.info(f"  Unmatched products: {self.stats.unmatched_products}")
        
        # SKU Validation
        self._validate_skus()
    
    def _validate_skus(self) -> None:
        """Validate SKU format using regex pattern."""
        sku_config = self.config['fuzzy_matching']['sku_validation']
        
        if not sku_config['enabled']:
            return
        
        pattern = sku_config['pattern']
        self.logger.info(f"  Validating SKU format with pattern: {pattern}")
        
        for name, df in self.staging_data.items():
            if 'product_id' in df.columns:
                # Check SKU format (product_id should match pattern)
                df['sku_valid'] = df['product_id'].apply(
                    lambda x: bool(re.match(pattern, str(x))) if pd.notna(x) else False
                )
                invalid_sku_count = (~df['sku_valid']).sum()
                self.logger.info(f"    {name}: {invalid_sku_count} non-standard SKUs")
    
    # =========================================================================
    # LAYER 3: RECONCILIATION - Compute Effective Stock Level
    # =========================================================================
    
    def reconcile_inventory(self) -> pd.DataFrame:
        """
        Reconcile inventory by computing effective stock level.
        
        Formula: effective_stock_level = snapshot_level + incoming_restock 
                                         - damaged_quantity - expired_quantity
        """
        self.logger.info("=" * 70)
        self.logger.info("LAYER 3: RECONCILIATION - Computing Effective Stock Level")
        self.logger.info("=" * 70)
        
        inventory_df = self.staging_data['inventory_snapshot'].copy()
        restock_df = self.staging_data['restock_events'].copy()
        
        # Aggregate restock events by store_id, product_id, date
        restock_agg = restock_df.groupby(
            ['store_id', 'product_id', 'restock_date']
        )['incoming_quantity'].sum().reset_index()
        restock_agg.columns = ['store_id', 'product_id', 'snapshot_date', 'incoming_restock']
        
        self.logger.info(f"  Aggregated restock events: {len(restock_agg)} unique combinations")
        
        # Merge inventory with restock
        merged_df = inventory_df.merge(
            restock_agg,
            on=['store_id', 'product_id', 'snapshot_date'],
            how='left'
        )
        
        # Fill missing values
        merged_df['incoming_restock'] = merged_df['incoming_restock'].fillna(0)
        merged_df['damaged_quantity'] = merged_df['damaged_quantity'].fillna(0)
        merged_df['expired_quantity'] = merged_df['expired_quantity'].fillna(0)
        
        # Compute effective stock level
        # Formula: effective_stock = snapshot_level + incoming_restock - damaged - expired
        merged_df['effective_stock_level'] = (
            merged_df['snapshot_level'] 
            + merged_df['incoming_restock']
            - merged_df['damaged_quantity']
            - merged_df['expired_quantity']
        )
        
        self.logger.info(f"  Formula: effective_stock = snapshot + restock - damaged - expired")
        
        # Add reconciliation flags
        merged_df['reconciliation_status'] = 'RECONCILED'
        merged_df['negative_effective_stock_flag'] = merged_df['effective_stock_level'] < 0
        
        # Flag negative effective stock
        neg_count = merged_df['negative_effective_stock_flag'].sum()
        if neg_count > 0:
            self.logger.warning(f"  Warning: {neg_count} records have negative effective stock")
            merged_df.loc[merged_df['negative_effective_stock_flag'], 'reconciliation_status'] = 'FLAGGED_NEGATIVE'
        
        # Calculate stock movement
        merged_df['stock_movement'] = (
            merged_df['incoming_restock'] 
            - merged_df['damaged_quantity'] 
            - merged_df['expired_quantity']
        )
        
        # Add metadata
        merged_df['reconciled_at'] = datetime.now()
        
        self.logger.info(f"  Total reconciled records: {len(merged_df)}")
        self.logger.info(f"  Average effective stock: {merged_df['effective_stock_level'].mean():.2f}")
        self.logger.info(f"  Total stock movement: {merged_df['stock_movement'].sum():.0f}")
        
        return merged_df
    
    # =========================================================================
    # LAYER 4: CURATED (GOLD) - Final Inventory Fact Table
    # =========================================================================
    
    def create_curated_layer(self, reconciled_df: pd.DataFrame) -> pd.DataFrame:
        """
        Create the final curated inventory fact table.
        This is the SINGLE SOURCE OF TRUTH for inventory data.
        """
        self.logger.info("=" * 70)
        self.logger.info("LAYER 4: CURATED (GOLD) - Inventory Fact Table")
        self.logger.info("=" * 70)
        
        # Select final columns for fact table
        fact_columns = [
            'snapshot_id',
            'store_id',
            'product_id',
            'snapshot_date',
            'snapshot_level',
            'incoming_restock',
            'damaged_quantity',
            'expired_quantity',
            'effective_stock_level',
            'stock_movement',
            'max_capacity',
            'reconciliation_status',
            'negative_effective_stock_flag',
            'reconciled_at'
        ]
        
        # Keep only available columns
        available_cols = [col for col in fact_columns if col in reconciled_df.columns]
        curated_df = reconciled_df[available_cols].copy()
        
        # Enrich with product information
        curated_df = curated_df.merge(
            self.products_master[['product_id', 'product_name', 'category', 'unit_price']],
            on='product_id',
            how='left'
        )
        
        # Enrich with store information
        curated_df = curated_df.merge(
            self.stores_master[['store_id', 'store_name', 'city', 'region']],
            on='store_id',
            how='left'
        )
        
        # Add pipeline metadata
        curated_df['pipeline_version'] = self.config['pipeline']['version']
        curated_df['created_at'] = datetime.now()
        
        # Save to curated layer
        curated_path = self.config['output']['curated']['inventory_fact_table']
        os.makedirs(curated_path, exist_ok=True)
        
        # Save as Parquet
        parquet_path = f"{curated_path}/inventory_fact.parquet"
        curated_df.to_parquet(parquet_path, index=False)
        self.logger.info(f"  Saved to: {parquet_path}")
        
        # Also save as CSV if configured
        if self.config['output']['curated'].get('also_save_csv', True):
            csv_path = f"{curated_path}/inventory_fact.csv"
            curated_df.to_csv(csv_path, index=False)
            self.logger.info(f"  Also saved as CSV: {csv_path}")
        
        self.stats.curated_records = len(curated_df)
        self.curated_data = curated_df
        
        self.logger.info(f"  Total records in fact table: {self.stats.curated_records}")
        
        return curated_df
    
    # =========================================================================
    # QUARANTINE LAYER - Invalid Records for Diagnostics
    # =========================================================================
    
    def save_quarantine_layer(self) -> None:
        """
        Save invalid records to quarantine for later diagnostics.
        """
        self.logger.info("=" * 70)
        self.logger.info("QUARANTINE LAYER - Invalid Records for Diagnostics")
        self.logger.info("=" * 70)
        
        total_quarantine = 0
        
        for name, invalid_df in self.quarantine_data.items():
            if invalid_df.empty:
                self.logger.info(f"  {name}: No invalid records")
                continue
            
            # Add audit metadata
            invalid_df = invalid_df.copy()
            invalid_df['quarantine_date'] = datetime.now()
            invalid_df['source_dataset'] = name
            
            # Classify error types
            invalid_df['error_type'] = invalid_df['validation_errors'].apply(
                self._classify_error_type
            )
            
            # Save to quarantine
            quarantine_path = f"quarantine/quarantine_{name}"
            os.makedirs(quarantine_path, exist_ok=True)
            
            # Save as Parquet
            parquet_path = f"{quarantine_path}/quarantine_{name}.parquet"
            invalid_df.to_parquet(parquet_path, index=False)
            
            # Save as CSV
            csv_path = f"{quarantine_path}/quarantine_{name}.csv"
            invalid_df.to_csv(csv_path, index=False)
            
            self.logger.info(f"  {name}: {len(invalid_df)} invalid records saved to {quarantine_path}")
            total_quarantine += len(invalid_df)
        
        self.stats.quarantine_records = total_quarantine
    
    def _classify_error_type(self, error_string: str) -> str:
        """Classify error type based on validation error string."""
        if pd.isna(error_string) or error_string == '':
            return 'UNKNOWN'
        
        error_types = []
        if 'MISSING_' in error_string:
            error_types.append('MISSING_FIELD')
        if 'NEGATIVE_STOCK' in error_string:
            error_types.append('NEGATIVE_STOCK')
        if 'MISMATCHED_PRODUCT_ID' in error_string:
            error_types.append('MISMATCHED_PRODUCT')
        if 'EXCEEDED_' in error_string:
            error_types.append('EXCEEDED_LIMIT')
        if 'INVALID_' in error_string:
            error_types.append('INVALID_VALUE')
        
        return '|'.join(error_types) if error_types else 'OTHER'
    
    # =========================================================================
    # PIPELINE EXECUTION
    # =========================================================================
    
    def run(self) -> Dict[str, Any]:
        """
        Execute the complete pipeline.
        
        Returns:
            Dictionary with pipeline execution results and statistics.
        """
        self.logger.info("*" * 70)
        self.logger.info("STARTING: UNIFIED PRODUCT & INVENTORY DATA HARMONIZATION PIPELINE")
        self.logger.info("*" * 70)
        
        start_time = datetime.now()
        
        try:
            # Step 1: Ingest RAW data
            self.ingest_raw_layer()
            
            # Step 2: Validate and create STAGING layer
            self.validate_staging_layer()
            
            # Step 3: Apply fuzzy matching for product ID correction
            self.apply_fuzzy_matching()
            
            # Step 4: Reconcile inventory
            reconciled_df = self.reconcile_inventory()
            
            # Step 5: Create CURATED layer
            self.create_curated_layer(reconciled_df)
            
            # Step 6: Save QUARANTINE layer
            self.save_quarantine_layer()
            
            # Calculate execution time
            end_time = datetime.now()
            self.stats.execution_time = (end_time - start_time).total_seconds()
            
            # Generate report
            self._generate_report()
            
            self.logger.info("*" * 70)
            self.logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
            self.logger.info("*" * 70)
            
            return {
                'status': 'SUCCESS',
                'stats': self.stats.__dict__,
                'curated_path': self.config['output']['curated']['inventory_fact_table'],
                'quarantine_path': 'quarantine/'
            }
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return {
                'status': 'FAILED',
                'error': str(e)
            }
    
    def _generate_report(self) -> None:
        """Generate and save execution report."""
        report = f"""
================================================================================
       INVENTORY HARMONIZATION PIPELINE - EXECUTION REPORT
================================================================================

Pipeline: {self.config['pipeline']['name']}
Version:  {self.config['pipeline']['version']}
Executed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

--------------------------------------------------------------------------------
                         INPUT DATA STATISTICS
--------------------------------------------------------------------------------

Inventory Snapshots:
  - Total Records:    {self.stats.snapshot_total:,}
  - Valid Records:    {self.stats.snapshot_valid:,}
  - Invalid Records:  {self.stats.snapshot_invalid:,}
  - Duplicates:       {self.stats.snapshot_duplicates:,}

Restock Events:
  - Total Records:    {self.stats.restock_total:,}
  - Valid Records:    {self.stats.restock_valid:,}
  - Invalid Records:  {self.stats.restock_invalid:,}

--------------------------------------------------------------------------------
                         VALIDATION STATISTICS
--------------------------------------------------------------------------------

  - Negative Stock:           {self.stats.negative_stock_count:,}
  - Mismatched Product IDs:   {self.stats.mismatched_product_count:,}
  - Exceeded Capacity:        {self.stats.exceeded_capacity_count:,}
  - Exceeded Restock Max:     {self.stats.exceeded_restock_max_count:,}

--------------------------------------------------------------------------------
                         FUZZY MATCHING RESULTS
--------------------------------------------------------------------------------

  - Fuzzy Matches Found:      {self.stats.fuzzy_matches_found:,}
  - Unmatched Products:       {self.stats.unmatched_products:,}

--------------------------------------------------------------------------------
                         OUTPUT STATISTICS
--------------------------------------------------------------------------------

  - Curated Records:          {self.stats.curated_records:,}
  - Quarantine Records:       {self.stats.quarantine_records:,}

--------------------------------------------------------------------------------
                         EXECUTION TIME
--------------------------------------------------------------------------------

  Total Execution Time:       {self.stats.execution_time:.2f} seconds

================================================================================
                              OUTPUT LOCATIONS
================================================================================

  Curated (Inventory Fact):   curated/inventory_fact/
  Quarantine (Invalid):       quarantine/

================================================================================
"""
        
        # Save report
        os.makedirs('curated', exist_ok=True)
        report_path = "curated/pipeline_execution_report.txt"
        with open(report_path, 'w') as f:
            f.write(report)
        
        self.logger.info(f"Report saved to: {report_path}")
        print(report)


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main entry point."""
    print("\n" + "=" * 70)
    print("    TASK 1: UNIFIED PRODUCT & INVENTORY DATA HARMONIZATION PIPELINE")
    print("=" * 70 + "\n")
    
    # Initialize and run pipeline
    pipeline = InventoryHarmonizationPipeline(config_path="config/task1_config.yml")
    result = pipeline.run()
    
    if result['status'] == 'SUCCESS':
        print("\n✅ Pipeline completed successfully!")
        print(f"   Curated data: {result['curated_path']}")
        print(f"   Quarantine data: {result['quarantine_path']}")
    else:
        print(f"\n❌ Pipeline failed: {result.get('error', 'Unknown error')}")


if __name__ == "__main__":
    main()

