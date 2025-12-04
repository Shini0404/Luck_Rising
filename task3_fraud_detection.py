"""
================================================================================
TASK 3: REFUND & FRAUD DETECTION ENGINE
================================================================================

Goal: Automatically detect suspicious refund activities using sales, refund logs,
      and customer behaviors.

Detection Methods:
    - Rule-based validation (amount, date, customer match)
    - Behavioral analysis (frequency, patterns)
    - Statistical anomaly detection (Z-score, IQR)

Output: fraud_flags table marking suspicious refunds

Author: Hackathon Team
Version: 1.0
================================================================================
"""

import pandas as pd
import numpy as np
import yaml
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field


# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging(config: Dict) -> logging.Logger:
    """Configure logging."""
    log_config = config.get('logging', {})
    log_file = log_config.get('file', 'logs/task3_pipeline.log')
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    logger = logging.getLogger('FraudDetection')
    logger.setLevel(logging.INFO)
    logger.handlers = []
    
    file_handler = logging.FileHandler(log_file)
    console_handler = logging.StreamHandler()
    
    formatter = logging.Formatter(log_config.get('format', '%(asctime)s | %(levelname)-8s | %(message)s'))
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


# =============================================================================
# STATISTICS TRACKING
# =============================================================================

@dataclass
class FraudStats:
    """Track pipeline statistics."""
    total_refunds: int = 0
    valid_refunds: int = 0
    invalid_refunds: int = 0
    
    # Fraud flags counts
    amount_exceeded_flags: int = 0
    customer_mismatch_flags: int = 0
    outside_window_flags: int = 0
    payment_mismatch_flags: int = 0
    high_frequency_flags: int = 0
    high_value_flags: int = 0
    repeat_product_flags: int = 0
    zscore_anomaly_flags: int = 0
    iqr_anomaly_flags: int = 0
    
    total_fraud_flags: int = 0
    suspicious_customers: int = 0
    clean_refunds: int = 0
    execution_time: float = 0.0


# =============================================================================
# MAIN PIPELINE CLASS
# =============================================================================

class FraudDetectionEngine:
    """
    Refund & Fraud Detection Engine.
    
    Detects suspicious refund activities using:
    1. Rule-based validation
    2. Behavioral pattern analysis
    3. Statistical anomaly detection (Z-score, IQR)
    """
    
    def __init__(self, config_path: str = "config/task3_config.yml"):
        """Initialize the engine."""
        self.config = self._load_config(config_path)
        self.logger = setup_logging(self.config)
        self.stats = FraudStats()
        
        # Data storage
        self.refunds: Optional[pd.DataFrame] = None
        self.sales_header: Optional[pd.DataFrame] = None
        self.sales_line_items: Optional[pd.DataFrame] = None
        self.customers: Optional[pd.DataFrame] = None
        self.products: Optional[pd.DataFrame] = None
        
        # Processed data
        self.enriched_refunds: Optional[pd.DataFrame] = None
        self.fraud_flags: Optional[pd.DataFrame] = None
        self.suspicious_customers_df: Optional[pd.DataFrame] = None
        self.clean_refunds_df: Optional[pd.DataFrame] = None
    
    def _load_config(self, config_path: str) -> Dict:
        """Load YAML configuration."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    # =========================================================================
    # LAYER 1: RAW - Data Ingestion
    # =========================================================================
    
    def ingest_data(self) -> None:
        """Load all required data sources."""
        self.logger.info("=" * 70)
        self.logger.info("LAYER 1: RAW - Data Ingestion")
        self.logger.info("=" * 70)
        
        # Load refund transactions
        refund_path = self.config['data_sources']['refund_transactions']['file_path']
        self.refunds = pd.read_csv(refund_path)
        self.stats.total_refunds = len(self.refunds)
        self.logger.info(f"Loaded refunds: {self.stats.total_refunds} records")
        
        # Load sales header (from Task 2)
        header_path = self.config['data_sources']['store_sales_header']['file_path']
        self.sales_header = pd.read_csv(header_path)
        self.logger.info(f"Loaded sales header: {len(self.sales_header)} transactions")
        
        # Load sales line items
        line_items_path = self.config['data_sources']['store_sales_line_items']['file_path']
        self.sales_line_items = pd.read_csv(line_items_path)
        self.logger.info(f"Loaded line items: {len(self.sales_line_items)} items")
        
        # Load customer details
        customer_path = self.config['data_sources']['customer_details']['file_path']
        self.customers = pd.read_csv(customer_path)
        self.logger.info(f"Loaded customers: {len(self.customers)} records")
        
        # Load products (from Task 1)
        products_path = self.config['data_sources']['products']['file_path']
        self.products = pd.read_csv(products_path)
        self.logger.info(f"Loaded products: {len(self.products)} records")
        
        # Convert date columns
        self.refunds['refund_date'] = pd.to_datetime(self.refunds['refund_date'])
        self.sales_header['sale_date'] = pd.to_datetime(self.sales_header['sale_date'])
    
    # =========================================================================
    # LAYER 2: STAGING - Data Enrichment & Validation
    # =========================================================================
    
    def enrich_refunds(self) -> pd.DataFrame:
        """
        Enrich refunds with sales and customer data.
        Join refunds with original transaction details.
        """
        self.logger.info("=" * 70)
        self.logger.info("LAYER 2: STAGING - Data Enrichment")
        self.logger.info("=" * 70)
        
        df = self.refunds.copy()
        
        # Join with sales header to get original transaction details
        df = df.merge(
            self.sales_header[['transaction_id', 'customer_id', 'sale_date', 'total_amount', 'payment_mode']],
            left_on='original_transaction_id',
            right_on='transaction_id',
            how='left',
            suffixes=('', '_original')
        )
        
        # Join with customer details
        df = df.merge(
            self.customers[['customer_id', 'customer_name', 'customer_tier', 'total_purchases', 'total_refunds', 'account_status']],
            on='customer_id',
            how='left'
        )
        
        # Join with products
        df = df.merge(
            self.products[['product_id', 'product_name', 'category']],
            on='product_id',
            how='left'
        )
        
        # Calculate derived fields
        df['refund_ratio'] = df['refund_amount'] / df['original_amount']
        df['days_since_purchase'] = (df['refund_date'] - df['sale_date']).dt.days
        
        # Fill NaN for unmatched transactions
        df['days_since_purchase'] = df['days_since_purchase'].fillna(999)  # Large number for unmatched
        
        self.logger.info(f"Enriched refunds: {len(df)} records")
        self.enriched_refunds = df
        
        # Save to staging
        staging_path = self.config['output']['staging']['enriched_refunds']
        os.makedirs(staging_path, exist_ok=True)
        df.to_parquet(f"{staging_path}/enriched_refunds.parquet", index=False)
        
        return df
    
    def validate_refunds(self) -> pd.DataFrame:
        """
        Validate refunds against business rules.
        
        Checks:
        - Refund amount <= original amount
        - Customer ID matches original transaction
        - Refund within allowed window
        - Payment mode matches
        """
        self.logger.info("-" * 50)
        self.logger.info("Validating Refunds...")
        self.logger.info("-" * 50)
        
        df = self.enriched_refunds.copy()
        validation_config = self.config['validation']
        
        # Initialize validation columns
        df['validation_status'] = 'VALID'
        df['validation_errors'] = ''
        
        # Check 1: Refund amount exceeds original
        max_ratio = validation_config['max_refund_ratio']
        exceeded_mask = df['refund_ratio'] > max_ratio
        df.loc[exceeded_mask, 'validation_status'] = 'INVALID'
        df.loc[exceeded_mask, 'validation_errors'] += 'AMOUNT_EXCEEDED; '
        self.stats.amount_exceeded_flags = exceeded_mask.sum()
        self.logger.info(f"  Amount exceeded flags: {self.stats.amount_exceeded_flags}")
        
        # Check 2: Customer ID mismatch
        if validation_config['require_customer_match']:
            mismatch_mask = df['customer_id'] != df['customer_id_original']
            # Handle NaN (unmatched transactions)
            mismatch_mask = mismatch_mask.fillna(False)
            df.loc[mismatch_mask, 'validation_status'] = 'INVALID'
            df.loc[mismatch_mask, 'validation_errors'] += 'CUSTOMER_MISMATCH; '
            self.stats.customer_mismatch_flags = mismatch_mask.sum()
            self.logger.info(f"  Customer mismatch flags: {self.stats.customer_mismatch_flags}")
        
        # Check 3: Refund outside allowed window
        allowed_days = validation_config['allowed_refund_window_days']
        outside_mask = df['days_since_purchase'] > allowed_days
        df.loc[outside_mask, 'validation_status'] = 'INVALID'
        df.loc[outside_mask, 'validation_errors'] += 'OUTSIDE_WINDOW; '
        self.stats.outside_window_flags = outside_mask.sum()
        self.logger.info(f"  Outside window flags: {self.stats.outside_window_flags}")
        
        # Check 4: Payment mode mismatch
        if validation_config['require_payment_mode_match']:
            payment_mismatch = df['payment_mode'] != df['original_payment_mode']
            df.loc[payment_mismatch, 'validation_status'] = 'INVALID'
            df.loc[payment_mismatch, 'validation_errors'] += 'PAYMENT_MISMATCH; '
            self.stats.payment_mismatch_flags = payment_mismatch.sum()
            self.logger.info(f"  Payment mismatch flags: {self.stats.payment_mismatch_flags}")
        
        # Update stats
        self.stats.valid_refunds = (df['validation_status'] == 'VALID').sum()
        self.stats.invalid_refunds = (df['validation_status'] == 'INVALID').sum()
        
        self.enriched_refunds = df
        return df
    
    # =========================================================================
    # LAYER 3: FRAUD DETECTION ENGINE
    # =========================================================================
    
    def detect_fraud_patterns(self) -> pd.DataFrame:
        """
        Apply fraud detection rules to identify suspicious patterns.
        
        Detects:
        - High-Frequency Refunds
        - High-Value Frequent Refunds
        - Same product refunded repeatedly
        """
        self.logger.info("=" * 70)
        self.logger.info("LAYER 3: FRAUD DETECTION - Pattern Analysis")
        self.logger.info("=" * 70)
        
        df = self.enriched_refunds.copy()
        fraud_config = self.config['fraud_detection']
        
        # Initialize fraud flag columns
        df['fraud_flags'] = ''
        df['fraud_score'] = 0
        df['fraud_severity'] = 'NONE'
        
        # === RULE 1: High-Frequency Refunds ===
        self.logger.info("-" * 50)
        self.logger.info("Rule 1: High-Frequency Refunds")
        hf_config = fraud_config['high_frequency']
        
        # Count refunds per customer in window
        window_days = hf_config['window_days']
        max_refunds = hf_config['max_refunds_per_window']
        
        # Calculate refund count per customer
        customer_refund_counts = df.groupby('customer_id').size().reset_index(name='refund_count')
        df = df.merge(customer_refund_counts, on='customer_id', how='left')
        
        high_freq_mask = df['refund_count'] > max_refunds
        df.loc[high_freq_mask, 'fraud_flags'] += 'HIGH_FREQUENCY; '
        df.loc[high_freq_mask, 'fraud_score'] += 30
        self.stats.high_frequency_flags = high_freq_mask.sum()
        self.logger.info(f"  High-frequency refund flags: {self.stats.high_frequency_flags}")
        
        # === RULE 2: High-Value Refunds ===
        self.logger.info("-" * 50)
        self.logger.info("Rule 2: High-Value Refunds")
        hv_config = fraud_config['high_value']
        
        high_value_mask = df['refund_amount'] > hv_config['amount_threshold']
        df.loc[high_value_mask, 'fraud_flags'] += 'HIGH_VALUE; '
        df.loc[high_value_mask, 'fraud_score'] += 20
        self.stats.high_value_flags = high_value_mask.sum()
        self.logger.info(f"  High-value refund flags: {self.stats.high_value_flags}")
        
        # === RULE 3: Same Product Refunded Repeatedly ===
        self.logger.info("-" * 50)
        self.logger.info("Rule 3: Repeat Product Refunds")
        rp_config = fraud_config['repeat_product']
        
        # Count refunds per customer-product combination
        product_refund_counts = df.groupby(['customer_id', 'product_id']).size().reset_index(name='product_refund_count')
        df = df.merge(product_refund_counts, on=['customer_id', 'product_id'], how='left')
        
        repeat_mask = df['product_refund_count'] > rp_config['max_same_product_refunds']
        df.loc[repeat_mask, 'fraud_flags'] += 'REPEAT_PRODUCT; '
        df.loc[repeat_mask, 'fraud_score'] += 25
        self.stats.repeat_product_flags = repeat_mask.sum()
        self.logger.info(f"  Repeat product refund flags: {self.stats.repeat_product_flags}")
        
        # === RULE 4: Amount Exceeded Original ===
        self.logger.info("-" * 50)
        self.logger.info("Rule 4: Amount Exceeded Original")
        if fraud_config['amount_exceeded']['enabled']:
            exceeded_mask = df['refund_ratio'] > 1.0
            df.loc[exceeded_mask, 'fraud_flags'] += 'AMOUNT_EXCEEDED; '
            df.loc[exceeded_mask, 'fraud_score'] += 50  # Critical
        
        # === RULE 5: Payment Mode Mismatch ===
        self.logger.info("-" * 50)
        self.logger.info("Rule 5: Payment Mode Mismatch")
        if fraud_config['payment_mismatch']['enabled']:
            payment_mismatch = df['payment_mode'] != df['original_payment_mode']
            df.loc[payment_mismatch, 'fraud_flags'] += 'PAYMENT_MISMATCH; '
            df.loc[payment_mismatch, 'fraud_score'] += 15
        
        self.enriched_refunds = df
        return df
    
    def detect_statistical_anomalies(self) -> pd.DataFrame:
        """
        Apply statistical anomaly detection.
        
        Methods:
        - Z-Score: Flag if |z| > threshold
        - IQR: Flag if outside Q1-1.5*IQR to Q3+1.5*IQR
        """
        self.logger.info("=" * 70)
        self.logger.info("LAYER 3: FRAUD DETECTION - Statistical Anomalies")
        self.logger.info("=" * 70)
        
        df = self.enriched_refunds.copy()
        anomaly_config = self.config['anomaly_detection']
        
        if not anomaly_config['enabled']:
            self.logger.info("Statistical anomaly detection disabled")
            return df
        
        # === Z-SCORE DETECTION ===
        if anomaly_config['zscore']['enabled']:
            self.logger.info("-" * 50)
            self.logger.info("Z-Score Anomaly Detection")
            threshold = anomaly_config['zscore']['threshold']
            
            # Calculate Z-score for refund amounts
            mean_amount = df['refund_amount'].mean()
            std_amount = df['refund_amount'].std()
            
            if std_amount > 0:
                df['zscore_amount'] = (df['refund_amount'] - mean_amount) / std_amount
                zscore_mask = df['zscore_amount'].abs() > threshold
                df.loc[zscore_mask, 'fraud_flags'] += 'ZSCORE_ANOMALY; '
                df.loc[zscore_mask, 'fraud_score'] += 20
                self.stats.zscore_anomaly_flags = zscore_mask.sum()
                self.logger.info(f"  Z-score anomaly flags: {self.stats.zscore_anomaly_flags}")
                self.logger.info(f"  Mean refund: ${mean_amount:.2f}, Std: ${std_amount:.2f}")
            else:
                df['zscore_amount'] = 0
        
        # === IQR DETECTION ===
        if anomaly_config['iqr']['enabled']:
            self.logger.info("-" * 50)
            self.logger.info("IQR Anomaly Detection")
            multiplier = anomaly_config['iqr']['multiplier']
            
            Q1 = df['refund_amount'].quantile(0.25)
            Q3 = df['refund_amount'].quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - multiplier * IQR
            upper_bound = Q3 + multiplier * IQR
            
            iqr_mask = (df['refund_amount'] < lower_bound) | (df['refund_amount'] > upper_bound)
            df.loc[iqr_mask, 'fraud_flags'] += 'IQR_ANOMALY; '
            df.loc[iqr_mask, 'fraud_score'] += 15
            self.stats.iqr_anomaly_flags = iqr_mask.sum()
            self.logger.info(f"  IQR anomaly flags: {self.stats.iqr_anomaly_flags}")
            self.logger.info(f"  Q1: ${Q1:.2f}, Q3: ${Q3:.2f}, IQR: ${IQR:.2f}")
            self.logger.info(f"  Bounds: ${lower_bound:.2f} to ${upper_bound:.2f}")
        
        # === REFUND RATIO DETECTION ===
        if anomaly_config['refund_ratio']['enabled']:
            self.logger.info("-" * 50)
            self.logger.info("Historical Refund Ratio Analysis")
            max_ratio = anomaly_config['refund_ratio']['max_ratio']
            
            # Calculate customer refund ratio
            customer_stats = df.groupby('customer_id').agg({
                'refund_amount': 'sum',
                'total_purchases': 'first'
            }).reset_index()
            customer_stats['customer_refund_ratio'] = customer_stats['refund_amount'] / customer_stats['total_purchases']
            customer_stats['customer_refund_ratio'] = customer_stats['customer_refund_ratio'].fillna(0)
            
            df = df.merge(customer_stats[['customer_id', 'customer_refund_ratio']], on='customer_id', how='left')
            
            ratio_mask = df['customer_refund_ratio'] > max_ratio
            df.loc[ratio_mask, 'fraud_flags'] += 'HIGH_REFUND_RATIO; '
            df.loc[ratio_mask, 'fraud_score'] += 25
            high_ratio_count = ratio_mask.sum()
            self.logger.info(f"  High refund ratio flags: {high_ratio_count}")
        
        self.enriched_refunds = df
        return df
    
    def calculate_severity(self) -> pd.DataFrame:
        """Calculate overall fraud severity based on score."""
        self.logger.info("-" * 50)
        self.logger.info("Calculating Fraud Severity...")
        
        df = self.enriched_refunds.copy()
        
        # Assign severity based on fraud score
        df['fraud_severity'] = pd.cut(
            df['fraud_score'],
            bins=[-1, 0, 30, 60, float('inf')],
            labels=['CLEAN', 'LOW', 'MEDIUM', 'HIGH']
        )
        
        # Count fraud flags
        self.stats.total_fraud_flags = (df['fraud_score'] > 0).sum()
        self.stats.clean_refunds = (df['fraud_severity'] == 'CLEAN').sum()
        
        self.logger.info(f"  CLEAN: {(df['fraud_severity'] == 'CLEAN').sum()}")
        self.logger.info(f"  LOW: {(df['fraud_severity'] == 'LOW').sum()}")
        self.logger.info(f"  MEDIUM: {(df['fraud_severity'] == 'MEDIUM').sum()}")
        self.logger.info(f"  HIGH: {(df['fraud_severity'] == 'HIGH').sum()}")
        
        self.enriched_refunds = df
        return df
    
    # =========================================================================
    # LAYER 4: CURATED - Output Tables
    # =========================================================================
    
    def create_fraud_flags_table(self) -> pd.DataFrame:
        """
        Create the fraud_flags table with all suspicious refunds.
        """
        self.logger.info("=" * 70)
        self.logger.info("LAYER 4: CURATED - Creating Fraud Flags Table")
        self.logger.info("=" * 70)
        
        df = self.enriched_refunds.copy()
        
        # Select columns for fraud flags table
        fraud_columns = [
            'refund_id',
            'original_transaction_id',
            'customer_id',
            'customer_name',
            'refund_date',
            'refund_amount',
            'original_amount',
            'refund_ratio',
            'product_id',
            'product_name',
            'payment_mode',
            'original_payment_mode',
            'days_since_purchase',
            'refund_count',
            'product_refund_count',
            'validation_status',
            'validation_errors',
            'fraud_flags',
            'fraud_score',
            'fraud_severity'
        ]
        
        # Keep only available columns
        available_cols = [c for c in fraud_columns if c in df.columns]
        fraud_df = df[available_cols].copy()
        
        # Add metadata
        fraud_df['detection_timestamp'] = datetime.now()
        fraud_df['pipeline_version'] = self.config['pipeline']['version']
        
        # Sort by fraud score (highest first)
        fraud_df = fraud_df.sort_values('fraud_score', ascending=False)
        
        self.fraud_flags = fraud_df
        
        # Save to curated
        output_path = self.config['output']['curated']['fraud_flags']
        os.makedirs(output_path, exist_ok=True)
        
        fraud_df.to_parquet(f"{output_path}/fraud_flags.parquet", index=False)
        if self.config['output']['curated'].get('also_save_csv', True):
            fraud_df.to_csv(f"{output_path}/fraud_flags.csv", index=False)
        
        self.logger.info(f"Saved fraud flags: {len(fraud_df)} records")
        
        return fraud_df
    
    def create_suspicious_customers_table(self) -> pd.DataFrame:
        """
        Create summary table of suspicious customers.
        """
        self.logger.info("-" * 50)
        self.logger.info("Creating Suspicious Customers Table...")
        
        df = self.enriched_refunds.copy()
        
        # Aggregate by customer
        customer_summary = df.groupby('customer_id').agg({
            'customer_name': 'first',
            'customer_tier': 'first',
            'account_status': 'first',
            'refund_id': 'count',
            'refund_amount': 'sum',
            'fraud_score': 'sum',
            'fraud_flags': lambda x: '; '.join(set([f for flags in x for f in flags.split('; ') if f]))
        }).reset_index()
        
        customer_summary.columns = [
            'customer_id', 'customer_name', 'customer_tier', 'account_status',
            'total_refunds_count', 'total_refund_amount', 'total_fraud_score', 'all_fraud_flags'
        ]
        
        # Filter suspicious customers (fraud_score > 0)
        suspicious = customer_summary[customer_summary['total_fraud_score'] > 0].copy()
        suspicious = suspicious.sort_values('total_fraud_score', ascending=False)
        
        # Add risk level
        suspicious['risk_level'] = pd.cut(
            suspicious['total_fraud_score'],
            bins=[-1, 30, 60, 100, float('inf')],
            labels=['LOW_RISK', 'MEDIUM_RISK', 'HIGH_RISK', 'CRITICAL_RISK']
        )
        
        self.stats.suspicious_customers = len(suspicious)
        self.suspicious_customers_df = suspicious
        
        # Save
        output_path = self.config['output']['curated']['suspicious_customers']
        os.makedirs(output_path, exist_ok=True)
        
        suspicious.to_parquet(f"{output_path}/suspicious_customers.parquet", index=False)
        if self.config['output']['curated'].get('also_save_csv', True):
            suspicious.to_csv(f"{output_path}/suspicious_customers.csv", index=False)
        
        self.logger.info(f"Saved suspicious customers: {len(suspicious)} records")
        
        # Log top suspicious customers
        self.logger.info("\nTOP SUSPICIOUS CUSTOMERS:")
        for _, row in suspicious.head(5).iterrows():
            self.logger.info(f"  {row['customer_id']}: {row['customer_name']} - Score: {row['total_fraud_score']}, Flags: {row['all_fraud_flags']}")
        
        return suspicious
    
    def create_clean_refunds_table(self) -> pd.DataFrame:
        """Create table of clean (non-suspicious) refunds."""
        self.logger.info("-" * 50)
        self.logger.info("Creating Clean Refunds Table...")
        
        df = self.enriched_refunds.copy()
        
        # Filter clean refunds
        clean = df[df['fraud_severity'] == 'CLEAN'].copy()
        
        # Select columns
        clean_columns = [
            'refund_id', 'original_transaction_id', 'customer_id',
            'refund_date', 'refund_amount', 'product_id', 'refund_reason'
        ]
        available_cols = [c for c in clean_columns if c in clean.columns]
        clean = clean[available_cols]
        
        self.clean_refunds_df = clean
        
        # Save
        output_path = self.config['output']['curated']['clean_refunds']
        os.makedirs(output_path, exist_ok=True)
        
        clean.to_parquet(f"{output_path}/clean_refunds.parquet", index=False)
        if self.config['output']['curated'].get('also_save_csv', True):
            clean.to_csv(f"{output_path}/clean_refunds.csv", index=False)
        
        self.logger.info(f"Saved clean refunds: {len(clean)} records")
        
        return clean
    
    def generate_report(self) -> str:
        """Generate execution report."""
        report = f"""
================================================================================
              REFUND & FRAUD DETECTION ENGINE - EXECUTION REPORT
================================================================================

Pipeline: {self.config['pipeline']['name']}
Version:  {self.config['pipeline']['version']}
Executed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

--------------------------------------------------------------------------------
                              INPUT STATISTICS
--------------------------------------------------------------------------------

  Total Refunds Processed:     {self.stats.total_refunds:,}
  Valid Refunds:               {self.stats.valid_refunds:,}
  Invalid Refunds:             {self.stats.invalid_refunds:,}

--------------------------------------------------------------------------------
                           VALIDATION FLAGS
--------------------------------------------------------------------------------

  Amount Exceeded Original:    {self.stats.amount_exceeded_flags:,}
  Customer ID Mismatch:        {self.stats.customer_mismatch_flags:,}
  Outside Refund Window:       {self.stats.outside_window_flags:,}
  Payment Mode Mismatch:       {self.stats.payment_mismatch_flags:,}

--------------------------------------------------------------------------------
                         FRAUD DETECTION FLAGS
--------------------------------------------------------------------------------

  High-Frequency Refunds:      {self.stats.high_frequency_flags:,}
  High-Value Refunds:          {self.stats.high_value_flags:,}
  Repeat Product Refunds:      {self.stats.repeat_product_flags:,}
  Z-Score Anomalies:           {self.stats.zscore_anomaly_flags:,}
  IQR Anomalies:               {self.stats.iqr_anomaly_flags:,}

--------------------------------------------------------------------------------
                              SUMMARY
--------------------------------------------------------------------------------

  Total Fraud Flags:           {self.stats.total_fraud_flags:,}
  Suspicious Customers:        {self.stats.suspicious_customers:,}
  Clean Refunds:               {self.stats.clean_refunds:,}

--------------------------------------------------------------------------------
                         OUTPUT LOCATIONS
--------------------------------------------------------------------------------

  Fraud Flags Table:        {self.config['output']['curated']['fraud_flags']}/
  Suspicious Customers:     {self.config['output']['curated']['suspicious_customers']}/
  Clean Refunds:            {self.config['output']['curated']['clean_refunds']}/

--------------------------------------------------------------------------------
                          EXECUTION TIME
--------------------------------------------------------------------------------

  Total Time:               {self.stats.execution_time:.2f} seconds

================================================================================
"""
        return report
    
    # =========================================================================
    # MAIN PIPELINE EXECUTION
    # =========================================================================
    
    def run(self) -> Dict[str, Any]:
        """Execute the complete fraud detection pipeline."""
        self.logger.info("*" * 70)
        self.logger.info("STARTING: REFUND & FRAUD DETECTION ENGINE")
        self.logger.info("*" * 70)
        
        start_time = datetime.now()
        
        try:
            # Step 1: Ingest data
            self.ingest_data()
            
            # Step 2: Enrich refunds
            self.enrich_refunds()
            
            # Step 3: Validate refunds
            self.validate_refunds()
            
            # Step 4: Detect fraud patterns
            self.detect_fraud_patterns()
            
            # Step 5: Detect statistical anomalies
            self.detect_statistical_anomalies()
            
            # Step 6: Calculate severity
            self.calculate_severity()
            
            # Step 7: Create fraud flags table
            self.create_fraud_flags_table()
            
            # Step 8: Create suspicious customers table
            self.create_suspicious_customers_table()
            
            # Step 9: Create clean refunds table
            self.create_clean_refunds_table()
            
            # Calculate execution time
            end_time = datetime.now()
            self.stats.execution_time = (end_time - start_time).total_seconds()
            
            # Generate and save report
            report = self.generate_report()
            
            os.makedirs('curated/task3', exist_ok=True)
            with open('curated/task3/fraud_detection_report.txt', 'w') as f:
                f.write(report)
            
            print(report)
            
            self.logger.info("*" * 70)
            self.logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
            self.logger.info("*" * 70)
            
            return {
                'status': 'SUCCESS',
                'stats': self.stats.__dict__,
                'suspicious_customers': len(self.suspicious_customers_df) if self.suspicious_customers_df is not None else 0
            }
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return {
                'status': 'FAILED',
                'error': str(e)
            }


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main entry point."""
    print("\n" + "=" * 70)
    print("        TASK 3: REFUND & FRAUD DETECTION ENGINE")
    print("=" * 70 + "\n")
    
    # Initialize and run pipeline
    engine = FraudDetectionEngine(config_path="config/task3_config.yml")
    result = engine.run()
    
    if result['status'] == 'SUCCESS':
        print("\n" + "=" * 70)
        print("FRAUD DETECTION SUMMARY:")
        print("=" * 70)
        print(f"  Total Refunds:           {result['stats']['total_refunds']}")
        print(f"  Total Fraud Flags:       {result['stats']['total_fraud_flags']}")
        print(f"  Suspicious Customers:    {result['suspicious_customers']}")
        print(f"  Clean Refunds:           {result['stats']['clean_refunds']}")
        print("=" * 70)
        print("\n Fraud Detection Pipeline completed successfully!")
    else:
        print(f"\n Pipeline failed: {result.get('error', 'Unknown error')}")


if __name__ == "__main__":
    main()

