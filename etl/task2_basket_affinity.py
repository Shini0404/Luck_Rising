"""
================================================================================
TASK 2: REAL-TIME SHOPPING BASKET AFFINITY ANALYZER
================================================================================

Goal: Identify which products are commonly purchased together and compute
      association strengths for recommendations.

Key Metrics:
    - Support: P(A and B) = transactions with both A and B / total transactions
    - Confidence: P(B|A) = transactions with both / transactions with A
    - Lift: Confidence / P(B) = how much more likely B is when A is present

Output: "Customers who buy X also buy Y" recommendations

Author: Hackathon Team
Version: 1.0
================================================================================
"""

import pandas as pd
import numpy as np
import yaml
import logging
import os
from datetime import datetime
from itertools import combinations
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass


# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging(config: Dict) -> logging.Logger:
    """Configure logging."""
    log_config = config.get('logging', {})
    log_file = log_config.get('file', 'logs/task2_pipeline.log')
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    logger = logging.getLogger('BasketAffinity')
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers
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
class AffinityStats:
    """Track pipeline statistics."""
    total_transactions: int = 0
    total_line_items: int = 0
    total_products: int = 0
    unique_products_in_sales: int = 0
    total_product_pairs: int = 0
    pairs_above_min_support: int = 0
    top_affinities_count: int = 0
    execution_time: float = 0.0


# =============================================================================
# MAIN PIPELINE CLASS
# =============================================================================

class BasketAffinityAnalyzer:
    """
    Shopping Basket Affinity Analyzer using Association Rule Mining.
    
    Computes:
    - Support: How often products appear together
    - Confidence: Probability of buying B given A
    - Lift: How much more likely B is when A is present
    """
    
    def __init__(self, config_path: str = "config/task2_config.yml"):
        """Initialize the analyzer."""
        self.config = self._load_config(config_path)
        self.logger = setup_logging(self.config)
        self.stats = AffinityStats()
        
        # Data storage
        self.sales_header: Optional[pd.DataFrame] = None
        self.sales_line_items: Optional[pd.DataFrame] = None
        self.products: Optional[pd.DataFrame] = None
        self.transaction_baskets: Optional[pd.DataFrame] = None
        self.product_pairs: Optional[pd.DataFrame] = None
        self.affinity_scores: Optional[pd.DataFrame] = None
        self.top_affinities: Optional[pd.DataFrame] = None
    
    def _load_config(self, config_path: str) -> Dict:
        """Load YAML configuration."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    # =========================================================================
    # LAYER 1: RAW - Data Ingestion
    # =========================================================================
    
    def ingest_data(self) -> None:
        """
        Load sales data and product master.
        Uses products from Task 1 output.
        """
        self.logger.info("=" * 70)
        self.logger.info("LAYER 1: RAW - Data Ingestion")
        self.logger.info("=" * 70)
        
        # Load sales header
        header_path = self.config['data_sources']['store_sales_header']['file_path']
        self.sales_header = pd.read_csv(header_path)
        self.stats.total_transactions = len(self.sales_header)
        self.logger.info(f"Loaded sales header: {self.stats.total_transactions} transactions")
        
        # Load sales line items
        line_items_path = self.config['data_sources']['store_sales_line_items']['file_path']
        self.sales_line_items = pd.read_csv(line_items_path)
        self.stats.total_line_items = len(self.sales_line_items)
        self.logger.info(f"Loaded line items: {self.stats.total_line_items} items")
        
        # Load products (from Task 1)
        products_path = self.config['data_sources']['products']['file_path']
        self.products = pd.read_csv(products_path)
        self.stats.total_products = len(self.products)
        self.logger.info(f"Loaded products: {self.stats.total_products} products")
        
        # Count unique products in sales
        self.stats.unique_products_in_sales = self.sales_line_items['product_id'].nunique()
        self.logger.info(f"Unique products in sales: {self.stats.unique_products_in_sales}")
    
    # =========================================================================
    # LAYER 2: STAGING - Build Transaction Baskets
    # =========================================================================
    
    def build_transaction_baskets(self) -> pd.DataFrame:
        """
        Group products by transaction to create shopping baskets.
        Each basket = list of products in one transaction.
        """
        self.logger.info("=" * 70)
        self.logger.info("LAYER 2: STAGING - Building Transaction Baskets")
        self.logger.info("=" * 70)
        
        # Group products by transaction_id
        baskets = self.sales_line_items.groupby('transaction_id')['product_id'].apply(list).reset_index()
        baskets.columns = ['transaction_id', 'products']
        
        # Add basket size
        baskets['basket_size'] = baskets['products'].apply(len)
        
        # Filter baskets with at least 2 products (needed for pairs)
        baskets_with_pairs = baskets[baskets['basket_size'] >= 2].copy()
        
        self.logger.info(f"Total baskets: {len(baskets)}")
        self.logger.info(f"Baskets with 2+ products: {len(baskets_with_pairs)}")
        self.logger.info(f"Average basket size: {baskets['basket_size'].mean():.2f}")
        
        self.transaction_baskets = baskets_with_pairs
        
        # Save to staging
        staging_path = self.config['output']['staging']['transaction_baskets']
        os.makedirs(staging_path, exist_ok=True)
        baskets_with_pairs.to_parquet(f"{staging_path}/transaction_baskets.parquet", index=False)
        
        return baskets_with_pairs
    
    def generate_product_pairs(self) -> pd.DataFrame:
        """
        Generate all product pairs from each transaction basket.
        Example: Basket [A, B, C] -> Pairs [(A,B), (A,C), (B,C)]
        """
        self.logger.info("-" * 50)
        self.logger.info("Generating Product Pairs...")
        self.logger.info("-" * 50)
        
        all_pairs = []
        
        for _, row in self.transaction_baskets.iterrows():
            transaction_id = row['transaction_id']
            products = row['products']
            
            # Generate all 2-product combinations
            # sorted() ensures (A,B) and (B,A) become same pair
            for pair in combinations(sorted(products), 2):
                all_pairs.append({
                    'transaction_id': transaction_id,
                    'product_a': pair[0],
                    'product_b': pair[1]
                })
        
        pairs_df = pd.DataFrame(all_pairs)
        self.stats.total_product_pairs = len(pairs_df)
        
        self.logger.info(f"Generated {self.stats.total_product_pairs} product pairs")
        
        self.product_pairs = pairs_df
        
        # Save to staging
        staging_path = self.config['output']['staging']['product_pairs']
        os.makedirs(staging_path, exist_ok=True)
        pairs_df.to_parquet(f"{staging_path}/product_pairs.parquet", index=False)
        
        return pairs_df
    
    # =========================================================================
    # LAYER 3: AFFINITY COMPUTATION - Support, Confidence, Lift
    # =========================================================================
    
    def compute_affinity_scores(self) -> pd.DataFrame:
        """
        Compute association rule metrics for all product pairs.
        
        Metrics:
        - Support(A,B) = P(A and B) = count(A and B) / total_transactions
        - Confidence(A->B) = P(B|A) = count(A and B) / count(A)
        - Lift(A->B) = Confidence(A->B) / P(B) = how much more likely
        """
        self.logger.info("=" * 70)
        self.logger.info("LAYER 3: AFFINITY COMPUTATION")
        self.logger.info("=" * 70)
        
        total_transactions = self.stats.total_transactions
        
        # Count occurrences of each product pair
        pair_counts = self.product_pairs.groupby(['product_a', 'product_b']).size().reset_index(name='pair_count')
        
        self.logger.info(f"Unique product pairs: {len(pair_counts)}")
        
        # Count occurrences of each individual product
        product_counts = self.sales_line_items.groupby('product_id')['transaction_id'].nunique().reset_index()
        product_counts.columns = ['product_id', 'product_count']
        
        # Merge to get counts for product_a
        affinity_df = pair_counts.merge(
            product_counts,
            left_on='product_a',
            right_on='product_id',
            how='left'
        )
        affinity_df = affinity_df.rename(columns={'product_count': 'count_a'})
        affinity_df = affinity_df.drop('product_id', axis=1)
        
        # Merge to get counts for product_b
        affinity_df = affinity_df.merge(
            product_counts,
            left_on='product_b',
            right_on='product_id',
            how='left'
        )
        affinity_df = affinity_df.rename(columns={'product_count': 'count_b'})
        affinity_df = affinity_df.drop('product_id', axis=1)
        
        # Calculate SUPPORT
        # Support(A,B) = P(A and B) = transactions with both / total transactions
        affinity_df['support'] = affinity_df['pair_count'] / total_transactions
        
        # Calculate CONFIDENCE (A -> B)
        # Confidence(A->B) = P(B|A) = transactions with both / transactions with A
        affinity_df['confidence_a_to_b'] = affinity_df['pair_count'] / affinity_df['count_a']
        
        # Calculate CONFIDENCE (B -> A)
        affinity_df['confidence_b_to_a'] = affinity_df['pair_count'] / affinity_df['count_b']
        
        # Calculate LIFT
        # Lift(A->B) = Confidence(A->B) / P(B) = how much more likely B is when A is present
        affinity_df['p_b'] = affinity_df['count_b'] / total_transactions
        affinity_df['p_a'] = affinity_df['count_a'] / total_transactions
        affinity_df['lift'] = affinity_df['confidence_a_to_b'] / affinity_df['p_b']
        
        # Add product names
        affinity_df = affinity_df.merge(
            self.products[['product_id', 'product_name']],
            left_on='product_a',
            right_on='product_id',
            how='left'
        )
        affinity_df = affinity_df.rename(columns={'product_name': 'product_a_name'})
        affinity_df = affinity_df.drop('product_id', axis=1)
        
        affinity_df = affinity_df.merge(
            self.products[['product_id', 'product_name']],
            left_on='product_b',
            right_on='product_id',
            how='left'
        )
        affinity_df = affinity_df.rename(columns={'product_name': 'product_b_name'})
        affinity_df = affinity_df.drop('product_id', axis=1)
        
        # Apply minimum thresholds from config
        min_support = self.config['affinity']['min_support']
        min_confidence = self.config['affinity']['min_confidence']
        min_lift = self.config['affinity']['min_lift']
        
        filtered_df = affinity_df[
            (affinity_df['support'] >= min_support) &
            (affinity_df['confidence_a_to_b'] >= min_confidence) &
            (affinity_df['lift'] >= min_lift)
        ].copy()
        
        self.stats.pairs_above_min_support = len(filtered_df)
        self.logger.info(f"Pairs meeting thresholds: {self.stats.pairs_above_min_support}")
        
        # Log metrics explanation
        self.logger.info("-" * 50)
        self.logger.info("METRICS EXPLANATION:")
        self.logger.info("  Support = How often products appear together")
        self.logger.info("  Confidence = Probability of buying B given A")
        self.logger.info("  Lift > 1 = Positive association (buy together)")
        self.logger.info("  Lift = 1 = No association (independent)")
        self.logger.info("  Lift < 1 = Negative association (don't buy together)")
        self.logger.info("-" * 50)
        
        self.affinity_scores = filtered_df
        return filtered_df
    
    # =========================================================================
    # LAYER 4: CURATED - Top Affinities & Recommendations
    # =========================================================================
    
    def get_top_affinities(self) -> pd.DataFrame:
        """
        Get the Top N strongest product affinities.
        Ranked by LIFT (strongest association indicator).
        """
        self.logger.info("=" * 70)
        self.logger.info("LAYER 4: CURATED - Top Affinities")
        self.logger.info("=" * 70)
        
        top_n = self.config['affinity']['top_n']
        
        # Sort by lift (descending) and get top N
        top_df = self.affinity_scores.sort_values('lift', ascending=False).head(top_n).copy()
        
        # Add rank
        top_df['rank'] = range(1, len(top_df) + 1)
        
        # Create recommendation text
        top_df['recommendation'] = top_df.apply(
            lambda row: f"Customers who buy {row['product_a_name']} also buy {row['product_b_name']}",
            axis=1
        )
        
        # Select final columns
        final_columns = [
            'rank',
            'product_a',
            'product_a_name',
            'product_b',
            'product_b_name',
            'support',
            'confidence_a_to_b',
            'lift',
            'pair_count',
            'recommendation'
        ]
        
        top_df = top_df[final_columns]
        
        # Round numeric columns for readability
        top_df['support'] = top_df['support'].round(4)
        top_df['confidence_a_to_b'] = top_df['confidence_a_to_b'].round(4)
        top_df['lift'] = top_df['lift'].round(4)
        
        self.stats.top_affinities_count = len(top_df)
        self.top_affinities = top_df
        
        # Log top affinities
        self.logger.info(f"\nTOP {top_n} PRODUCT AFFINITIES:")
        self.logger.info("-" * 80)
        for _, row in top_df.iterrows():
            self.logger.info(
                f"#{row['rank']}: {row['product_a_name']} + {row['product_b_name']} "
                f"(Lift: {row['lift']:.2f}, Confidence: {row['confidence_a_to_b']:.2%})"
            )
        self.logger.info("-" * 80)
        
        return top_df
    
    def save_curated_data(self) -> None:
        """Save all curated outputs."""
        self.logger.info("-" * 50)
        self.logger.info("Saving Curated Data...")
        self.logger.info("-" * 50)
        
        # Save affinity scores
        affinity_path = self.config['output']['curated']['affinity_scores']
        os.makedirs(affinity_path, exist_ok=True)
        
        self.affinity_scores.to_parquet(f"{affinity_path}/affinity_scores.parquet", index=False)
        if self.config['output']['curated'].get('also_save_csv', True):
            self.affinity_scores.to_csv(f"{affinity_path}/affinity_scores.csv", index=False)
        self.logger.info(f"Saved affinity scores to {affinity_path}")
        
        # Save top affinities
        top_path = self.config['output']['curated']['top_affinities']
        os.makedirs(top_path, exist_ok=True)
        
        self.top_affinities.to_parquet(f"{top_path}/top_affinities.parquet", index=False)
        if self.config['output']['curated'].get('also_save_csv', True):
            self.top_affinities.to_csv(f"{top_path}/top_affinities.csv", index=False)
        self.logger.info(f"Saved top affinities to {top_path}")
    
    def generate_report(self) -> str:
        """Generate a summary report."""
        report = f"""
================================================================================
          SHOPPING BASKET AFFINITY ANALYSIS - EXECUTION REPORT
================================================================================

Pipeline: {self.config['pipeline']['name']}
Version:  {self.config['pipeline']['version']}
Executed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

--------------------------------------------------------------------------------
                         INPUT DATA STATISTICS
--------------------------------------------------------------------------------

  Total Transactions:        {self.stats.total_transactions:,}
  Total Line Items:          {self.stats.total_line_items:,}
  Total Products (Master):   {self.stats.total_products:,}
  Unique Products in Sales:  {self.stats.unique_products_in_sales:,}

--------------------------------------------------------------------------------
                         AFFINITY ANALYSIS RESULTS
--------------------------------------------------------------------------------

  Total Product Pairs:       {self.stats.total_product_pairs:,}
  Pairs Above Threshold:     {self.stats.pairs_above_min_support:,}
  Top Affinities Returned:   {self.stats.top_affinities_count:,}

--------------------------------------------------------------------------------
                         THRESHOLDS USED
--------------------------------------------------------------------------------

  Minimum Support:           {self.config['affinity']['min_support']}
  Minimum Confidence:        {self.config['affinity']['min_confidence']}
  Minimum Lift:              {self.config['affinity']['min_lift']}

--------------------------------------------------------------------------------
                         TOP {self.config['affinity']['top_n']} RECOMMENDATIONS
--------------------------------------------------------------------------------

"""
        # Add top recommendations
        for _, row in self.top_affinities.iterrows():
            report += f"""
  #{row['rank']}: {row['recommendation']}
       Support: {row['support']:.4f} | Confidence: {row['confidence_a_to_b']:.2%} | Lift: {row['lift']:.2f}
"""
        
        report += f"""
--------------------------------------------------------------------------------
                         OUTPUT LOCATIONS
--------------------------------------------------------------------------------

  Affinity Scores:   {self.config['output']['curated']['affinity_scores']}/
  Top Affinities:    {self.config['output']['curated']['top_affinities']}/

--------------------------------------------------------------------------------
                         EXECUTION TIME
--------------------------------------------------------------------------------

  Total Time:        {self.stats.execution_time:.2f} seconds

================================================================================
"""
        return report
    
    # =========================================================================
    # MAIN PIPELINE EXECUTION
    # =========================================================================
    
    def run(self) -> Dict[str, Any]:
        """Execute the complete affinity analysis pipeline."""
        self.logger.info("*" * 70)
        self.logger.info("STARTING: SHOPPING BASKET AFFINITY ANALYZER")
        self.logger.info("*" * 70)
        
        start_time = datetime.now()
        
        try:
            # Step 1: Ingest data
            self.ingest_data()
            
            # Step 2: Build transaction baskets
            self.build_transaction_baskets()
            
            # Step 3: Generate product pairs
            self.generate_product_pairs()
            
            # Step 4: Compute affinity scores
            self.compute_affinity_scores()
            
            # Step 5: Get top affinities
            self.get_top_affinities()
            
            # Step 6: Save curated data
            self.save_curated_data()
            
            # Calculate execution time
            end_time = datetime.now()
            self.stats.execution_time = (end_time - start_time).total_seconds()
            
            # Generate and save report
            report = self.generate_report()
            
            os.makedirs('curated/task2', exist_ok=True)
            with open('curated/task2/affinity_report.txt', 'w') as f:
                f.write(report)
            
            print(report)
            
            self.logger.info("*" * 70)
            self.logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
            self.logger.info("*" * 70)
            
            return {
                'status': 'SUCCESS',
                'stats': self.stats.__dict__,
                'top_affinities': self.top_affinities.to_dict('records')
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
    print("    TASK 2: REAL-TIME SHOPPING BASKET AFFINITY ANALYZER")
    print("=" * 70 + "\n")
    
    # Initialize and run pipeline
    analyzer = BasketAffinityAnalyzer(config_path="config/task2_config.yml")
    result = analyzer.run()
    
    if result['status'] == 'SUCCESS':
        print("\n" + "=" * 70)
        print("TOP PRODUCT AFFINITIES (Recommendations):")
        print("=" * 70)
        for aff in result['top_affinities']:
            print(f"  #{aff['rank']}: {aff['recommendation']}")
            print(f"       Lift: {aff['lift']:.2f} | Confidence: {aff['confidence_a_to_b']:.2%}")
        print("=" * 70)
        print("\n✅ Pipeline completed successfully!")
    else:
        print(f"\n❌ Pipeline failed: {result.get('error', 'Unknown error')}")


if __name__ == "__main__":
    main()

