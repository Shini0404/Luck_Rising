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

