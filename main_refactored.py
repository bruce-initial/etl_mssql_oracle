#!/usr/bin/env python3
"""
Refactored ETL Migration Tool
Multi-table transfer system from SQL Server to Oracle using modular architecture
"""

import sys
import argparse
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.transfer.engine import TransferEngine
from src.utils.logging import setup_logging
from src.utils.exceptions import ETLError

logger = setup_logging()


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description="ETL Migration Tool - Transfer data from SQL Server to Oracle"
    )
    parser.add_argument(
        "--config", 
        "-c",
        default="config/tables_config.yaml",
        help="Path to configuration file (default: config/tables_config.yaml)"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set logging level (default: INFO)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration without executing transfer"
    )
    
    args = parser.parse_args()
    
    try:
        logger.info("="*80)
        logger.info("ETL Migration Tool - Starting")
        logger.info("="*80)
        logger.info(f"Configuration file: {args.config}")
        logger.info(f"Log level: {args.log_level}")
        
        # Initialize transfer engine
        transfer_engine = TransferEngine(args.config)
        
        if args.dry_run:
            logger.info("DRY RUN MODE - Validating configuration only")
            
            # Validate configuration
            transfer_engine.config_manager.validate_all_configurations()
            logger.info("✓ Configuration validation successful")
            
            # Validate dependencies
            from src.dependency.resolver import DependencyResolver
            tables_config = transfer_engine.config_manager.get_tables_config()
            resolver = DependencyResolver(tables_config)
            resolver.validate_dependencies()
            ordered_tables = resolver.resolve_dependencies()
            
            logger.info(f"✓ Transfer order: {[t['target_table'] for t in ordered_tables]}")
            logger.info("✓ Dependency resolution successful")
            
            # Show what would be done
            logger.info("✓ Tables to transfer:")
            for i, table in enumerate(ordered_tables, 1):
                fk_count = len(table.get('foreign_keys', []))
                idx_count = len(table.get('indexes', []))
                logger.info(f"  {i}. {table['target_table']} (FKs: {fk_count}, Indexes: {idx_count})")
            
            logger.info("DRY RUN COMPLETE - No data was transferred")
            logger.info("Note: Database connections were not tested in dry-run mode")
            
        else:
            # Execute full transfer
            result = transfer_engine.transfer_all_tables()
            
            if result['success']:
                logger.info("="*80)
                logger.info("ETL MIGRATION COMPLETED SUCCESSFULLY!")
                logger.info(f"Transferred {result['successful_tables']}/{result['total_tables']} tables")
                logger.info(f"Total duration: {result['duration']:.2f} seconds")
                
                # Log quality check summary
                quality_summary = result.get('quality_summary')
                if quality_summary:
                    logger.info(f"Quality checks: {quality_summary['passed']}/{quality_summary['total']} passed "
                               f"({quality_summary['success_rate']:.1f}% success rate)")
                    if quality_summary['warning'] > 0:
                        logger.warning(f"Quality warnings: {quality_summary['warning']} tables")
                    if quality_summary['failed'] > 0:
                        logger.warning(f"Quality failures: {quality_summary['failed']} tables")
                
                logger.info("="*80)
                return 0
            else:
                logger.error("="*80)
                logger.error("ETL MIGRATION FAILED!")
                logger.error(f"Only {result['successful_tables']}/{result['total_tables']} tables transferred successfully")
                
                # Log quality check summary for failed migration
                quality_summary = result.get('quality_summary')
                if quality_summary and quality_summary['total'] > 0:
                    logger.error(f"Quality checks: {quality_summary['passed']}/{quality_summary['total']} passed")
                
                logger.error("="*80)
                return 1
                
    except ETLError as e:
        logger.error(f"ETL operation failed: {e}")
        return 1
    except KeyboardInterrupt:
        logger.warning("Migration interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)