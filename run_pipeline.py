"""Main entry point for running data pipelines."""
import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.config import get_config, DemandConfig, DiagnosisConfig
from pipelines.shared import setup_logging

logger = setup_logging()


def run_demand_pipeline(config: Optional[DemandConfig] = None):
    """Run demand pipeline."""
    if config is None:
        config = get_config("demand")

    logger.info("Starting demand pipeline...")

    try:
        # Import here to avoid circular imports
        from pipelines.demand.main import main as demand_main

        demand_main()
        logger.info("Demand pipeline completed successfully")
        return True
    except Exception as e:
        logger.error(f"Demand pipeline failed: {e}", exc_info=True)
        return False


def run_diagnosis_pipeline(config: Optional[DiagnosisConfig] = None):
    """Run diagnosis pipeline."""
    if config is None:
        config = get_config("diagnosis")

    logger.info("Starting diagnosis pipeline...")

    try:
        # Import here to avoid circular imports
        from pipelines.diagnosis.diagnosis_main import main as diagnosis_main

        diagnosis_main()
        logger.info("Diagnosis pipeline completed successfully")
        return True
    except Exception as e:
        logger.error(f"Diagnosis pipeline failed: {e}", exc_info=True)
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run PREDAP data processing pipelines",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py --demand         Run demand pipeline only
  python run_pipeline.py --diagnosis      Run diagnosis pipeline only
  python run_pipeline.py --both           Run both pipelines
  python run_pipeline.py                  Run both pipelines (default)
        """,
    )

    parser.add_argument(
        "--demand",
        action="store_true",
        help="Run demand pipeline only",
    )
    parser.add_argument(
        "--diagnosis",
        action="store_true",
        help="Run diagnosis pipeline only",
    )
    parser.add_argument(
        "--both",
        action="store_true",
        help="Run both pipelines",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Determine which pipelines to run
    run_demand = False
    run_diagnosis = False

    if args.demand:
        run_demand = True
    elif args.diagnosis:
        run_diagnosis = True
    else:
        # Default: run both
        run_demand = True
        run_diagnosis = True

    if args.both:
        run_demand = True
        run_diagnosis = True

    logger.info("=" * 80)
    logger.info("PREDAP Pipeline Execution Started")
    logger.info("=" * 80)

    results = []

    if run_demand:
        success = run_demand_pipeline()
        results.append(("Demand", success))

    if run_diagnosis:
        success = run_diagnosis_pipeline()
        results.append(("Diagnosis", success))

    logger.info("=" * 80)
    logger.info("Pipeline Execution Summary:")
    for name, success in results:
        status = "✓ SUCCESS" if success else "✗ FAILED"
        logger.info(f"  {name}: {status}")
    logger.info("=" * 80)

    # Exit with appropriate code
    all_success = all(success for _, success in results)
    return 0 if all_success else 1


if __name__ == "__main__":
    sys.exit(main())
