import argparse

# Import logging configuration first to set up logging
# This file (logging.py) imports the standard library logging module
import logging

# Now we can use the standard library logging functions
from logging import getLogger

from pipeline_dimensional_data.flow import DimensionalDataFlow

logger = getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description="Run dimensional data pipeline (ORDER_DDS)")
    parser.add_argument("--start_date", required=True, help="Start date in YYYY-MM-DD")
    parser.add_argument("--end_date", required=True, help="End date in YYYY-MM-DD")
    parser.add_argument("--execution_id", required=False, help="Optional execution id")
    return parser.parse_args()

def main():
    args = parse_args()
    flow = DimensionalDataFlow(execution_id=args.execution_id)
    logger.info(f"Starting pipeline execution_id={flow.execution_id} start_date={args.start_date} end_date={args.end_date}")
    result = flow.exec(args.start_date, args.end_date)
    logger.info(f"Pipeline finished: {result}")
    failed_tasks = [k for k,v in result.get("tasks", {}).items() if not v.get("success")]
    if failed_tasks:
        logger.error(f"Pipeline completed with failed tasks: {failed_tasks}")
        raise SystemExit(1)
    logger.info("Pipeline completed successfully.")
    return result

if __name__ == "__main__":
    main()
