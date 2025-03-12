import os
import sys
import time
from sqlalchemy.orm import Session
from database import init_db, get_db
from data_loader import load_all_data
from logger import logger

def main():
    try:
        # Initialize database
        logger.info("Initializing database...")
        init_db()
        
        # Get database session
        db = next(get_db())
        
        # Load data from parent directory
        # Navigate one level up from the src directory to the root
        current_dir = os.path.dirname(os.path.abspath(__file__))
        root_dir = os.path.dirname(current_dir)
        data_dir = os.path.join(root_dir, "data")
        
        logger.info(f"Looking for data in: {data_dir}")
        
        # Verify data directory exists
        if not os.path.exists(data_dir):
            logger.error(f"Error: Data directory not found at {data_dir}")
            return 1
            
        # Load data with timeout handling
        logger.info("Starting data loading process...")
        start_time = time.time()
        
        success = load_all_data(db, data_dir)
        
        end_time = time.time()
        duration = end_time - start_time
        
        if success:
            logger.info(f"Data loading completed successfully in {duration:.2f} seconds")
        else:
            logger.error("Data loading process failed or was interrupted")
            return 1
        
        return 0
    
    except KeyboardInterrupt:
        logger.warning("\nData loading interrupted by user (Ctrl+C)")
        return 130  # Standard exit code for interrupt signal
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return 1

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.warning("\nProgram interrupted by user (Ctrl+C)")
        sys.exit(130)
