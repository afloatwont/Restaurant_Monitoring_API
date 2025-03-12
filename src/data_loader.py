import os
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import func
import csv
import time
from datetime import datetime
import traceback
from database import StoreStatus, BusinessHours, Timezone
from logger import logger

# Set chunk size for processing large CSV files
CHUNK_SIZE = 10000

def load_store_status(db: Session, file_path: str):
    """Load store status data from CSV"""
    try:
        # Check if data is already loaded
        count = db.query(func.count(StoreStatus.id)).scalar()
        if count > 0:
            logger.info("Store status data already loaded. Skipping...")
            return
        
        logger.info(f"Loading store status data from {file_path}")
        
        # Check file exists
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return
            
        # Read and process CSV in chunks to handle large files
        total_records = 0
        chunk_count = 0
        
        for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):
            chunk_count += 1
            logger.info(f"Processing chunk {chunk_count} of store status data ({len(chunk)} records)")
            
            # Insert data into database
            for _, row in chunk.iterrows():
                try:
                    # Handle timestamp parsing explicitly to avoid issues
                    try:
                        timestamp_utc = pd.to_datetime(row['timestamp_utc'], format='%Y-%m-%d %H:%M:%S.%f %Z', errors='coerce')
                        if pd.isna(timestamp_utc):
                            timestamp_utc = pd.to_datetime(row['timestamp_utc'], errors='coerce')
                            
                        if pd.isna(timestamp_utc):
                            logger.warning(f"Could not parse timestamp: {row['timestamp_utc']} for store {row['store_id']}")
                            continue
                    except Exception as e:
                        logger.warning(f"Error parsing timestamp for store {row['store_id']}: {str(e)}")
                        continue
                    
                    store_status = StoreStatus(
                        store_id=str(row['store_id']),
                        timestamp_utc=timestamp_utc,
                        status=row['status']
                    )
                    db.add(store_status)
                    total_records += 1
                    
                    # Commit every 1000 records to avoid memory issues
                    if total_records % 1000 == 0:
                        db.commit()
                        logger.info(f"Committed {total_records} records so far")
                        
                except KeyError as ke:
                    logger.error(f"Missing column in store status data: {str(ke)}")
                    logger.info(f"Available columns: {chunk.columns.tolist()}")
                    db.rollback()
                    return
                except Exception as e:
                    logger.warning(f"Error processing store status record: {str(e)}")
                    continue
            
            # Commit after each chunk
            try:
                db.commit()
            except Exception as e:
                logger.error(f"Error committing store status chunk: {str(e)}")
                db.rollback()
        
        logger.info(f"Successfully loaded {total_records} store status records")
    
    except KeyboardInterrupt:
        logger.warning("Store status data loading interrupted by user")
        db.rollback()
        raise
    except Exception as e:
        logger.error(f"Error loading store status data: {str(e)}")
        logger.debug(traceback.format_exc())
        db.rollback()

def load_business_hours(db: Session, file_path: str):
    """Load business hours data from CSV"""
    try:
        # Check if data is already loaded
        count = db.query(func.count(BusinessHours.id)).scalar()
        if count > 0:
            logger.info("Business hours data already loaded. Skipping...")
            return
        
        logger.info(f"Loading business hours data from {file_path}")
        
        # Check file exists
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return
        
        # Read CSV file
        df = pd.read_csv(file_path)
        logger.info(f"Columns in business hours CSV: {df.columns.tolist()}")
        
        # Determine day column name - updated to include 'dayOfWeek'
        day_col = None
        for possible_name in ['day_of_week', 'day', 'dayOfWeek']:
            if possible_name in df.columns:
                day_col = possible_name
                break
                
        if day_col is None:
            logger.error(f"Could not find day column in {file_path}")
            logger.info(f"Available columns: {df.columns.tolist()}")
            return
            
        total_records = 0
        error_count = 0
        
        # Insert data into database
        for _, row in df.iterrows():
            try:
                # Parse time strings into time objects
                try:
                    start_time_local = datetime.strptime(row['start_time_local'], '%H:%M:%S').time()
                    end_time_local = datetime.strptime(row['end_time_local'], '%H:%M:%S').time()
                except ValueError:
                    logger.warning(f"Invalid time format for store {row['store_id']}, day {row[day_col]}")
                    error_count += 1
                    continue
                
                business_hours = BusinessHours(
                    store_id=str(row['store_id']),
                    day_of_week=int(row[day_col]),
                    start_time_local=start_time_local,
                    end_time_local=end_time_local
                )
                db.add(business_hours)
                total_records += 1
                
                # Commit every 1000 records
                if total_records % 1000 == 0:
                    db.commit()
                    logger.info(f"Committed {total_records} business hours records")
                    
            except KeyError as ke:
                logger.error(f"Missing column in business hours data: {str(ke)}")
                logger.info(f"Available columns: {df.columns.tolist()}")
                db.rollback()
                return
            except Exception as e:
                logger.warning(f"Error processing business hours record: {str(e)}")
                error_count += 1
                continue
        
        # Final commit
        db.commit()
        logger.info(f"Successfully loaded {total_records} business hours records")
        if error_count > 0:
            logger.warning(f"Encountered {error_count} errors while processing business hours data")
    
    except KeyboardInterrupt:
        logger.warning("Business hours data loading interrupted by user")
        db.rollback()
        raise
    except Exception as e:
        logger.error(f"Error loading business hours data: {str(e)}")
        logger.debug(traceback.format_exc())
        db.rollback()

def load_timezone(db: Session, file_path: str):
    """Load timezone data from CSV"""
    try:
        # Check if data is already loaded
        count = db.query(func.count(Timezone.id)).scalar()
        if count > 0:
            logger.info("Timezone data already loaded. Skipping...")
            return
        
        logger.info(f"Loading timezone data from {file_path}")
        
        # Check file exists
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return
        
        # Read CSV file
        df = pd.read_csv(file_path)
        logger.info(f"Columns in timezone CSV: {df.columns.tolist()}")
        
        total_records = 0
        error_count = 0
        
        # Insert data into database
        for _, row in df.iterrows():
            try:
                timezone = Timezone(
                    store_id=str(row['store_id']),
                    timezone_str=row['timezone_str']
                )
                db.add(timezone)
                total_records += 1
                
                # Commit every 1000 records
                if total_records % 1000 == 0:
                    db.commit()
                    logger.info(f"Committed {total_records} timezone records")
                    
            except KeyError as ke:
                logger.error(f"Missing column in timezone data: {str(ke)}")
                logger.info(f"Available columns: {df.columns.tolist()}")
                db.rollback()
                return
            except Exception as e:
                logger.warning(f"Error processing timezone record: {str(e)}")
                error_count += 1
                continue
        
        # Final commit
        db.commit()
        logger.info(f"Successfully loaded {total_records} timezone records")
        if error_count > 0:
            logger.warning(f"Encountered {error_count} errors while processing timezone data")
    
    except KeyboardInterrupt:
        logger.warning("Timezone data loading interrupted by user")
        db.rollback()
        raise
    except Exception as e:
        logger.error(f"Error loading timezone data: {str(e)}")
        logger.debug(traceback.format_exc())
        db.rollback()

def load_all_data(db: Session, data_dir: str):
    """Load all data from CSV files"""
    try:
        logger.info(f"Starting data loading from {data_dir}")
        
        # Check if directory exists
        if not os.path.isdir(data_dir):
            logger.error(f"Data directory not found: {data_dir}")
            return False
        
        # Check if required files exist
        required_files = ['store_status.csv', 'menu_hours.csv', 'timezones.csv']
        for file in required_files:
            file_path = os.path.join(data_dir, file)
            if not os.path.exists(file_path):
                logger.error(f"Required file not found: {file_path}")
                return False
                
        # Load data
        try:
            load_store_status(db, os.path.join(data_dir, 'store_status.csv'))
        except Exception as e:
            logger.error(f"Failed to load store status data: {str(e)}")
            
        try:
            load_business_hours(db, os.path.join(data_dir, 'menu_hours.csv'))
        except Exception as e:
            logger.error(f"Failed to load business hours data: {str(e)}")
            
        try:
            load_timezone(db, os.path.join(data_dir, 'timezones.csv'))
        except Exception as e:
            logger.error(f"Failed to load timezone data: {str(e)}")
            
        logger.info("Data loading process completed")
        return True
        
    except KeyboardInterrupt:
        logger.warning("Data loading interrupted by user")
        return False
    except Exception as e:
        logger.error(f"Error in load_all_data: {str(e)}")
        logger.debug(traceback.format_exc())
        return False