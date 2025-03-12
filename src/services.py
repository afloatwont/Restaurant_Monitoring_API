import pandas as pd
import numpy as np
import pytz
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import datetime, timedelta, time
import os
import csv
import traceback
from typing import List, Dict, Tuple, Optional
from database import StoreStatus, BusinessHours, Timezone, ReportStatus
from logger import logger

chunk_size = 1000

def get_report_status(report_id: str, db: Session):
    return db.query(ReportStatus).filter(ReportStatus.report_id == report_id).first()

def get_max_timestamp(db: Session) -> datetime:
    try:
        max_timestamp = db.query(func.max(StoreStatus.timestamp_utc)).scalar()
        if not max_timestamp:
            max_timestamp = datetime.utcnow()
        return max_timestamp
    except Exception as e:
        logger.error(f"Error getting max timestamp: {str(e)}")
        return datetime.utcnow()

def get_store_timezone(store_id: str, db: Session) -> str:
    try:
        timezone_record = db.query(Timezone).filter(Timezone.store_id == store_id).first()
        if timezone_record:
            return timezone_record.timezone_str
        return 'America/Chicago'
    except Exception as e:
        logger.warning(f"Error getting timezone for store {store_id}: {str(e)}")
        return 'America/Chicago'

def get_business_hours(store_id: str, db: Session) -> List[Dict]:
    try:
        hours = db.query(BusinessHours).filter(BusinessHours.store_id == store_id).all()
        if hours:
            return [
                {
                    'day_of_week': hour.day_of_week,
                    'start_time_local': hour.start_time_local,
                    'end_time_local': hour.end_time_local
                }
                for hour in hours
            ]
        logger.info(f"No business hours found for store {store_id}, assuming 24/7 operation")
        return [
            {
                'day_of_week': day,
                'start_time_local': time(0, 0),
                'end_time_local': time(23, 59, 59)
            }
            for day in range(7)
        ]
    except Exception as e:
        logger.error(f"Error getting business hours for store {store_id}: {str(e)}")
        return [
            {
                'day_of_week': day,
                'start_time_local': time(0, 0),
                'end_time_local': time(23, 59, 59)
            }
            for day in range(7)
        ]

def is_store_open(timestamp_utc: datetime, store_id: str, business_hours: List[Dict], tz_str: str) -> bool:
    try:
        local_tz = pytz.timezone(tz_str)
        local_time = timestamp_utc.replace(tzinfo=pytz.utc).astimezone(local_tz)
        day_of_week = local_time.weekday()
        for hours in business_hours:
            if hours['day_of_week'] == day_of_week:
                start_dt = datetime.combine(local_time.date(), hours['start_time_local'])
                end_dt = datetime.combine(local_time.date(), hours['end_time_local'])
                if start_dt <= local_time.replace(tzinfo=None) <= end_dt:
                    return True
        return False
    except Exception as e:
        logger.error(f"Error checking if store {store_id} is open: {str(e)}")
        return True

def calculate_uptime_downtime(store_id: str, current_time: datetime, db: Session) -> Dict:
    try:
        logger.info(f"Calculating metrics for store {store_id}")
        tz_str = get_store_timezone(store_id, db)
        local_tz = pytz.timezone(tz_str)
        business_hours = get_business_hours(store_id, db)
        hour_ago = current_time - timedelta(hours=1)
        day_ago = current_time - timedelta(days=1)
        week_ago = current_time - timedelta(days=7)
        result = {
            'store_id': store_id,
            'uptime_last_hour': 0,
            'uptime_last_day': 0,
            'uptime_last_week': 0,
            'downtime_last_hour': 0,
            'downtime_last_day': 0,
            'downtime_last_week': 0
        }
        chunk_query = db.query(StoreStatus).filter(
            StoreStatus.store_id == store_id,
            StoreStatus.timestamp_utc >= week_ago,
            StoreStatus.timestamp_utc <= current_time
        ).order_by(StoreStatus.timestamp_utc)
        total_records = chunk_query.count()
        if total_records == 0:
            logger.warning(f"No status data found for store {store_id} in the time range")
            return result
        all_data = []
        offset = 0
        while offset < total_records:
            chunk = chunk_query.offset(offset).limit(chunk_size).all()
            chunk_data = [
                {
                    'timestamp_utc': record.timestamp_utc,
                    'status': record.status
                }
                for record in chunk
            ]
            all_data.extend(chunk_data)
            offset += chunk_size
        records_df = pd.DataFrame(all_data)
        records_df['is_business_hours'] = records_df['timestamp_utc'].apply(
            lambda ts: is_store_open(ts, store_id, business_hours, tz_str)
        )
        business_hours_df = records_df[records_df['is_business_hours']]
        if business_hours_df.empty:
            logger.warning(f"No observations during business hours for store {store_id}")
            return result
        hour_df = business_hours_df[business_hours_df['timestamp_utc'] >= hour_ago]
        if not hour_df.empty:
            active_ratio = hour_df[hour_df['status'] == 'active'].shape[0] / hour_df.shape[0]
            result['uptime_last_hour'] = round(active_ratio * 60, 2)
            result['downtime_last_hour'] = round(60 - result['uptime_last_hour'], 2)
        day_df = business_hours_df[business_hours_df['timestamp_utc'] >= day_ago]
        if not day_df.empty:
            active_ratio = day_df[day_df['status'] == 'active'].shape[0] / day_df.shape[0]
            total_business_hours = calculate_business_hours_in_range(
                day_ago, current_time, store_id, business_hours, tz_str
            )
            result['uptime_last_day'] = round(active_ratio * total_business_hours, 2)
            result['downtime_last_day'] = round(total_business_hours - result['uptime_last_day'], 2)
        if not business_hours_df.empty:
            active_ratio = business_hours_df[business_hours_df['status'] == 'active'].shape[0] / business_hours_df.shape[0]
            total_business_hours = calculate_business_hours_in_range(
                week_ago, current_time, store_id, business_hours, tz_str
            )
            result['uptime_last_week'] = round(active_ratio * total_business_hours, 2)
            result['downtime_last_week'] = round(total_business_hours - result['uptime_last_week'], 2)
        logger.info(f"Completed metrics calculation for store {store_id}")
        return result
    except Exception as e:
        logger.error(f"Error calculating uptime/downtime for store {store_id}: {str(e)}")
        logger.debug(traceback.format_exc())
        return {
            'store_id': store_id,
            'uptime_last_hour': 0,
            'uptime_last_day': 0,
            'uptime_last_week': 0,
            'downtime_last_hour': 0,
            'downtime_last_day': 0,
            'downtime_last_week': 0
        }

def calculate_business_hours_in_range(start_time: datetime, end_time: datetime, 
                                     store_id: str, business_hours: List[Dict], tz_str: str) -> float:
    try:
        hours_count = 0
        current = start_time
        while current <= end_time:
            if is_store_open(current, store_id, business_hours, tz_str):
                hours_count += 1
            current += timedelta(hours=1)
        return hours_count
    except Exception as e:
        logger.error(f"Error calculating business hours in range for store {store_id}: {str(e)}")
        return 24

async def trigger_report_generation(report_id: str, db: Session):
    logger.info(f"Starting report generation for report_id: {report_id}")
    try:
        current_time = get_max_timestamp(db)
        logger.info(f"Using current time: {current_time}")
        os.makedirs("reports", exist_ok=True)
        store_ids = [row[0] for row in db.query(StoreStatus.store_id).distinct().all()]
        logger.info(f"Processing {len(store_ids)} stores")
        batch_size = 100
        results = []
        for i in range(0, len(store_ids), batch_size):
            batch = store_ids[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} of {(len(store_ids) + batch_size - 1) // batch_size}")
            for store_id in batch:
                result = calculate_uptime_downtime(store_id, current_time, db)
                results.append(result)
            progress = min(100, int((i + len(batch)) / len(store_ids) * 100))
            logger.info(f"Progress: {progress}%")
        output_file = f"reports/{report_id}.csv"
        logger.info(f"Writing report to {output_file}")
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = [
                'store_id', 
                'uptime_last_hour', 
                'uptime_last_day', 
                'uptime_last_week', 
                'downtime_last_hour', 
                'downtime_last_day', 
                'downtime_last_week'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for result in results:
                writer.writerow(result)
        report = db.query(ReportStatus).filter(ReportStatus.report_id == report_id).first()
        if report:
            report.status = "Complete"
            report.completed_at = datetime.utcnow()
            db.commit()
            logger.info(f"Report generation completed successfully for report_id: {report_id}")
    except Exception as e:
        logger.error(f"Error generating report {report_id}: {str(e)}")
        logger.error(traceback.format_exc())
        try:
            report = db.query(ReportStatus).filter(ReportStatus.report_id == report_id).first()
            if report:
                report.status = "Error"
                db.commit()
        except Exception as db_error:
            logger.error(f"Failed to update report status: {str(db_error)}")