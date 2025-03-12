from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.responses import FileResponse
import uuid
import os
from datetime import datetime
import pandas as pd
from sqlalchemy.orm import Session
from typing import Dict, Optional

from database import get_db, init_db, ReportStatus, StoreStatus, BusinessHours, Timezone
from services import trigger_report_generation, get_report_status
from logger import logger

os.makedirs("reports", exist_ok=True)
os.makedirs("logs", exist_ok=True)

app = FastAPI(
    title="Restaurant Monitoring API",
    description="API for monitoring restaurant uptime and downtime",
    version="1.0.0"
)

@app.on_event("startup")
async def startup_event():
    logger.info("Starting Restaurant Monitoring API")
    try:
        init_db()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
    
@app.get("/")
def read_root():
    return {"message": "Welcome to Restaurant Monitoring API"}

@app.post("/trigger_report")
async def trigger_report(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    try:
        report_id = str(uuid.uuid4())
        logger.info(f"Triggering new report with ID: {report_id}")
        report_status = ReportStatus(report_id=report_id, status="Running")
        db.add(report_status)
        db.commit()
        background_tasks.add_task(trigger_report_generation, report_id, db)
        return {"report_id": report_id}
    except Exception as e:
        logger.error(f"Error triggering report: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to trigger report generation")

@app.get("/get_report")
async def get_report(report_id: str, db: Session = Depends(get_db)):
    try:
        status = get_report_status(report_id, db)
        if not status:
            logger.warning(f"Report with ID {report_id} not found")
            raise HTTPException(status_code=404, detail=f"Report with ID {report_id} not found")
        if status.status == "Running":
            logger.info(f"Report {report_id} is still running")
            return {"status": "Running"}
        if status.status == "Complete":
            file_path = f"reports/{report_id}.csv"
            if os.path.exists(file_path):
                logger.info(f"Serving completed report {report_id}")
                return FileResponse(
                    path=file_path, 
                    filename=f"report_{report_id}.csv",
                    media_type="text/csv"
                )
            else:
                logger.error(f"Report file for {report_id} not found at {file_path}")
                raise HTTPException(status_code=404, detail="Report file not found")
        if status.status == "Error":
            logger.warning(f"Report {report_id} encountered an error during generation")
            raise HTTPException(status_code=500, detail="An error occurred during report generation")
        logger.error(f"Unknown report status for {report_id}: {status.status}")
        raise HTTPException(status_code=500, detail=f"Unknown report status: {status.status}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving report {report_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred while retrieving the report")

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting server")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, log_level="info")