"""
Project: QF5214 Polymarket Data Pipeline
File: ingestion_tasks.py
Author: Xu
Created: 2026-03-14

Description:
Polymarket API ingestion task definitions
"""
from app.celery.celery_app import celery_app
from app.services.ingestion_service import ingestion_service


@celery_app.task(name="app.tasks.ingestion_tasks.ingest_test_raw_data")
def ingest_test_raw_data():
    """
    Ingest test raw data.
    """
    ingestion_service.insert_test_raw_data()
