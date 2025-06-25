#!/usr/bin/env python3
"""
Suno Data Migration Pipeline
ETL pipeline to migrate legacy EMR data to Suno's target schema
"""

import pandas as pd
import sqlite3
import json
import uuid
import hashlib
from datetime import datetime
import pytz
import phonenumbers
from phonenumbers import NumberParseException
import os

def main():
    """Main ETL pipeline execution"""
    print("Starting Suno Data Migration Pipeline...")
    
    # Load source data files
    print("\nLoading source data...")
    patients_df = pd.read_csv('data/patients_data.csv')
    appointments_df = pd.read_csv('data/appointments_data.csv')
    invoices_df = pd.read_csv('data/invoices_data.csv')
    
    print(f"Loaded {len(patients_df)} patients")
    print(f"Loaded {len(appointments_df)} appointments")  
    print(f"Loaded {len(invoices_df)} invoices")
    
    # Basic data exploration
    print("\nData exploration:")
    print("Patients columns:", patients_df.columns.tolist())
    print("Appointments columns:", appointments_df.columns.tolist())
    print("Invoices columns:", invoices_df.columns.tolist())

if __name__ == "__main__":
    main() 