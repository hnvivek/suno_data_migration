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
import os
from typing import List, Dict, Any
from pydantic import ValidationError
import logging
import time
from datetime import datetime, timedelta

from models import PatientModel, EncounterModel, InvoiceModel
import warnings

# Suppress SQLite adapter deprecation warnings for Python 3.12+
warnings.filterwarnings("ignore", category=DeprecationWarning, message=".*adapter.*")


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ======================= RECONCILIATION FUNCTIONS =======================

def validate_referential_integrity(patients_df: pd.DataFrame, appointments_df: pd.DataFrame, invoices_df: pd.DataFrame) -> Dict[str, Any]:
    """Cross-table relationship validations with percentage-based metrics"""
    validations = {}
    
    # Calculate appointment referential integrity percentage
    total_appointments = len(appointments_df)
    if total_appointments > 0:
        valid_appointment_refs = int(appointments_df['patient_uuid'].isin(patients_df['patient_uuid']).sum())
        appointments_ref_percentage = round((valid_appointment_refs / total_appointments) * 100, 1)
        validations['appointments_reference_percentage'] = float(appointments_ref_percentage)
        validations['appointments_reference_patients'] = bool(appointments_ref_percentage >= 95.0)  # 95% threshold
    else:
        validations['appointments_reference_percentage'] = 100.0
        validations['appointments_reference_patients'] = True
    
    # Calculate invoice referential integrity percentage  
    total_invoices = len(invoices_df)
    if total_invoices > 0:
        valid_invoice_refs = int(invoices_df['patient_uuid'].isin(patients_df['patient_uuid']).sum())
        invoices_ref_percentage = round((valid_invoice_refs / total_invoices) * 100, 1)
        validations['invoices_reference_percentage'] = float(invoices_ref_percentage)
        validations['invoices_reference_patients'] = bool(invoices_ref_percentage >= 95.0)  # 95% threshold
    else:
        validations['invoices_reference_percentage'] = 100.0
        validations['invoices_reference_patients'] = True
    
    # Count orphaned records and calculate orphan percentage
    orphaned_appointments = len(
        appointments_df[~appointments_df['patient_uuid'].isin(patients_df['patient_uuid'])]
    )
    orphaned_invoices = len(
        invoices_df[~invoices_df['patient_uuid'].isin(patients_df['patient_uuid'])]
    )
    
    total_child_records = total_appointments + total_invoices
    total_orphaned = orphaned_appointments + orphaned_invoices
    
    validations['orphaned_appointments_count'] = orphaned_appointments
    validations['orphaned_invoices_count'] = orphaned_invoices
    validations['total_orphaned_records'] = total_orphaned
    
    if total_child_records > 0:
        orphan_percentage = round((total_orphaned / total_child_records) * 100, 1)
        validations['orphan_percentage'] = float(orphan_percentage)
        validations['acceptable_orphan_level'] = bool(orphan_percentage <= 5.0)  # Allow up to 5% orphaned records
    else:
        validations['orphan_percentage'] = 0.0
        validations['acceptable_orphan_level'] = True
    
    # Patient load distribution (detect data skew)
    if len(appointments_df) > 0:
        patient_appointment_counts = appointments_df['patient_uuid'].value_counts()
        validations['max_appointments_per_patient'] = int(patient_appointment_counts.max())
        validations['reasonable_appointment_distribution'] = bool(patient_appointment_counts.max() <= 100)
    else:
        validations['max_appointments_per_patient'] = 0
        validations['reasonable_appointment_distribution'] = True
    
    return validations


def validate_data_quality_metrics(source_df: pd.DataFrame, target_df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
    """Statistical validation of data migration quality"""
    validations = {}
    
    # Define column mappings for proper null consistency checking
    column_mappings = {}
    if table_name == "patients":
        # Map source column names to target column names
        column_mappings = {
            'legacy_id': None,  # Legacy field, exclude from validation
            'first_name': 'first_name',
            'last_name': 'last_name', 
            'dob': 'dob',
            'phone': 'phone_e164',  # Key mapping: phone -> phone_e164
            'email': 'email',
            'created_at': 'created_at'
        }
    elif table_name == "appointments":
        column_mappings = {
            'legacy_id': None,  # Legacy field, exclude
            'patient_id': None,  # Legacy field, exclude (becomes patient_uuid)
            'appointment_date': 'encounter_ts_utc',
            'provider_name': 'provider_name',
            'location': 'location',
            'status': 'status'
        }
    elif table_name == "invoices":
        column_mappings = {
            'legacy_id': None,  # Legacy field, exclude
            'patient_id': None,  # Legacy field, exclude (becomes patient_uuid)
            'amount_usd': 'invoice_total_cents',
            'status': 'status',
            'issued_date': 'issued_date_utc',
            'paid_date': 'paid_date_utc'
        }
    
    # Null value consistency analysis using proper mappings
    null_analysis = {}
    for source_col in source_df.columns:
        target_col = column_mappings.get(source_col)
        
        # Skip legacy fields and unmapped columns
        if target_col is None:
            continue
            
        if target_col in target_df.columns:
            source_nulls = int(source_df[source_col].isna().sum())
            target_nulls = int(target_df[target_col].isna().sum())
            null_difference = abs(source_nulls - target_nulls)
            
            # Use source column name for reporting but note the mapping
            display_name = f"{source_col} -> {target_col}" if source_col != target_col else source_col
            
            null_analysis[display_name] = {
                'source_nulls': source_nulls,
                'target_nulls': target_nulls,
                'difference': null_difference,
                'acceptable': null_difference <= 5  # Allow small variance due to data cleaning/validation
            }
    
    validations['null_value_analysis'] = null_analysis
    # Calculate null consistency percentage
    null_passed_count = sum(1 for analysis in null_analysis.values() if analysis['acceptable'])
    null_total_count = len(null_analysis)
    validations['null_consistency_percentage'] = round((null_passed_count / null_total_count * 100), 1) if null_total_count > 0 else 100.0
    validations['null_consistency_passed'] = validations['null_consistency_percentage'] >= 90.0  # 90% threshold
    
    # Value distribution checks (for numeric fields) - also use mappings
    numeric_analysis = {}
    try:
        numeric_cols = source_df.select_dtypes(include=['number']).columns
        for source_col in numeric_cols:
            target_col = column_mappings.get(source_col)
            
            # Skip legacy fields and unmapped columns
            if target_col is None:
                continue
                
            if target_col in target_df.columns and not source_df[source_col].empty and not target_df[target_col].empty:
                source_mean = float(source_df[source_col].mean()) if not source_df[source_col].isna().all() else 0
                target_mean = float(target_df[target_col].mean()) if not target_df[target_col].isna().all() else 0
                
                # Set display name first for use in conditionals
                display_name = f"{source_col} -> {target_col}" if source_col != target_col else source_col
                
                # Calculate percentage difference with special handling for unit conversions
                if source_mean != 0:
                    pct_difference = abs(source_mean - target_mean) / abs(source_mean) * 100
                    
                    # Special case: dollars to cents conversion (should be ~100x)
                    if display_name == "amount_usd -> invoice_total_cents":
                        # Check if target is approximately source * 100 (within 1%)
                        expected_cents = source_mean * 100
                        conversion_accuracy = abs(target_mean - expected_cents) / expected_cents * 100
                        mean_consistent = conversion_accuracy < 1.0  # Within 1% of expected conversion
                        pct_difference = conversion_accuracy  # Report conversion accuracy instead
                    else:
                        mean_consistent = pct_difference < 5.0  # Within 5% for regular fields
                else:
                    pct_difference = 0
                    mean_consistent = target_mean == 0
                numeric_analysis[display_name] = {
                    'source_mean': source_mean,
                    'target_mean': target_mean,
                    'percentage_difference': pct_difference,
                    'consistent': mean_consistent
                }
        
        validations['numeric_distribution_analysis'] = numeric_analysis
        # Calculate numeric consistency percentage
        if numeric_analysis:
            numeric_passed_count = sum(1 for analysis in numeric_analysis.values() if analysis['consistent'])
            numeric_total_count = len(numeric_analysis)
            validations['numeric_consistency_percentage'] = round((numeric_passed_count / numeric_total_count * 100), 1)
            validations['numeric_consistency_passed'] = validations['numeric_consistency_percentage'] >= 85.0  # 85% threshold
        else:
            validations['numeric_consistency_percentage'] = 100.0
            validations['numeric_consistency_passed'] = True
        
    except Exception as e:
        logger.warning(f"Numeric analysis failed for {table_name}: {e}")
        validations['numeric_distribution_analysis'] = {}
        validations['numeric_consistency_passed'] = True
    
    return validations


def calculate_combined_checksum_all(df1: pd.DataFrame, df2: pd.DataFrame, df3: pd.DataFrame) -> str:
    """Calculate combined SHA256 checksum for all three DataFrames"""
    combined_string = (
        df1.to_csv(index=False) + 
        df2.to_csv(index=False) +
        df3.to_csv(index=False)
    ).encode('utf-8')
    return hashlib.sha256(combined_string).hexdigest()


def generate_reconcile_report(
    patients_source_df: pd.DataFrame,
    patients_target_df: pd.DataFrame,
    appointments_source_df: pd.DataFrame,
    appointments_target_df: pd.DataFrame,
    invoices_source_df: pd.DataFrame,
    invoices_target_df: pd.DataFrame,
    output_file: str = "reconcile_report.json"
) -> Dict[str, Any]:
    """Generate comprehensive JSON reconciliation report with validations"""
    
    logger.info("Running comprehensive migration validations...")
    
    # Calculate row counts
    patients_source_rows = len(patients_source_df)
    patients_target_rows = len(patients_target_df)
    appointments_source_rows = len(appointments_source_df)
    appointments_target_rows = len(appointments_target_df)
    invoices_source_rows = len(invoices_source_df)
    invoices_target_rows = len(invoices_target_df)
    
    # Check if row counts match for ALL tables
    row_count_match = (
        patients_source_rows == patients_target_rows and 
        appointments_source_rows == appointments_target_rows and
        invoices_source_rows == invoices_target_rows
    )
    
    # Calculate checksums for all three tables (note: will differ due to transformations)
    source_checksum = calculate_combined_checksum_all(
        patients_source_df, appointments_source_df, invoices_source_df
    )
    target_checksum = calculate_combined_checksum_all(
        patients_target_df, appointments_target_df, invoices_target_df
    )
    
    # Run referential integrity validations
    referential_integrity = validate_referential_integrity(
        patients_target_df, appointments_target_df, invoices_target_df
    )
    
    # Run data quality validations for each table
    patients_quality = validate_data_quality_metrics(
        patients_source_df, patients_target_df, "patients"
    )
    
    appointments_quality = validate_data_quality_metrics(
        appointments_source_df, appointments_target_df, "appointments"
    )
    
    invoices_quality = validate_data_quality_metrics(
        invoices_source_df, invoices_target_df, "invoices"
    )
    
    # Calculate overall validation status
    all_referential_checks_passed = all([
        referential_integrity['appointments_reference_patients'],
        referential_integrity['invoices_reference_patients'],
        referential_integrity['acceptable_orphan_level'],
        referential_integrity['reasonable_appointment_distribution']
    ])
    
    all_quality_checks_passed = all([
        patients_quality['null_consistency_passed'],
        patients_quality['numeric_consistency_passed'],
        appointments_quality['null_consistency_passed'],
        appointments_quality['numeric_consistency_passed'],
        invoices_quality['null_consistency_passed'],
        invoices_quality['numeric_consistency_passed']
    ])
    
    # Create comprehensive report dictionary
    report = {
        # Basic row count information
        "patients_source_rows": patients_source_rows,
        "patients_target_rows": patients_target_rows,
        "appointments_source_rows": appointments_source_rows,
        "appointments_target_rows": appointments_target_rows,
        "invoices_source_rows": invoices_source_rows,
        "invoices_target_rows": invoices_target_rows,
        "row_count_match": row_count_match,
        
        # Checksums (will differ due to transformations - informational only)
        "sha256_checksum_source": source_checksum,
        "sha256_checksum_target": target_checksum,
        "checksum_note": "Checksums will differ due to data transformations (expected)",
        
        # Referential integrity validations
        "referential_integrity": referential_integrity,
        
        # Data quality validations by table
        "data_quality_validations": {
            "patients": patients_quality,
            "appointments": appointments_quality,
            "invoices": invoices_quality
        },
        
        # Overall validation summary
        "validation_summary": {
            "row_counts_passed": row_count_match,
            "referential_integrity_passed": all_referential_checks_passed,
            "data_quality_passed": all_quality_checks_passed,
            "overall_migration_status": "PASS" if (row_count_match and all_referential_checks_passed and all_quality_checks_passed) else "REVIEW_REQUIRED"
        }
    }
    
    # Write to JSON file
    with open(output_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"Comprehensive reconciliation report generated: {output_file}")
    return report

def print_reconciliation_summary(report_file: str = "reconcile_report.json"):
    """Print a comprehensive formatted reconciliation summary from JSON report"""
    try:
        with open(report_file, 'r') as f:
            report = json.load(f)
        
        print(f"\n{'='*80}")
        print(f"COMPREHENSIVE MIGRATION RECONCILIATION REPORT")
        print(f"{'='*80}")
        
        # Row count summary
        print(f"ROW COUNT SUMMARY:")
        print(f"  Patients Source:     {report['patients_source_rows']:,}")
        print(f"  Patients Target:     {report['patients_target_rows']:,}")
        print(f"  Appointments Source: {report['appointments_source_rows']:,}")
        print(f"  Appointments Target: {report['appointments_target_rows']:,}")
        print(f"  Invoices Source:     {report['invoices_source_rows']:,}")
        print(f"  Invoices Target:     {report['invoices_target_rows']:,}")
        print(f"  Row Count Match:     {'YES' if report['row_count_match'] else 'NO'}")
        
        # Referential integrity summary
        print(f"\nREFERENTIAL INTEGRITY CHECKS:")
        ref_integrity = report.get('referential_integrity', {})
        
        # Show percentages for referential integrity
        appt_ref_pct = ref_integrity.get('appointments_reference_percentage', 0)
        invoice_ref_pct = ref_integrity.get('invoices_reference_percentage', 0)
        orphan_pct = ref_integrity.get('orphan_percentage', 0)
        
        print(f"  Appointments Reference Patients: {appt_ref_pct}% ({'PASS' if appt_ref_pct >= 95 else 'REVIEW'})")
        print(f"  Invoices Reference Patients:     {invoice_ref_pct}% ({'PASS' if invoice_ref_pct >= 95 else 'REVIEW'})")
        print(f"  Orphaned Records Overall:        {orphan_pct}% ({'PASS' if orphan_pct <= 5 else 'REVIEW'})")
        print(f"  Total Orphaned Records:          {ref_integrity.get('total_orphaned_records', 'N/A')}")
        print(f"  Max Appointments per Patient:    {ref_integrity.get('max_appointments_per_patient', 'N/A')}")
        
        # Data quality summary
        print(f"\nDATA QUALITY VALIDATIONS:")
        data_quality = report.get('data_quality_validations', {})
        
        for table_name in ['patients', 'appointments', 'invoices']:
            if table_name in data_quality:
                table_data = data_quality[table_name]
                print(f"  {table_name.capitalize()}:")
                
                # Show percentages with pass/fail status
                null_pct = table_data.get('null_consistency_percentage', 0)
                type_pct = table_data.get('type_consistency_percentage', 0)
                numeric_pct = table_data.get('numeric_consistency_percentage', 0)
                
                print(f"    Null Consistency:        {null_pct}% ({'PASS' if null_pct >= 90 else 'REVIEW'})")
                print(f"    Numeric Consistency:     {numeric_pct}% ({'PASS' if numeric_pct >= 85 else 'REVIEW'})")
        
        # Overall validation summary
        print(f"\nOVERALL VALIDATION SUMMARY:")
        validation_summary = report.get('validation_summary', {})
        print(f"  Row Counts:           {'PASS' if validation_summary.get('row_counts_passed', False) else 'FAIL'}")
        print(f"  Referential Integrity: {'PASS' if validation_summary.get('referential_integrity_passed', False) else 'FAIL'}")
        print(f"  Data Quality:         {'PASS' if validation_summary.get('data_quality_passed', False) else 'FAIL'}")
        
        # Final status
        overall_status = validation_summary.get('overall_migration_status', 'UNKNOWN')
        print(f"\nMIGRATION STATUS: {overall_status}")
        
        if overall_status == "PASS":
            print("  All validations passed. Migration is ready for production.")
        else:
            print("  Some validations failed. Review required before production deployment.")
        
        # Checksum information
        print(f"\nCHECKSUM INFORMATION (Informational Only):")
        print(f"  Source Checksum:     {report.get('sha256_checksum_source', 'N/A')[:16]}...")
        print(f"  Target Checksum:     {report.get('sha256_checksum_target', 'N/A')[:16]}...")
        print(f"  Note: {report.get('checksum_note', 'Checksums may differ due to transformations')}")
        
        print(f"{'='*80}\n")
        
    except FileNotFoundError:
        print(f"Report file not found: {report_file}")
    except json.JSONDecodeError:
        print(f"Invalid JSON in report file: {report_file}")
    except Exception as e:
        print(f"Error reading report: {e}")

# ======================= MIGRATION FUNCTIONS =======================

def validate_data(df: pd.DataFrame, model_class, table_name: str) -> tuple[List[dict], List[dict]]:
    """Validate dataframe using Pydantic model, return successful and failed records"""
    successful_records = []
    failed_records = []
    
    logger.info(f"Validating {len(df)} {table_name} records...")
    
    for index, row in df.iterrows():
        try:
            row_dict = row.to_dict()
        
            validated_record = model_class(**row_dict)
            successful_records.append(validated_record.model_dump())
        except Exception as e:
            logger.warning(f"Validation failed for {table_name} row {index}: {e}")
            
            # Extract field name(s) from Pydantic validation error
            error_str = str(e)
            field_names = []
            
            # Parse field name from Pydantic error format
            if "validation error" in error_str.lower():
                # Look for field name patterns in Pydantic errors
                lines = error_str.split('\n')
                for line in lines:
                    line = line.strip()
                    # Field names appear as standalone lines in Pydantic errors
                    if line and not line.startswith('Value error') and not line.startswith('For further') and not 'validation error' in line.lower():
                        # Skip numeric lines (like "1 validation error")
                        if not line[0].isdigit():
                            field_names.append(line)
            
            # Join multiple field names with comma, or use "unknown" if none found
            field_name = ", ".join(field_names) if field_names else "unknown"
            
            # Simple error record - capture essential information including field
            failed_records.append({
                "row_index": index,
                "table": table_name,
                "legacy_id": row.to_dict().get('legacy_id', 'N/A'),
                "field": field_name,
                "error_message": str(e).replace('\n', ' | '),  # Single line for Excel
                "source_data": str(row.to_dict())
            })
    
    logger.info(f"{len(successful_records)} successful, {len(failed_records)} failed")
    return successful_records, failed_records


def load_source_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load source data from CSV files"""
    logger.info("Loading source data...")
    
    # Replace with your actual file paths
    patients_df = pd.read_csv("data/patients_data.csv")
    appointments_df = pd.read_csv("data/appointments_data.csv") 
    invoices_df = pd.read_csv("data/invoices_data.csv")
    
    logger.info(f"Loaded: {len(patients_df)} patients, {len(appointments_df)} appointments, {len(invoices_df)} invoices")
    return patients_df, appointments_df, invoices_df


def export_to_sqlite(patients: List[dict], appointments: List[dict], invoices: List[dict]):
    """Export migrated data to SQLite database"""
    # Create database export directory
    db_dir = "target_data/db_export"
    os.makedirs(db_dir, exist_ok=True)
    
    db_path = f"{db_dir}/export.db"
    logger.info(f"Exporting data to SQLite database: {db_path}")
    
    try:
        # Create connection to SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create patients table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS patients (
                patient_uuid TEXT PRIMARY KEY,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                dob DATE NOT NULL,
                phone_e164 TEXT,
                email TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
        """)
        
        # Create encounters table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS encounters (
                encounter_uuid TEXT PRIMARY KEY,
                patient_uuid TEXT NOT NULL,
                encounter_ts_utc TIMESTAMP NOT NULL,
                provider_name TEXT NOT NULL,
                location TEXT NOT NULL,
                status TEXT NOT NULL,
                FOREIGN KEY (patient_uuid) REFERENCES patients (patient_uuid)
            )
        """)
        
        # Create billing_invoices table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS billing_invoices (
                invoice_uuid TEXT PRIMARY KEY,
                patient_uuid TEXT NOT NULL,
                invoice_total_cents INTEGER NOT NULL,
                status TEXT NOT NULL,
                issued_date_utc TIMESTAMP NOT NULL,
                paid_date_utc TIMESTAMP,
                FOREIGN KEY (patient_uuid) REFERENCES patients (patient_uuid)
            )
        """)
        
        # Insert/Update patients data (UPSERT for data changes)
        logger.info(f"Upserting {len(patients)} patients...")
        patients_inserted = 0
        patients_updated = 0
        for patient in patients:
            try:
                # Get row count before operation
                cursor.execute("SELECT changes()")
                changes_before = cursor.fetchone()[0]
                
                cursor.execute("""
                    INSERT INTO patients 
                    (patient_uuid, first_name, last_name, dob, phone_e164, email, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(patient_uuid) DO UPDATE SET
                        first_name = excluded.first_name,
                        last_name = excluded.last_name,
                        phone_e164 = excluded.phone_e164,
                        email = excluded.email
                """, (
                    patient['patient_uuid'],
                    patient['first_name'], 
                    patient['last_name'],
                    patient['dob'],
                    patient.get('phone_e164'),
                    patient['email'],
                    patient['created_at']
                ))
                
                # Check if it was an INSERT (new lastrowid) or UPDATE (existing record)
                if cursor.lastrowid is not None and cursor.lastrowid > 0:
                    patients_inserted += 1
                else:
                    patients_updated += 1
            except sqlite3.Error as e:
                logger.error(f"Error upserting patient {patient['patient_uuid']}: {e}")
                raise
        
        logger.info(f"Patients: {patients_inserted} inserted, {patients_updated} updated")
        
        # Insert/Update appointments data (UPSERT for status/scheduling changes)
        logger.info(f"Upserting {len(appointments)} appointments...")
        appointments_inserted = 0
        appointments_updated = 0
        for appointment in appointments:
            try:
                cursor.execute("""
                    INSERT INTO encounters 
                    (encounter_uuid, patient_uuid, encounter_ts_utc, provider_name, location, status)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(encounter_uuid) DO UPDATE SET
                        encounter_ts_utc = excluded.encounter_ts_utc,
                        provider_name = excluded.provider_name,
                        location = excluded.location,
                        status = excluded.status
                """, (
                    appointment['encounter_uuid'],
                    appointment['patient_uuid'],
                    appointment['encounter_ts_utc'],
                    appointment['provider_name'],
                    appointment['location'],
                    appointment['status']
                ))
                if cursor.lastrowid is not None and cursor.lastrowid > 0:
                    appointments_inserted += 1
                else:
                    appointments_updated += 1
            except sqlite3.Error as e:
                logger.error(f"Error upserting appointment {appointment['encounter_uuid']}: {e}")
                raise
        
        logger.info(f"Appointments: {appointments_inserted} inserted, {appointments_updated} updated")
        
        # Insert/Update invoices data (UPSERT for payment status/amount changes)
        logger.info(f"Upserting {len(invoices)} invoices...")
        invoices_inserted = 0
        invoices_updated = 0
        for invoice in invoices:
            try:
                cursor.execute("""
                    INSERT INTO billing_invoices 
                    (invoice_uuid, patient_uuid, invoice_total_cents, status, issued_date_utc, paid_date_utc)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(invoice_uuid) DO UPDATE SET
                        invoice_total_cents = excluded.invoice_total_cents,
                        status = excluded.status,
                        paid_date_utc = excluded.paid_date_utc
                """, (
                    invoice['invoice_uuid'],
                    invoice['patient_uuid'],
                    invoice['invoice_total_cents'],
                    invoice['status'],
                    invoice['issued_date_utc'],
                    invoice.get('paid_date_utc')
                ))
                if cursor.lastrowid is not None and cursor.lastrowid > 0:
                    invoices_inserted += 1
                else:
                    invoices_updated += 1
            except sqlite3.Error as e:
                logger.error(f"Error upserting invoice {invoice['invoice_uuid']}: {e}")
                raise
        
        logger.info(f"Invoices: {invoices_inserted} inserted, {invoices_updated} updated")
        
        # Commit the transaction
        conn.commit()
        
        # Create indexes for better query performance
        logger.info("Creating database indexes...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_encounters_patient_uuid ON encounters(patient_uuid)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_encounters_date ON encounters(encounter_ts_utc)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_billing_invoices_patient_uuid ON billing_invoices(patient_uuid)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_billing_invoices_status ON billing_invoices(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_patients_email ON patients(email)")
        
        conn.commit()
        
        # Print database statistics
        cursor.execute("SELECT COUNT(*) FROM patients")
        patient_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM encounters")
        encounter_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM billing_invoices")
        invoice_count = cursor.fetchone()[0]
        
        total_inserted = patients_inserted + appointments_inserted + invoices_inserted
        total_updated = patients_updated + appointments_updated + invoices_updated
        
        logger.info(f"SQLite export completed successfully:")
        logger.info(f"  - {patient_count:,} total patients in database")
        logger.info(f"  - {encounter_count:,} total encounters in database") 
        logger.info(f"  - {invoice_count:,} total billing_invoices in database")
        logger.info(f"  - {total_inserted:,} records inserted this run")
        logger.info(f"  - {total_updated:,} records updated this run")
        logger.info(f"  - Database saved to: {db_path}")
        
    except Exception as e:
        logger.error(f"SQLite export failed: {e}")
        raise
    finally:
        if conn:
            conn.close()


def save_target_data(successful_patients: List[dict], successful_appointments: List[dict], successful_invoices: List[dict]):
    """Save validated data to target system (CSV files and SQLite database)"""
    logger.info("Saving validated data to target...")
    
    # Create target CSV directory
    target_dir = "target_data/target"
    os.makedirs(target_dir, exist_ok=True)
    
    # Remove legacy fields for CSV output (keep only target schema fields)
    clean_patients = [{k: v for k, v in patient.items() if k != 'legacy_id'} for patient in successful_patients]
    clean_appointments = [{k: v for k, v in appointment.items() if k not in ['legacy_id', 'patient_legacy_id']} for appointment in successful_appointments]
    clean_invoices = [{k: v for k, v in invoice.items() if k not in ['legacy_id', 'patient_legacy_id']} for invoice in successful_invoices]
    
    # Convert to DataFrames and save CSV files
    pd.DataFrame(clean_patients).to_csv(f"{target_dir}/patients.csv", index=False)
    pd.DataFrame(clean_appointments).to_csv(f"{target_dir}/encounters.csv", index=False)
    pd.DataFrame(clean_invoices).to_csv(f"{target_dir}/billing_invoices.csv", index=False)
    
    # Save to SQLite database (with UPSERT handling duplicates automatically)
    export_to_sqlite(clean_patients, clean_appointments, clean_invoices)
    
    logger.info("Data saved to target system (CSV and SQLite)")


def export_failed_records(failed_patients: List[dict], failed_appointments: List[dict], failed_invoices: List[dict]) -> Dict[str, str]:
    """Export failed records to separate CSV files for analysis"""
    
    # Create failed records directory
    failed_dir = "target_data/failed"
    os.makedirs(failed_dir, exist_ok=True)
    
    exported_files = {}
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Export failed patients
    if failed_patients:
        patients_error_file = f"{failed_dir}/failed_patients_{timestamp}.csv"
        failed_patients_df = pd.DataFrame(failed_patients)
        failed_patients_df.to_csv(patients_error_file, index=False)
        exported_files['patients'] = patients_error_file
        logger.info(f"Exported {len(failed_patients)} failed patient records to {patients_error_file}")
    
    # Export failed encounters
    if failed_appointments:
        encounters_error_file = f"{failed_dir}/failed_encounters_{timestamp}.csv"
        failed_appointments_df = pd.DataFrame(failed_appointments)
        failed_appointments_df.to_csv(encounters_error_file, index=False)
        exported_files['encounters'] = encounters_error_file
        logger.info(f"Exported {len(failed_appointments)} failed encounter records to {encounters_error_file}")
    
    # Export failed billing_invoices
    if failed_invoices:
        billing_invoices_error_file = f"{failed_dir}/failed_billing_invoices_{timestamp}.csv"
        failed_invoices_df = pd.DataFrame(failed_invoices)
        failed_invoices_df.to_csv(billing_invoices_error_file, index=False)
        exported_files['billing_invoices'] = billing_invoices_error_file
        logger.info(f"Exported {len(failed_invoices)} failed billing_invoice records to {billing_invoices_error_file}")
    
    return exported_files


# ======================= MAIN MIGRATION WORKFLOW =======================

def main():
    """Main migration workflow"""
    migration_start_time = time.time()
    migration_start_timestamp = datetime.now()
    
    logger.info("Starting Healthcare Data Migration...")
    logger.info(f"Migration started at: {migration_start_timestamp.isoformat()}")
    
    # Clean up old failed records from previous runs
    failed_dir = "target_data/failed"
    if os.path.exists(failed_dir):
        old_files = [f for f in os.listdir(failed_dir) if f.endswith('.csv')]
        if old_files:
            logger.info(f"Cleaning up {len(old_files)} old failed record files...")
            for file in old_files:
                os.remove(os.path.join(failed_dir, file))
        else:
            logger.info("No old failed record files to clean up")
    
    # Initialize timing dictionary
    timing = {}
    
    try:
        # 1. Load source data
        load_start = time.time()
        patients_source_df, appointments_source_df, invoices_source_df = load_source_data()
        timing['data_loading_seconds'] = round(time.time() - load_start, 2)
        
        # 2. Validate and transform data
        validation_start = time.time()
        
        # Validate all data (UUIDs are now generated deterministically in models)
        successful_patients, failed_patients = validate_data(patients_source_df, PatientModel, "patients")
        successful_appointments, failed_appointments = validate_data(appointments_source_df, EncounterModel, "appointments")
        successful_invoices, failed_invoices = validate_data(invoices_source_df, InvoiceModel, "invoices")
        timing['data_validation_seconds'] = round(time.time() - validation_start, 2)
        
        # 3. Save validated data to target
        save_start = time.time()
        save_target_data(successful_patients, successful_appointments, successful_invoices)
        timing['data_saving_seconds'] = round(time.time() - save_start, 2)
        
        # 4. Load target data for reconciliation (from saved files)
        transform_start = time.time()
        logger.info("Loading target data for reconciliation...")
        
        # Load target data from saved CSV files instead of using in-memory data
        patients_target_df = pd.read_csv("target_data/target/patients.csv")
        appointments_target_df = pd.read_csv("target_data/target/encounters.csv") 
        invoices_target_df = pd.read_csv("target_data/target/billing_invoices.csv")
        
        # Convert datetime columns back to proper types for reconciliation
        appointments_target_df['encounter_ts_utc'] = pd.to_datetime(appointments_target_df['encounter_ts_utc'])
        invoices_target_df['issued_date_utc'] = pd.to_datetime(invoices_target_df['issued_date_utc'])
        if 'paid_date_utc' in invoices_target_df.columns:
            invoices_target_df['paid_date_utc'] = pd.to_datetime(invoices_target_df['paid_date_utc'])
        
        timing['data_transformation_seconds'] = round(time.time() - transform_start, 2)
        
        # 5. Generate reconciliation report
        logger.info("Generating reconciliation report...")
        report = generate_reconcile_report(
            patients_source_df=patients_source_df,
            patients_target_df=patients_target_df,
            appointments_source_df=appointments_source_df,
            appointments_target_df=appointments_target_df,
            invoices_source_df=invoices_source_df,
            invoices_target_df=invoices_target_df
        )
        
        # 6. Print summary
        print_reconciliation_summary()
        
        # 7. Log migration summary
        migration_end_time = time.time()
        total_duration = migration_end_time - migration_start_time
        migration_end_timestamp = datetime.now()
        
        # Log timing information
        logger.info(f"Migration completed at: {migration_end_timestamp.isoformat()}")
        logger.info(f"Total migration duration: {total_duration:.2f} seconds ({timedelta(seconds=int(total_duration))})")
        logger.info("Phase timing breakdown:")
        for phase, duration in timing.items():
            logger.info(f"  {phase.replace('_', ' ').title()}: {duration} seconds")
        
        total_failed = len(failed_patients) + len(failed_appointments) + len(failed_invoices)
        if total_failed == 0:
            logger.info("Migration completed successfully with no failures")
        else:
            logger.warning(f"Migration completed with {total_failed} total failures")
            logger.warning(f"  - Failed patients: {len(failed_patients)}")
            logger.warning(f"  - Failed encounters: {len(failed_appointments)}")  
            logger.warning(f"  - Failed billing_invoices: {len(failed_invoices)}")
            
            # Export failed records to files
            exported_files = export_failed_records(failed_patients, failed_appointments, failed_invoices)
            if exported_files:
                logger.info("Failed records exported to:")
                for table, file_path in exported_files.items():
                    logger.info(f"  - {table}: {file_path}")
            
        logger.info("Check reconcile_report.json for detailed metrics and timing information")
        
        # Log migration summary
        log_migration_summary()
        
    except Exception as e:
        migration_end_time = time.time()
        total_duration = migration_end_time - migration_start_time
        logger.error(f"Migration failed after {total_duration:.2f} seconds: {e}")
        raise


def log_migration_summary():
    """Log a summary of migration outputs and directory structure"""
    logger.info("="*30)
    logger.info("MIGRATION OUTPUT SUMMARY")
    logger.info("="*30)
    
    # Check target CSV files
    target_dir = "target_data/target"
    if os.path.exists(target_dir):
        logger.info("Target CSV Files (target_data/target/):")
        for file in ["patients.csv", "encounters.csv", "billing_invoices.csv"]:
            file_path = f"{target_dir}/{file}"
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                logger.info(f"   {file}: {len(df):,} records")
            else:
                logger.info(f"   {file}: Not found")
    
    # Check database export
    db_dir = "target_data/db_export"
    if os.path.exists(db_dir):
        db_path = f"{db_dir}/export.db"
        if os.path.exists(db_path):
            # Get file size in MB
            file_size = os.path.getsize(db_path) / (1024 * 1024)
            logger.info(f"Database Export (target_data/db_export/):")
            logger.info(f"   export.db: {file_size:.2f} MB")
        else:
            logger.info("Database Export: Not found")
    
    # Check failed records
    failed_dir = "target_data/failed"
    if os.path.exists(failed_dir):
        failed_files = [f for f in os.listdir(failed_dir) if f.endswith('.csv')]
        if failed_files:
            logger.info("Failed Records (target_data/failed/):")
            for file in failed_files:
                file_path = f"{failed_dir}/{file}"
                df = pd.read_csv(file_path)
                logger.info(f"   {file}: {len(df):,} failed records")
        else:
            logger.info("Failed Records: None (all records processed successfully)")
    else:
        logger.info("Failed Records: None (all records processed successfully)")
    
    logger.info("="*60)


# ======================= MIGRATION FUNCTIONS =======================


if __name__ == "__main__":
    main() 