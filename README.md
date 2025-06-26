# Healthcare Data Migration

A simple tool to migrate patient, appointment, and invoice data from legacy EMR systems to a new format.

## What it does

Takes your old CSV files and converts them to a clean, standardized format. Handles data validation, duplicate checking, and exports everything to both CSV files and a SQLite database.

## Requirements

- Python 3.8 or higher

## Quick start

1. **Create virtual environment** (recommended)
   ```bash
   python3 -m venv venv
   ```

2. **Activate virtual environment**
   ```bash
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the migration**
   ```bash
   python3 main.py
   ```

5. **Run test cases** (optional)
   ```bash
   python3 test_models.py
   ```

That's it! The tool will process everything and show you a summary. For the sample dataset (1,000 patients, 1,600 appointments, 1,400 invoices), processing typically takes about `0.3 seconds`.

## What you get

After running, check the `target_data/` folder:

- **target/** - Clean CSV files ready to import
- **db_export/** - SQLite database with your migrated data
- **failed/** - Any records that couldn't be processed (if any)

The tool also creates a `reconcile_report.json` with detailed stats about the migration.

## Troubleshooting

- Check the failed records in `target_data/failed/` - they're Excel-friendly and show exactly what needs fixing
- Look at the console output for a quick summary
- The migration is safe to run multiple times

## Verify your data

After migration, you can check your data integrity:

```sql
-- Connect to the database (make sure you've run the migration first)
sqlite3 target_data/db_export/export.db

-- 1. Check primary keys and foreign key integrity
SELECT 
    (SELECT COUNT(*) FROM patients) as total_patients,
    (SELECT COUNT(DISTINCT patient_uuid) FROM patients) as unique_patients,
    (SELECT COUNT(*) FROM encounters e LEFT JOIN patients p ON e.patient_uuid = p.patient_uuid WHERE p.patient_uuid IS NULL) as orphaned_encounters,
    (SELECT COUNT(*) FROM billing_invoices i LEFT JOIN patients p ON i.patient_uuid = p.patient_uuid WHERE p.patient_uuid IS NULL) as orphaned_invoices;

-- 2. Verify idempotency after second run (record counts should stay the same)
SELECT 
    'patients' as table_name, COUNT(*) as record_count FROM patients
UNION ALL
SELECT 'encounters', COUNT(*) FROM encounters  
UNION ALL
SELECT 'billing_invoices', COUNT(*) FROM billing_invoices;
```


## Sample data

Run `python3 test_models.py` to make sure everything works with test data first.

## Future Improvements

With more time, we would add:

- **Incremental updates** - Only process new or changed records
- **Rollback capability** - Undo migrations if something goes wrong

