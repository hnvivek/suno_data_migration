#!/usr/bin/env python3
"""
Unit tests for Pydantic models
"""

import unittest
from datetime import datetime, date
from zoneinfo import ZoneInfo
from pydantic import ValidationError

from models import PatientModel, EncounterModel, InvoiceModel


class TestPatientModel(unittest.TestCase):
    
    def test_valid_patient(self):
        """Test valid patient data"""
        patient_data = {
            'legacy_id': 1,
            'first_name': 'John',
            'last_name': 'Doe',
            'dob': '1990-01-15',
            'phone': '(555) 123-4567',
            'email': 'JOHN.DOE@EXAMPLE.COM',
            'created_at': '2022-01-01 10:30'
        }
        
        patient = PatientModel(**patient_data)
        
        self.assertEqual(patient.legacy_id, 1)
        self.assertEqual(patient.first_name, 'John')
        self.assertEqual(patient.dob, date(1990, 1, 15))
        self.assertEqual(patient.phone_e164, '+15551234567')  # E.164 format - corrected field name
        self.assertEqual(patient.email, 'john.doe@example.com')  # lowercase
        
        # Check timezone conversion
        self.assertEqual(patient.created_at.tzinfo, ZoneInfo('UTC'))
        
        # Check UUID generation
        self.assertIsInstance(patient.patient_uuid, str)
        self.assertEqual(len(patient.patient_uuid), 32)  # hex format
    
    def test_phone_number_formats(self):
        """Test various phone number formats"""
        test_cases = [
            ('(555) 123-4567', '+15551234567'),
            ('555-123-4567', '+15551234567'),
            ('555.123.4567', '+15551234567'),
            ('5551234567', '+15551234567'),
            ('invalid', None),
            ('', None),
            (None, None)
        ]
        
        base_data = {
            'legacy_id': 1,
            'first_name': 'John',
            'last_name': 'Doe',
            'dob': '1990-01-15',
            'email': 'john@example.com',
            'created_at': '2022-01-01 10:30'
        }
        
        for input_phone, expected in test_cases:
            patient_data = base_data.copy()
            patient_data['phone'] = input_phone
            
            patient = PatientModel(**patient_data)
            self.assertEqual(patient.phone_e164, expected, f"Failed for input: {input_phone}")  # corrected field name
    
    def test_invalid_date(self):
        """Test invalid date handling - should raise validation error"""
        patient_data = {
            'legacy_id': 1,
            'first_name': 'John',
            'last_name': 'Doe',
            'dob': 'invalid-date',
            'phone': '555-123-4567',
            'email': 'john@example.com',
            'created_at': '2022-01-01 10:30'
        }
        
        with self.assertRaises(ValidationError):
            PatientModel(**patient_data)
    
    def test_blank_phone_becomes_null(self):
        """Test that blank phone becomes None per spec"""
        patient_data = {
            'legacy_id': 1,
            'first_name': 'John',
            'last_name': 'Doe',
            'dob': '1990-01-15',
            'phone': '',  # Blank should become NULL
            'email': 'john@example.com',
            'created_at': '2022-01-01 10:30'
        }
        
        patient = PatientModel(**patient_data)
        self.assertIsNone(patient.phone_e164)  # corrected field name


class TestEncounterModel(unittest.TestCase):
    
    def test_valid_appointment(self):
        """Test valid appointment data"""
        appointment_data = {
            'legacy_id': 1,
            'patient_id': 123,
            'patient_uuid': b'1234567890123456',  # Added required patient_uuid as bytes
            'appointment_date': '2023-01-15 14:30',
            'provider_name': 'Dr. Smith',
            'location': 'Main Clinic',
            'status': 'SCHEDULED'
        }
        
        appointment = EncounterModel(**appointment_data)
        
        self.assertEqual(appointment.legacy_id, 1)
        self.assertEqual(appointment.patient_legacy_id, 123)  # corrected field name
        self.assertEqual(appointment.status, 'scheduled')  # mapped to lowercase
        
        # Check timezone conversion
        self.assertEqual(appointment.encounter_ts_utc.tzinfo, ZoneInfo('UTC'))  # corrected field name
        
        # Check UUID generation
        self.assertIsInstance(appointment.encounter_uuid, str)
        self.assertEqual(len(appointment.encounter_uuid), 32)  # hex format
    
    def test_status_mapping(self):
        """Test status value mapping per requirements"""
        test_cases = [
            ('SCHEDULED', 'scheduled'),
            ('CANCELLED', 'cancelled'),
            ('COMPLETED', 'completed'),
            ('scheduled', 'scheduled'),  # already lowercase
            ('unknown_status', 'unknown_status')  # fallback to lowercase
        ]
        
        base_data = {
            'legacy_id': 1,
            'patient_id': 123,
            'patient_uuid': b'1234567890123456',  # Added required patient_uuid as bytes
            'appointment_date': '2023-01-15 14:30',
            'provider_name': 'Dr. Smith',
            'location': 'Main Clinic'
        }
        
        for input_status, expected in test_cases:
            appointment_data = base_data.copy()
            appointment_data['status'] = input_status
            
            appointment = EncounterModel(**appointment_data)
            self.assertEqual(appointment.status, expected, f"Failed for status: {input_status}")


class TestInvoiceModel(unittest.TestCase):
    
    def test_valid_invoice_paid(self):
        """Test valid paid invoice"""
        invoice_data = {
            'legacy_id': 1,
            'patient_id': 123,
            'patient_uuid': b'1234567890123456',  # Added required patient_uuid as bytes
            'amount_usd': 150.75,
            'status': 'PAID',
            'issued_date': '2023-01-15 10:00',
            'paid_date': '2023-01-20 15:30'
        }
        
        invoice = InvoiceModel(**invoice_data)
        
        self.assertEqual(invoice.legacy_id, 1)
        self.assertEqual(invoice.invoice_total_cents, 15075)  # corrected field name and value (cents)
        self.assertEqual(invoice.status, 'paid')  # mapped to lowercase
        self.assertIsNotNone(invoice.paid_date_utc)  # corrected field name
        
        # Check timezone conversion
        self.assertEqual(invoice.issued_date_utc.tzinfo, ZoneInfo('UTC'))  # corrected field name
        self.assertEqual(invoice.paid_date_utc.tzinfo, ZoneInfo('UTC'))  # corrected field name
        
        # Check UUID generation
        self.assertIsInstance(invoice.invoice_uuid, str)
        self.assertEqual(len(invoice.invoice_uuid), 32)  # hex format
    
    def test_valid_invoice_open(self):
        """Test valid open invoice with no paid date"""
        invoice_data = {
            'legacy_id': 1,
            'patient_id': 123,
            'patient_uuid': b'1234567890123456',  # Added required patient_uuid as bytes
            'amount_usd': 75.50,
            'status': 'OPEN',
            'issued_date': '2023-01-15 10:00',
            'paid_date': ''  # empty string should become None
        }
        
        invoice = InvoiceModel(**invoice_data)
        
        self.assertEqual(invoice.status, 'open')  # mapped to lowercase
        self.assertIsNone(invoice.paid_date_utc)  # corrected field name
        self.assertEqual(invoice.invoice_total_cents, 7550)  # corrected field name and value (cents)
    
    def test_status_mapping(self):
        """Test invoice status mapping per requirements"""
        test_cases = [
            ('OPEN', 'open'),
            ('PAID', 'paid'),
            ('open', 'open'),  # already lowercase
        ]
        
        base_data = {
            'legacy_id': 1,
            'patient_id': 123,
            'patient_uuid': b'1234567890123456',  # Added required patient_uuid as bytes
            'amount_usd': 100.0,
            'issued_date': '2023-01-15 10:00',
            'paid_date': None
        }
        
        for input_status, expected in test_cases:
            invoice_data = base_data.copy()
            invoice_data['status'] = input_status
            
            invoice = InvoiceModel(**invoice_data)
            self.assertEqual(invoice.status, expected, f"Failed for status: {input_status}")


if __name__ == '__main__':
    unittest.main() 