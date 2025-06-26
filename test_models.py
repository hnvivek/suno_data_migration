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
            'phone': '(818) 555-1234',  
            'email': 'JOHN.DOE@EXAMPLE.COM',
            'created_at': '2022-01-01 10:30'
        }
        
        patient = PatientModel(**patient_data)
        
        self.assertEqual(patient.legacy_id, 1)
        self.assertEqual(patient.first_name, 'John')
        self.assertEqual(patient.dob, date(1990, 1, 15))
        self.assertEqual(patient.phone_e164, '+18185551234')  # E.164 format with valid phone number
        self.assertEqual(patient.email, 'john.doe@example.com')  # lowercase
        
        # Check timezone conversion
        self.assertEqual(patient.created_at.tzinfo, ZoneInfo('UTC'))
        
        # Check UUID generation
        self.assertIsInstance(patient.patient_uuid, str)
        self.assertEqual(len(patient.patient_uuid), 32)  # hex format
    
    def test_phone_number_formats(self):
        """Test various phone number formats - valid ones should convert, invalid ones should raise ValidationError"""
        # Test valid phone formats that should convert successfully
        valid_test_cases = [
            ('(818) 555-1234', '+18185551234'),
            ('818-555-1234', '+18185551234'),
            ('818.555.1234', '+18185551234'),
            ('8185551234', '+18185551234'),
            ('', None),  # blank should become None
            (None, None)  # None should stay None
        ]
        
        base_data = {
            'legacy_id': 1,
            'first_name': 'John',
            'last_name': 'Doe',
            'dob': '1990-01-15',
            'email': 'john@example.com',
            'created_at': '2022-01-01 10:30'
        }
        
        # Test valid phone numbers
        for input_phone, expected in valid_test_cases:
            patient_data = base_data.copy()
            patient_data['phone'] = input_phone
            
            patient = PatientModel(**patient_data)
            self.assertEqual(patient.phone_e164, expected, f"Failed for input: {input_phone}")
        
        # Test invalid phone numbers that should raise ValidationError
        invalid_phones = [
            '(555) 123-4567',  # Invalid area code
            'invalid',         # Invalid format
            '(011) 123-4567',  # International prefix
            '(960) 123-4567'   # Unassigned area code
        ]
        
        for invalid_phone in invalid_phones:
            patient_data = base_data.copy()
            patient_data['phone'] = invalid_phone
            
            with self.assertRaises(ValidationError, msg=f"Should have failed for: {invalid_phone}"):
                PatientModel(**patient_data)
    
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
    
    def test_email_validation(self):
        """Test email validation - valid emails should pass, invalid ones should raise ValidationError"""
        base_data = {
            'legacy_id': 1,
            'first_name': 'John',
            'last_name': 'Doe',
            'dob': '1990-01-15',
            'phone': '(818) 555-1234',
            'created_at': '2022-01-01 10:30'
        }
        
        # Test valid emails
        valid_emails = [
            'john@example.com',
            'JOHN.DOE@EXAMPLE.COM',  # should be converted to lowercase
            'user.name+tag@domain.co.uk',
            'test123@test-domain.org'
        ]
        
        for email in valid_emails:
            patient_data = base_data.copy()
            patient_data['email'] = email
            
            patient = PatientModel(**patient_data)
            self.assertEqual(patient.email, email.lower(), f"Failed for email: {email}")
        
        # Test invalid emails that should raise ValidationError
        invalid_emails = [
            'userexample.com',      # missing @
            'user@@example.com',    # double @
            'user@',                # missing domain
            '@example.com',         # missing username
            '',                     # empty string
            'user@domain',          # missing TLD
            'user name@example.com' # space in username
        ]
        
        for invalid_email in invalid_emails:
            patient_data = base_data.copy()
            patient_data['email'] = invalid_email
            
            with self.assertRaises(ValidationError, msg=f"Should have failed for: {invalid_email}"):
                PatientModel(**patient_data)


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
        """Test status value mapping per requirements - only valid statuses accepted"""
        # Test valid statuses that should be accepted and converted to lowercase
        valid_test_cases = [
            ('SCHEDULED', 'scheduled'),
            ('CANCELLED', 'cancelled'),
            ('COMPLETED', 'completed'),
            ('scheduled', 'scheduled'),  # already lowercase
            ('cancelled', 'cancelled'),  # already lowercase
            ('completed', 'completed')   # already lowercase
        ]
        
        base_data = {
            'legacy_id': 1,
            'patient_id': 123,
            'patient_uuid': b'1234567890123456',  # Added required patient_uuid as bytes
            'appointment_date': '2023-01-15 14:30',
            'provider_name': 'Dr. Smith',
            'location': 'Main Clinic'
        }
        
        # Test valid statuses
        for input_status, expected in valid_test_cases:
            appointment_data = base_data.copy()
            appointment_data['status'] = input_status
            
            appointment = EncounterModel(**appointment_data)
            self.assertEqual(appointment.status, expected, f"Failed for status: {input_status}")
        
        # Test invalid statuses that should raise ValidationError
        invalid_statuses = ['unknown_status', 'INVALID', 'pending', 'COMPLE']
        
        for invalid_status in invalid_statuses:
            appointment_data = base_data.copy()
            appointment_data['status'] = invalid_status
            
            with self.assertRaises(ValidationError, msg=f"Should have failed for: {invalid_status}"):
                EncounterModel(**appointment_data)


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