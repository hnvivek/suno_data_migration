"""
Pydantic models for data validation and transformation
"""

from pydantic import BaseModel, validator, Field
from datetime import datetime, date
from typing import Optional, Dict, Any
from zoneinfo import ZoneInfo
import phonenumbers
from phonenumbers import NumberParseException
import logging
import pandas as pd
import uuid


logger = logging.getLogger(__name__)

def generate_deterministic_uuid(namespace: str, value: int) -> str:
    """Generate deterministic UUID based on namespace and legacy_id to ensure consistency across runs"""
    namespace_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, f"suno_migration_{namespace}")
    deterministic_uuid = uuid.uuid5(namespace_uuid, str(value))
    return deterministic_uuid.hex

def is_null_or_empty(v):
    """Check if value is None, NaN, or empty string"""
    if v is None:
        return True
    if pd.isna(v):  # This should catch NaN from pandas
        return True
    if isinstance(v, str) and v.strip() == '':
        return True
    # Handle the case where NaN gets converted to string "nan"
    if isinstance(v, str) and v.lower() == 'nan':
        return True
    return False

class PatientModel(BaseModel):
    class Config:
        populate_by_name = True  # Allow both field names and aliases
    
    patient_uuid: str = Field(default="")  # Will be set deterministically based on legacy_id
    first_name: str
    last_name: str
    dob: date  # Required 
    phone_e164: Optional[str] = Field(default=None, alias='phone')  # Map phone to phone_e164
    email: str  # Required  - "Lower-case"
    created_at: datetime  # Required- "Convert to UTC"
    
    # Keep legacy_id for mapping purposes (will be excluded in final output)
    legacy_id: int = Field(alias='legacy_id')
    
    def __init__(self, **data):
        # Generate deterministic UUID based on legacy_id before validation
        if 'legacy_id' in data and not data.get('patient_uuid'):
            data['patient_uuid'] = generate_deterministic_uuid('patient', data['legacy_id'])
        super().__init__(**data)

    @validator('dob', pre=True)
    def parse_dob(cls, v):
        if is_null_or_empty(v):
            raise ValueError("DOB is required")
        try:
            return datetime.strptime(str(v), '%Y-%m-%d').date()
        except Exception:
            raise ValueError(f"Invalid date format: {v}")

    @validator('created_at', pre=True)
    def parse_created(cls, v):
        if is_null_or_empty(v):
            raise ValueError("created_at is required")
        try:
            local = datetime.strptime(str(v), '%Y-%m-%d %H:%M').replace(tzinfo=ZoneInfo('America/New_York'))
            return local.astimezone(ZoneInfo('UTC'))
        except Exception:
            raise ValueError(f"Invalid datetime format: {v}")

    @validator('email', pre=True)
    def email_lower(cls, v):
        if is_null_or_empty(v):
            raise ValueError("Email is required")
        return str(v).strip().lower()

    @validator('phone_e164', pre=True)
    def to_e164(cls, v):
        if is_null_or_empty(v):
            return None  # "Blank ➜ NULL" 
        try:
            parsed = phonenumbers.parse(str(v), "US")
            if phonenumbers.is_possible_number(parsed):
                return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
        except NumberParseException:
            pass
        
        # Invalid phone numbers become NULL (graceful handling)
        logger.warning(f"Invalid phone number format: {v}, setting to NULL")
        return None


class EncounterModel(BaseModel):
    class Config:
        populate_by_name = True  # Allow both field names and aliases
    
    encounter_uuid: str = Field(default="")  # Will be set deterministically based on legacy_id
    patient_uuid: str = Field(default="")  # Will be set deterministically based on patient_legacy_id
    encounter_ts_utc: datetime = Field(alias='appointment_date')  # Map appointment_date to encounter_ts_utc
    provider_name: str
    location: str
    status: str
    
    # Keep legacy IDs for mapping purposes (will be excluded in final output)
    legacy_id: int = Field(alias='legacy_id')
    patient_legacy_id: int = Field(alias='patient_id')
    
    def __init__(self, **data):
        # Generate deterministic UUIDs based on legacy IDs
        if 'legacy_id' in data and not data.get('encounter_uuid'):
            data['encounter_uuid'] = generate_deterministic_uuid('encounter', data['legacy_id'])
        if 'patient_id' in data and not data.get('patient_uuid'):
            data['patient_uuid'] = generate_deterministic_uuid('patient', data['patient_id'])
        super().__init__(**data)

    @validator('encounter_ts_utc', pre=True)
    def parse_apt(cls, v):
        if is_null_or_empty(v):
            raise ValueError("encounter_ts_utc is required")
        try:
            local = datetime.strptime(str(v), '%Y-%m-%d %H:%M').replace(tzinfo=ZoneInfo('America/New_York'))
            return local.astimezone(ZoneInfo('UTC'))
        except Exception:
            raise ValueError(f"Invalid datetime format: {v}")

    @validator('status', pre=True)
    def status_map(cls, v):
        if is_null_or_empty(v):
            raise ValueError("Status is required")
        # Map: SCHEDULED→scheduled, CANCELLED→cancelled, COMPLETED→completed
        mapping = {'SCHEDULED': 'scheduled', 'CANCELLED': 'cancelled', 'COMPLETED': 'completed'}
        return mapping.get(str(v).strip().upper(), str(v).strip().lower())


class InvoiceModel(BaseModel):
    class Config:
        populate_by_name = True  # Allow both field names and aliases
    
    invoice_uuid: str = Field(default="")  # Will be set deterministically based on legacy_id
    patient_uuid: str = Field(default="")  # Will be set deterministically based on patient_legacy_id
    invoice_total_cents: int = Field(alias='amount_usd')  # Map amount_usd to invoice_total_cents
    status: str
    issued_date_utc: datetime = Field(alias='issued_date')  # Map issued_date to issued_date_utc
    paid_date_utc: Optional[datetime] = Field(default=None, alias='paid_date')  # Map paid_date to paid_date_utc
    
    # Keep legacy IDs for mapping purposes (will be excluded in final output)
    legacy_id: int = Field(alias='legacy_id')
    patient_legacy_id: int = Field(alias='patient_id')
    
    def __init__(self, **data):
        # Generate deterministic UUIDs based on legacy IDs
        if 'legacy_id' in data and not data.get('invoice_uuid'):
            data['invoice_uuid'] = generate_deterministic_uuid('invoice', data['legacy_id'])
        if 'patient_id' in data and not data.get('patient_uuid'):
            data['patient_uuid'] = generate_deterministic_uuid('patient', data['patient_id'])
        super().__init__(**data) 

    @validator('invoice_total_cents', pre=True)
    def convert_dollars_to_cents(cls, v):
        """Convert dollars (float) to cents (int)"""
        if is_null_or_empty(v):
            raise ValueError("invoice_total_cents is required")
        try:
            # Convert dollars to cents (multiply by 100 and round)
            dollars = float(v)
            cents = round(dollars * 100)
            return cents
        except (ValueError, TypeError):
            raise ValueError(f"Invalid amount format: {v}")

    @validator('issued_date_utc', pre=True)
    def parse_issued(cls, v):
        if is_null_or_empty(v):
            return None  # "blank paid date ➜ NULL"
        try:
            local = datetime.strptime(str(v), '%Y-%m-%d %H:%M').replace(tzinfo=ZoneInfo('America/New_York'))
            return local.astimezone(ZoneInfo('UTC'))
        except Exception:
            raise ValueError(f"Invalid datetime format: {v}")

    @validator('paid_date_utc', pre=True)
    def parse_paid(cls, v):
        if is_null_or_empty(v): 
            return None  # "blank paid date ➜ NULL"
        try:
            local = datetime.strptime(str(v), '%Y-%m-%d %H:%M').replace(tzinfo=ZoneInfo('America/New_York'))
            return local.astimezone(ZoneInfo('UTC'))
        except Exception:
            raise ValueError(f"Invalid datetime format: {v}")

    @validator('status', pre=True)
    def status_lower(cls, v):
        if is_null_or_empty(v):
            raise ValueError("Status is required")
        # Map: OPEN→open, PAID→paid
        mapping = {'OPEN': 'open', 'PAID': 'paid'}
        return mapping.get(str(v).strip().upper(), str(v).strip().lower()) 

 