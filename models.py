"""
Pydantic models for data validation and transformation
"""

from pydantic import BaseModel, validator
from datetime import datetime, date
from typing import Optional
from zoneinfo import ZoneInfo
import phonenumbers
from phonenumbers import NumberParseException
import logging

logger = logging.getLogger(__name__)

class PatientModel(BaseModel):
    legacy_id: int
    first_name: str
    last_name: str
    dob: date  # Required 
    phone: Optional[str] = None  # "Blank ➜ NULL" 
    email: str  # Required  - "Lower-case"
    created_at: datetime  # Required- "Convert to UTC"

    @validator('dob', pre=True)
    def parse_dob(cls, v):
        if not v:
            raise ValueError("DOB is required")
        try:
            return datetime.strptime(str(v), '%Y-%m-%d').date()
        except Exception:
            raise ValueError(f"Invalid date format: {v}")

    @validator('created_at', pre=True)
    def parse_created(cls, v):
        if not v:
            raise ValueError("created_at is required")
        try:
            local = datetime.strptime(str(v), '%Y-%m-%d %H:%M').replace(tzinfo=ZoneInfo('America/New_York'))
            return local.astimezone(ZoneInfo('UTC'))
        except Exception:
            raise ValueError(f"Invalid datetime format: {v}")

    @validator('email', pre=True)
    def email_lower(cls, v):
        if not v:
            raise ValueError("Email is required")
        return str(v).strip().lower()

    @validator('phone', pre=True)
    def to_e164(cls, v):
        if not v or str(v).strip() == '':
            return None  # "Blank ➜ NULL" 
        try:
            parsed = phonenumbers.parse(str(v), "US")
            if phonenumbers.is_possible_number(parsed):  # Use is_possible_number for test data
                return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
        except NumberParseException:
            pass
        
        # Invalid phone numbers become NULL (graceful handling)
        logger.warning(f"Invalid phone number format: {v}, setting to NULL")
        return None


class AppointmentModel(BaseModel):
    legacy_id: int
    patient_id: int
    appointment_date: datetime  
    provider_name: str
    location: str
    status: str

    @validator('appointment_date', pre=True)
    def parse_apt(cls, v):
        if not v:
            raise ValueError("appointment_date is required")
        try:
            local = datetime.strptime(str(v), '%Y-%m-%d %H:%M').replace(tzinfo=ZoneInfo('America/New_York'))
            return local.astimezone(ZoneInfo('UTC'))
        except Exception:
            raise ValueError(f"Invalid datetime format: {v}")

    @validator('status', pre=True)
    def status_map(cls, v):
        if not v:
            raise ValueError("Status is required")
        # Map: SCHEDULED→scheduled, CANCELLED→cancelled, COMPLETED→completed
        mapping = {'SCHEDULED': 'scheduled', 'CANCELLED': 'cancelled', 'COMPLETED': 'completed'}
        return mapping.get(str(v).strip().upper(), str(v).strip().lower())


class InvoiceModel(BaseModel):
    legacy_id: int
    patient_id: int
    amount_usd: float
    status: str
    issued_date: datetime  # Required
    paid_date: Optional[datetime] = None  # Blank paid date → NULL 

    @validator('issued_date', pre=True)
    def parse_issued(cls, v):
        if not v:
            raise ValueError("issued_date is required")
        try:
            local = datetime.strptime(str(v), '%Y-%m-%d %H:%M').replace(tzinfo=ZoneInfo('America/New_York'))
            return local.astimezone(ZoneInfo('UTC'))
        except Exception:
            raise ValueError(f"Invalid datetime format: {v}")

    @validator('paid_date', pre=True)
    def parse_paid(cls, v):
        if not v or str(v).strip() == '':
            return None  # Blank paid date → NULL 
        try:
            local = datetime.strptime(str(v), '%Y-%m-%d %H:%M').replace(tzinfo=ZoneInfo('America/New_York'))
            return local.astimezone(ZoneInfo('UTC'))
        except Exception:
            raise ValueError(f"Invalid datetime format: {v}")

    @validator('status', pre=True)
    def status_lower(cls, v):
        if not v:
            raise ValueError("Status is required")
        # Map: OPEN→open, PAID→paid
        mapping = {'OPEN': 'open', 'PAID': 'paid'}
        return mapping.get(str(v).strip().upper(), str(v).strip().lower()) 