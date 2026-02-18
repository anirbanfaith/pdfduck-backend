from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pdfplumber
import io
import re
from typing import Dict, Any, List, Optional
from datetime import datetime
from decimal import Decimal

app = FastAPI(title="pdfduck API", version="2.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def clean_text(text: str) -> Optional[str]:
    """Clean text, return None if empty"""
    if not text:
        return None
    text = re.sub(r'\s+', ' ', str(text)).strip()
    return text if text and text not in ['N/A', 'NA', '-', ''] else None

def parse_date(date_str: str) -> Optional[str]:
    """Parse date to YYYY-MM-DD"""
    if not date_str:
        return None
    
    date_str = clean_text(date_str)
    if not date_str:
        return None
    
    # Try various formats
    formats = ['%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y', '%Y-%m-%d', '%d/%m/%y', '%d-%m-%y']
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y-%m-%d')
        except:
            continue
    return date_str

def safe_decimal(value: str) -> Optional[str]:
    """Extract clean decimal value"""
    if not value:
        return None
    try:
        cleaned = re.sub(r'[^\d.]', '', str(value))
        if cleaned and cleaned not in ['.', '']:
            return str(Decimal(cleaned))
    except:
        pass
    return None

def find_value_in_table_row(row: List[Any], key_pattern: str) -> Optional[str]:
    """Find value in a table row where first cell matches key pattern"""
    if not row or len(row) < 2:
        return None
    
    key = clean_text(str(row[0])) if row[0] else ""
    if key and re.search(key_pattern, key, re.IGNORECASE):
        # Value is in next cell
        val = clean_text(str(row[1])) if len(row) > 1 and row[1] else None
        return val
    return None

def extract_from_structured_pdf(pdf_bytes: bytes) -> Dict[str, Any]:
    """
    Extract from structured Indian shipping bills (CSB-V format).
    Uses table cell parsing instead of regex on full text.
    """
    
    data = {}
    
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        # Extract all tables from all pages
        all_tables = []
        full_text = ""
        
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                full_text += text + "\n"
            
            tables = page.extract_tables()
            if tables:
                all_tables.extend(tables)
        
        # Parse tables for key-value pairs
        for table in all_tables:
            if not table:
                continue
            
            for row in table:
                if not row or len(row) < 2:
                    continue
                
                # CSB Number
                val = find_value_in_table_row(row, r'CSB\s*Number')
                if val and not data.get('shipping_bill_no'):
                    # Extract just the number part
                    match = re.search(r'(\d{7,})', val)
                    if match:
                        data['shipping_bill_no'] = match.group(1)
                    else:
                        data['shipping_bill_no'] = val
                
                # Filling/Filing Date
                val = find_value_in_table_row(row, r'Fill(?:ing|ing)\s*Date')
                if val and not data.get('shipping_bill_date'):
                    data['shipping_bill_date'] = parse_date(val)
                
                # HAWB Number (Tracking ID)
                val = find_value_in_table_row(row, r'HAWB\s*Number')
                if val and not data.get('tracking_id'):
                    data['tracking_id'] = val
                
                # Invoice Number
                val = find_value_in_table_row(row, r'Invoice\s*Number')
                if val and not data.get('order_id'):
                    data['order_id'] = val
                
                # Invoice Date
                val = find_value_in_table_row(row, r'Invoice\s*Date')
                if val and not data.get('invoice_date'):
                    data['invoice_date'] = parse_date(val)
                
                # FOB Value (INR)
                val = find_value_in_table_row(row, r'FOB\s*Value.*INR')
                if val and not data.get('declared_value'):
                    data['declared_value'] = safe_decimal(val)
                
                # Invoice Value
                val = find_value_in_table_row(row, r'Invoice\s*Value.*INR')
                if val and not data.get('declared_value'):
                    data['declared_value'] = safe_decimal(val)
                
                # Total Item Value
                val = find_value_in_table_row(row, r'Total\s*Item\s*Value.*INR')
                if val and not data.get('total_item_value'):
                    data['total_item_value'] = safe_decimal(val)
                
                # Currency
                val = find_value_in_table_row(row, r'FOB\s*Currency')
                if val and not data.get('currency'):
                    match = re.search(r'\b([A-Z]{3})\b', val)
                    if match:
                        data['currency'] = match.group(1)
                
                # IEC Code
                val = find_value_in_table_row(row, r'Import\s*Export\s*Code')
                if val and not data.get('iec_code'):
                    data['iec_code'] = val
                
                # AD Code
                val = find_value_in_table_row(row, r'AD\s*Code')
                if val and not data.get('ad_code'):
                    data['ad_code'] = val
                
                # GSTIN / KYC ID
                val = find_value_in_table_row(row, r'KYC\s*ID')
                if val and not data.get('gstin'):
                    data['gstin'] = val
                
                # Port of Loading
                val = find_value_in_table_row(row, r'Port\s*of\s*Loading')
                if val and not data.get('port_of_loading'):
                    data['port_of_loading'] = val
                
                # Airport of Destination / Port of Discharge
                val = find_value_in_table_row(row, r'Airport\s*of\s*Destination')
                if val and not data.get('port_of_discharge'):
                    data['port_of_discharge'] = val
                
                # Number of Packages
                val = find_value_in_table_row(row, r'Number\s*of\s*Packages')
                if val and not data.get('total_packages'):
                    match = re.search(r'(\d+)', val)
                    if match:
                        data['total_packages'] = match.group(1)
                
                # Quantity
                val = find_value_in_table_row(row, r'Quantity')
                if val and not data.get('quantity'):
                    match = re.search(r'(\d+)', val)
                    if match:
                        data['quantity'] = match.group(1)
                
                # Declared Weight / Gross Weight
                val = find_value_in_table_row(row, r'Declared\s*Weight')
                if val and not data.get('gross_weight'):
                    data['gross_weight'] = val
                
                # HS Code / CTSH
                val = find_value_in_table_row(row, r'CTSH')
                if val and not data.get('hs_code'):
                    match = re.search(r'(\d{4,10})', val)
                    if match:
                        data['hs_code'] = match.group(1)
                
                # Goods Description
                val = find_value_in_table_row(row, r'Goods\s*Description')
                if val and not data.get('item_description'):
                    data['item_description'] = val
                
                # Consignor Name
                val = find_value_in_table_row(row, r'Name\s*of\s*the\s*Consignor')
                if val and not data.get('exporter_name'):
                    data['exporter_name'] = val
                
                # Consignor Address
                val = find_value_in_table_row(row, r'Address\s*of\s*the\s*Consignor')
                if val and not data.get('exporter_address'):
                    data['exporter_address'] = val
                
                # Consignee Name
                val = find_value_in_table_row(row, r'Name\s*of\s*the\s*Consignee')
                if val and not data.get('consignee_name'):
                    data['consignee_name'] = val
                
                # Consignee Address
                val = find_value_in_table_row(row, r'Address\s*of\s*the\s*Consignee')
                if val and not data.get('consignee_address'):
                    data['consignee_address'] = val
                
                # Flight Number
                val = find_value_in_table_row(row, r'Flight\s*Number')
                if val and not data.get('flight_number'):
                    data['flight_number'] = val
                
                # Airlines
                val = find_value_in_table_row(row, r'Airlines')
                if val and not data.get('airline'):
                    data['airline'] = val
                
                # MHBS Number
                val = find_value_in_table_row(row, r'MHBS\s*No')
                if val and not data.get('mhbs_number'):
                    data['mhbs_number'] = val
                
                # Under MEIS Scheme
                val = find_value_in_table_row(row, r'Under\s*MEIS\s*Scheme')
                if val and not data.get('scheme'):
                    if val.upper() in ['YES', 'Y']:
                        data['scheme'] = 'MEIS'
                    elif val.upper() in ['NO', 'N']:
                        data['scheme'] = None
        
        # Fallback: Extract country from consignee address
        if not data.get('consignee_country') and data.get('consignee_address'):
            addr = data['consignee_address']
            # Look for country names at end of address
            countries = ['AUSTRALIA', 'UNITED STATES', 'USA', 'CANADA', 'UK', 'SINGAPORE', 'UAE', 'GERMANY', 'FRANCE']
            for country in countries:
                if country in addr.upper():
                    data['consignee_country'] = country
                    break
        
        # Extract mode of transport from context
        if 'flight' in full_text.lower() or 'airline' in full_text.lower() or 'airport' in full_text.lower():
            data['mode_of_transport'] = 'AIR'
        elif 'vessel' in full_text.lower() or 'ship' in full_text.lower():
            data['mode_of_transport'] = 'SEA'
        
        # Metadata
        data['_total_pages'] = len(pdf.pages)
        data['_text_length'] = len(full_text)
        
        return data

@app.get("/")
async def root():
    return {
        "service": "pdfduck API",
        "version": "2.1.0",
        "extraction": "Table cell parsing for Indian CSB-V shipping bills",
        "optimized_for": "Structured forms with key-value table layouts"
    }

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.post("/extract")
async def extract_pdf(file: UploadFile = File(...)):
    """
    Extract ONE summary row per PDF.
    Optimized for Indian Courier Shipping Bill (CSB-V) format.
    """
    
    if not file.filename.endswith('.pdf'):
        raise HTTPException(status_code=400, detail="File must be a PDF")
    
    try:
        pdf_bytes = await file.read()
        data = extract_from_structured_pdf(pdf_bytes)
        
        return JSONResponse({
            "success": True,
            "method": "table_cell_parsing",
            "rows": 1,
            "data": [data]
        })
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Extraction failed: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)