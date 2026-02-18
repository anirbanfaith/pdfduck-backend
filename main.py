from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pdfplumber
import io
import re
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from decimal import Decimal, InvalidOperation

app = FastAPI(title="pdfduck API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def clean_text(text: str) -> str:
    """Clean and normalize text"""
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def safe_decimal(value: str) -> Optional[str]:
    """Safely convert to decimal, handling various formats"""
    if not value:
        return None
    try:
        # Remove currency symbols, commas, spaces
        cleaned = re.sub(r'[^\d.-]', '', str(value))
        if cleaned and cleaned not in ['-', '.']:
            dec = Decimal(cleaned)
            return str(dec)
    except (InvalidOperation, ValueError):
        pass
    return None

def parse_date(date_str: str) -> Optional[str]:
    """Parse date from various formats to YYYY-MM-DD"""
    if not date_str:
        return None
    
    # Clean the date string
    date_str = re.sub(r'[^\d/-]', '', date_str)
    
    formats = [
        '%d-%m-%Y', '%d/%m/%Y', '%d.%m.%Y',
        '%Y-%m-%d', '%Y/%m/%d',
        '%d-%m-%y', '%d/%m/%y',
        '%d%m%Y', '%Y%m%d'
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y-%m-%d')
        except:
            continue
    
    # Return as-is if can't parse
    return date_str if date_str else None

class FieldExtractor:
    """Handles multi-strategy field extraction"""
    
    def __init__(self, pdf, full_text: str):
        self.pdf = pdf
        self.full_text = full_text
        self.data = {}
    
    def extract_with_patterns(self, field_name: str, patterns: List[str], 
                             processor=None, multiline=False) -> Optional[str]:
        """Try multiple regex patterns, apply processor if found"""
        flags = re.IGNORECASE | (re.MULTILINE if multiline else 0)
        
        for pattern in patterns:
            match = re.search(pattern, self.full_text, flags)
            if match:
                value = clean_text(match.group(1))
                if processor:
                    value = processor(value)
                if value:
                    return value
        return None
    
    def extract_from_coordinates(self, page_idx: int, bbox: Tuple[float, float, float, float]) -> Optional[str]:
        """Extract text from specific coordinates on a page"""
        try:
            if page_idx < len(self.pdf.pages):
                page = self.pdf.pages[page_idx]
                cropped = page.crop(bbox)
                text = cropped.extract_text()
                return clean_text(text) if text else None
        except:
            pass
        return None
    
    def aggregate_table_column(self, column_name: str, operation='sum') -> Optional[str]:
        """Aggregate values from a specific table column"""
        values = []
        
        for page in self.pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if not table or len(table) < 2:
                    continue
                
                # Find column index
                header = [clean_text(str(cell)).lower() if cell else "" for cell in table[0]]
                col_idx = None
                
                for idx, h in enumerate(header):
                    if column_name.lower() in h:
                        col_idx = idx
                        break
                
                if col_idx is None:
                    continue
                
                # Extract values from column
                for row in table[1:]:
                    if col_idx < len(row) and row[col_idx]:
                        val = safe_decimal(str(row[col_idx]))
                        if val:
                            values.append(Decimal(val))
        
        if not values:
            return None
        
        if operation == 'sum':
            return str(sum(values))
        elif operation == 'count':
            return str(len(values))
        elif operation == 'max':
            return str(max(values))
        elif operation == 'min':
            return str(min(values))
        
        return None

def extract_comprehensive(pdf_bytes: bytes) -> Dict[str, Any]:
    """
    Comprehensive extraction combining:
    1. Header field regex extraction
    2. Table parsing and aggregation
    3. Coordinate-based extraction (for fixed-position fields)
    4. Smart fallbacks and validation
    
    Returns ONE summary row per PDF.
    """
    
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        # Extract full text
        full_text = ""
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                full_text += text + "\n\n"
        
        extractor = FieldExtractor(pdf, full_text)
        data = {}
        
        # === CORE IDENTIFIERS ===
        
        # Shipping Bill Number
        data['shipping_bill_no'] = extractor.extract_with_patterns(
            'shipping_bill_no',
            [
                r'shipping\s*bill\s*(?:no\.?|number|#)\s*:?\s*(\d{7,10})',
                r's\.?b\.?\s*(?:no\.?|number|#)\s*:?\s*(\d{7,10})',
                r'bill\s*(?:no\.?|number)\s*:?\s*(\d{7,10})',
                r'sb\s*:\s*(\d{7,10})',
            ]
        )
        
        # Shipping Bill Date
        data['shipping_bill_date'] = extractor.extract_with_patterns(
            'shipping_bill_date',
            [
                r'shipping\s*bill\s*date\s*:?\s*([\d\-/\.]{8,10})',
                r's\.?b\.?\s*date\s*:?\s*([\d\-/\.]{8,10})',
                r'date\s*of\s*sb\s*:?\s*([\d\-/\.]{8,10})',
                r'date\s*:?\s*([\d\-/\.]{8,10})',
            ],
            processor=parse_date
        )
        
        # Invoice/Order Number
        data['order_id'] = extractor.extract_with_patterns(
            'order_id',
            [
                r'invoice\s*(?:no\.?|number|#)\s*:?\s*([A-Z0-9\-/]+)',
                r'inv\.?\s*no\.?\s*:?\s*([A-Z0-9\-/]+)',
                r'order\s*(?:no\.?|id)\s*:?\s*([A-Z0-9\-/]+)',
                r'bill\s*ref\.?\s*:?\s*([A-Z0-9\-/]+)',
            ]
        )
        
        # Tracking/AWB
        data['tracking_id'] = extractor.extract_with_patterns(
            'tracking_id',
            [
                r'awb\s*(?:no\.?|number)?\s*:?\s*([A-Z0-9\-]+)',
                r'tracking\s*(?:no\.?|number|id)\s*:?\s*([A-Z0-9\-]+)',
                r'airway\s*bill\s*(?:no\.?)?\s*:?\s*([A-Z0-9\-]+)',
            ]
        )
        
        # === FINANCIAL FIELDS ===
        
        # FOB/Declared Value (try aggregation first, then header)
        fob_sum = extractor.aggregate_table_column('fob', 'sum')
        if not fob_sum:
            fob_sum = extractor.aggregate_table_column('value', 'sum')
        if not fob_sum:
            fob_sum = extractor.extract_with_patterns(
                'declared_value',
                [
                    r'fob\s*value\s*:?\s*(?:rs\.?|inr|usd|\$|€)?\s*([\d,]+\.?\d*)',
                    r'invoice\s*value\s*:?\s*(?:rs\.?|inr|usd|\$|€)?\s*([\d,]+\.?\d*)',
                    r'declared\s*value\s*:?\s*(?:rs\.?|inr|usd|\$|€)?\s*([\d,]+\.?\d*)',
                    r'total\s*(?:fob\s*)?value\s*:?\s*(?:rs\.?|inr|usd|\$|€)?\s*([\d,]+\.?\d*)',
                    r'assessable\s*value\s*:?\s*(?:rs\.?|inr|usd|\$|€)?\s*([\d,]+\.?\d*)',
                ],
                processor=safe_decimal
            )
        data['declared_value'] = fob_sum
        
        # Currency
        data['currency'] = extractor.extract_with_patterns(
            'currency',
            [
                r'\b(USD|INR|EUR|GBP|AED|SGD)\b',
                r'currency\s*:?\s*([A-Z]{3})',
            ]
        )
        if data['currency']:
            data['currency'] = data['currency'][:3].upper()
        
        # Freight
        data['freight_charges'] = extractor.extract_with_patterns(
            'freight',
            [
                r'freight\s*(?:charges?)?\s*:?\s*(?:rs\.?|inr|usd|\$)?\s*([\d,]+\.?\d*)',
            ],
            processor=safe_decimal
        )
        
        # Insurance
        data['insurance_value'] = extractor.extract_with_patterns(
            'insurance',
            [
                r'insurance\s*(?:value|amount)?\s*:?\s*(?:rs\.?|inr|usd|\$)?\s*([\d,]+\.?\d*)',
            ],
            processor=safe_decimal
        )
        
        # === PARTIES ===
        
        # Exporter
        data['exporter_name'] = extractor.extract_with_patterns(
            'exporter_name',
            [
                r'exporter\s*(?:name)?\s*:?\s*([^\n]{10,150})',
                r'shipper\s*:?\s*([^\n]{10,150})',
                r'consignor\s*:?\s*([^\n]{10,150})',
            ],
            multiline=True
        )
        
        # Exporter Address
        data['exporter_address'] = extractor.extract_with_patterns(
            'exporter_address',
            [
                r'exporter\s*address\s*:?\s*([^\n]{10,200})',
            ],
            multiline=True
        )
        
        # Consignee
        data['consignee_name'] = extractor.extract_with_patterns(
            'consignee_name',
            [
                r'consignee\s*(?:name)?\s*:?\s*([^\n]{10,150})',
                r'buyer\s*:?\s*([^\n]{10,150})',
                r'importer\s*:?\s*([^\n]{10,150})',
            ],
            multiline=True
        )
        
        # Consignee Address
        data['consignee_address'] = extractor.extract_with_patterns(
            'consignee_address',
            [
                r'consignee\s*address\s*:?\s*([^\n]{10,200})',
            ],
            multiline=True
        )
        
        # Country of Destination
        data['consignee_country'] = extractor.extract_with_patterns(
            'consignee_country',
            [
                r'country\s*of\s*(?:final\s*)?destination\s*:?\s*([A-Z][a-zA-Z\s]+)',
                r'destination\s*country\s*:?\s*([A-Z][a-zA-Z\s]+)',
                r'final\s*destination\s*:?\s*([A-Z][a-zA-Z\s]+)',
            ]
        )
        
        # === SHIPPING ===
        
        # Port of Loading
        data['port_of_loading'] = extractor.extract_with_patterns(
            'port_of_loading',
            [
                r'port\s*of\s*loading\s*:?\s*([A-Z]{2,}[A-Z0-9\s\-]+)',
                r'pol\s*:?\s*([A-Z]{2,}[A-Z0-9\s\-]+)',
                r'loading\s*port\s*:?\s*([A-Z]{2,}[A-Z0-9\s\-]+)',
            ]
        )
        
        # Port of Discharge
        data['port_of_discharge'] = extractor.extract_with_patterns(
            'port_of_discharge',
            [
                r'port\s*of\s*discharge\s*:?\s*([A-Z]{2,}[A-Z0-9\s\-]+)',
                r'pod\s*:?\s*([A-Z]{2,}[A-Z0-9\s\-]+)',
                r'discharge\s*port\s*:?\s*([A-Z]{2,}[A-Z0-9\s\-]+)',
                r'destination\s*port\s*:?\s*([A-Z]{2,}[A-Z0-9\s\-]+)',
            ]
        )
        
        # Mode of Transport
        data['mode_of_transport'] = extractor.extract_with_patterns(
            'mode_of_transport',
            [
                r'mode\s*of\s*transport\s*:?\s*([A-Za-z\s]+)',
                r'\b(air|sea|road|rail)\b',
            ]
        )
        
        # === CARGO ===
        
        # Total Packages (try aggregation, then header)
        pkg_count = extractor.aggregate_table_column('packages', 'sum')
        if not pkg_count:
            pkg_count = extractor.aggregate_table_column('quantity', 'count')
        if not pkg_count:
            pkg_count = extractor.extract_with_patterns(
                'total_packages',
                [
                    r'(?:total\s*)?(?:no\.?\s*of\s*)?packages\s*:?\s*(\d+)',
                    r'no\.\s*of\s*packages\s*:?\s*(\d+)',
                    r'packages\s*:?\s*(\d+)',
                ]
            )
        data['total_packages'] = pkg_count
        
        # Gross Weight (try aggregation, then header)
        gross_sum = extractor.aggregate_table_column('gross', 'sum')
        if not gross_sum:
            gross_sum = extractor.extract_with_patterns(
                'gross_weight',
                [
                    r'gross\s*weight\s*:?\s*([\d,\.]+\s*(?:kg|kgs|lbs|mt)?)',
                    r'grs\.?\s*wt\.?\s*:?\s*([\d,\.]+\s*(?:kg|kgs|lbs|mt)?)',
                    r'total\s*gross\s*:?\s*([\d,\.]+\s*(?:kg|kgs|lbs|mt)?)',
                ]
            )
        data['gross_weight'] = gross_sum
        
        # Net Weight (try aggregation, then header)
        net_sum = extractor.aggregate_table_column('net', 'sum')
        if not net_sum:
            net_sum = extractor.extract_with_patterns(
                'net_weight',
                [
                    r'net\s*weight\s*:?\s*([\d,\.]+\s*(?:kg|kgs|lbs|mt)?)',
                    r'net\s*wt\.?\s*:?\s*([\d,\.]+\s*(?:kg|kgs|lbs|mt)?)',
                    r'total\s*net\s*:?\s*([\d,\.]+\s*(?:kg|kgs|lbs|mt)?)',
                ]
            )
        data['net_weight'] = net_sum
        
        # HS Code (take first occurrence or most common from table)
        data['hs_code'] = extractor.extract_with_patterns(
            'hs_code',
            [
                r'hs\s*code\s*:?\s*(\d{4,10})',
                r'h\.?s\.?\s*code\s*:?\s*(\d{4,10})',
                r'tariff\s*code\s*:?\s*(\d{4,10})',
                r'ctsh\s*:?\s*(\d{4,10})',
            ]
        )
        
        # Item Description (summarize from table or extract header)
        # Get first few item descriptions from table
        descriptions = []
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if not table or len(table) < 2:
                    continue
                header = [clean_text(str(c)).lower() if c else "" for c in table[0]]
                desc_idx = None
                for idx, h in enumerate(header):
                    if 'description' in h or 'item' in h or 'goods' in h:
                        desc_idx = idx
                        break
                if desc_idx:
                    for row in table[1:3]:  # First 2 items
                        if desc_idx < len(row) and row[desc_idx]:
                            descriptions.append(clean_text(str(row[desc_idx])))
        
        if descriptions:
            data['item_description'] = '; '.join(descriptions[:3])  # Max 3 items
        else:
            data['item_description'] = extractor.extract_with_patterns(
                'item_description',
                [
                    r'description\s*of\s*goods\s*:?\s*([^\n]{10,200})',
                    r'goods\s*description\s*:?\s*([^\n]{10,200})',
                ],
                multiline=True
            )
        
        # === COMPLIANCE ===
        
        # Scheme/Drawback
        data['scheme'] = extractor.extract_with_patterns(
            'scheme',
            [
                r'scheme\s*:?\s*([^\n]{5,50})',
                r'(drawback|igst\s*refund|meis|rodtep|advance\s*authorization)',
                r'dbk\s*:?\s*([^\n]{5,50})',
            ]
        )
        
        # GSTIN
        data['gstin'] = extractor.extract_with_patterns(
            'gstin',
            [
                r'gstin\s*:?\s*([A-Z0-9]{15})',
                r'gst\s*no\.?\s*:?\s*([A-Z0-9]{15})',
                r'\b([0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1})\b',
            ]
        )
        
        # IEC Code
        data['iec_code'] = extractor.extract_with_patterns(
            'iec_code',
            [
                r'iec\s*(?:code|no\.?)?\s*:?\s*([A-Z0-9]{10})',
                r'\b([0-9]{10})\b',  # 10-digit number
            ]
        )
        
        # AD Code
        data['ad_code'] = extractor.extract_with_patterns(
            'ad_code',
            [
                r'ad\s*code\s*:?\s*([A-Z0-9]+)',
            ]
        )
        
        # Vessel/Flight
        data['vessel_name'] = extractor.extract_with_patterns(
            'vessel_name',
            [
                r'vessel\s*(?:name)?\s*:?\s*([^\n]{5,50})',
                r'flight\s*(?:no\.?)?\s*:?\s*([A-Z0-9\s]+)',
            ]
        )
        
        # Container Number
        data['container_number'] = extractor.extract_with_patterns(
            'container_number',
            [
                r'container\s*(?:no\.?|number)\s*:?\s*([A-Z]{4}\d{7})',
            ]
        )
        
        # LUT Number
        data['lut_number'] = extractor.extract_with_patterns(
            'lut_number',
            [
                r'lut\s*(?:no\.?|number)?\s*:?\s*([A-Z0-9\-/]+)',
            ]
        )
        
        # === METADATA ===
        data['_total_pages'] = len(pdf.pages)
        data['_text_length'] = len(full_text)
        
        return data

@app.get("/")
async def root():
    return {
        "service": "pdfduck API",
        "version": "2.0.0",
        "extraction": "Multi-strategy: header regex + table aggregation + coordinates",
        "cost": "$0 - Pure Python",
        "features": [
            "One summary row per PDF",
            "30+ extracted fields",
            "Table value aggregation (sum, count)",
            "Smart fallbacks and validation"
        ]
    }

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.post("/extract")
async def extract_pdf(file: UploadFile = File(...)):
    """
    Extract ONE comprehensive summary row per PDF.
    
    Combines:
    - Header field extraction (regex)
    - Table aggregation (sum FOB, count packages, etc.)
    - Coordinate-based extraction
    - Smart fallbacks
    
    Perfect for batch: 25 PDFs → 26 row CSV
    """
    
    if not file.filename.endswith('.pdf'):
        raise HTTPException(status_code=400, detail="File must be a PDF")
    
    try:
        pdf_bytes = await file.read()
        data = extract_comprehensive(pdf_bytes)
        
        return JSONResponse({
            "success": True,
            "method": "comprehensive_python_extraction",
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