from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pdfplumber
import io
import re
from typing import Dict, Any, List, Optional
from datetime import datetime
from decimal import Decimal

app = FastAPI(title="pdfduck API", version="3.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def clean(text: str) -> Optional[str]:
    """Clean text, return None if empty/useless"""
    if not text:
        return None
    text = re.sub(r'\s+', ' ', str(text)).strip()
    return text if text and text not in ['N/A', 'NA', '-', '', 'None', 'null'] else None

def parse_date(date_str: str) -> Optional[str]:
    """Parse any date format to YYYY-MM-DD"""
    if not date_str:
        return None
    
    date_str = clean(date_str)
    if not date_str:
        return None
    
    formats = ['%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y', '%Y-%m-%d', '%d/%m/%y', '%d-%m-%y', '%d%m%Y']
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except:
            continue
    return date_str

def to_decimal(val: str) -> Optional[str]:
    """Extract clean decimal"""
    if not val:
        return None
    try:
        cleaned = re.sub(r'[^\d.]', '', str(val))
        if cleaned and cleaned not in ['.', '']:
            return str(Decimal(cleaned))
    except:
        pass
    return None

class ExtractionEngine:
    """Exhaustive multi-method field extraction"""
    
    def __init__(self, pdf):
        self.pdf = pdf
        self.full_text = ""
        self.tables = []
        
        # Extract everything
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                self.full_text += text + "\n"
            
            page_tables = page.extract_tables()
            if page_tables:
                self.tables.extend(page_tables)
        
        self.data = {}
    
    def find_in_tables(self, key_patterns: List[str], value_column: int = 1) -> Optional[str]:
        """Search tables for key pattern, return value from specified column"""
        for table in self.tables:
            if not table:
                continue
            for row in table:
                if not row or len(row) <= value_column:
                    continue
                
                key = clean(str(row[0])) if row[0] else ""
                if not key:
                    continue
                
                for pattern in key_patterns:
                    if re.search(pattern, key, re.IGNORECASE):
                        val = clean(str(row[value_column])) if row[value_column] else None
                        if val:
                            return val
        return None
    
    def find_in_text(self, patterns: List[str], group: int = 1) -> Optional[str]:
        """Search full text with regex patterns"""
        for pattern in patterns:
            match = re.search(pattern, self.full_text, re.IGNORECASE | re.MULTILINE)
            if match:
                val = clean(match.group(group))
                if val:
                    return val
        return None
    
    def find_all_in_text(self, patterns: List[str]) -> List[str]:
        """Find all matches in text"""
        results = []
        for pattern in patterns:
            matches = re.findall(pattern, self.full_text, re.IGNORECASE)
            for m in matches:
                val = clean(m if isinstance(m, str) else m[0])
                if val and val not in results:
                    results.append(val)
        return results
    
    def extract_field(self, field_name: str, table_keys: List[str], text_patterns: List[str], 
                     processor=None) -> Optional[str]:
        """Try multiple methods to extract a field"""
        # Method 1: Search tables
        val = self.find_in_tables(table_keys)
        
        # Method 2: Search text with regex
        if not val:
            val = self.find_in_text(text_patterns)
        
        # Apply processor
        if val and processor:
            val = processor(val)
        
        return val
    
    def extract(self) -> Dict[str, Any]:
        """Extract all fields with exhaustive search"""
        
        # === SHIPPING BILL NUMBER ===
        sb_no = self.extract_field(
            'shipping_bill_no',
            [r'CSB\s*Number', r'Shipping\s*Bill\s*No'],
            [
                r'CSB\s*Number\s*:?\s*([A-Z0-9_\-\s]+)',
                r'Shipping\s*Bill\s*(?:No|Number)\s*:?\s*(\d{7,})',
                r'S\.?B\.?\s*No\.?\s*:?\s*(\d{7,})',
            ]
        )
        if sb_no:
            # Clean: extract numeric part or keep full
            match = re.search(r'(\d{7,})', sb_no.replace(' ', '').replace('_', ''))
            self.data['shipping_bill_no'] = match.group(1) if match else sb_no
        
        # === DATES ===
        self.data['shipping_bill_date'] = self.extract_field(
            'shipping_bill_date',
            [r'Fill(?:ing|ing)\s*Date', r'SB\s*Date', r'Bill\s*Date'],
            [
                r'Fill(?:ing|ing)\s*Date\s*:?\s*([\d/\-\.]+)',
                r'SB\s*Date\s*:?\s*([\d/\-\.]+)',
                r'Date\s*of\s*(?:SB|Shipping\s*Bill)\s*:?\s*([\d/\-\.]+)',
            ],
            processor=parse_date
        )
        
        self.data['invoice_date'] = self.extract_field(
            'invoice_date',
            [r'Invoice\s*Date'],
            [r'Invoice\s*Date\s*:?\s*([\d/\-\.]+)'],
            processor=parse_date
        )
        
        # === INVOICE NUMBER (CRITICAL - multiple attempts) ===
        invoice = self.extract_field(
            'order_id',
            [r'Invoice\s*Number', r'Invoice\s*No'],
            [
                r'Invoice\s*(?:Number|No\.?)\s*:?\s*([A-Z0-9\-/]+)',
                r'Inv\.?\s*(?:No\.?|#)\s*:?\s*([A-Z0-9\-/]+)',
                r'Bill\s*(?:No\.?|Number)\s*:?\s*([A-Z0-9\-/]+)',
            ]
        )
        if not invoice:
            # Fallback: look for patterns like JOD-12122024, INV-xxx
            all_inv = self.find_all_in_text([r'\b([A-Z]{2,4}[-_]\d{6,10})\b'])
            if all_inv:
                invoice = all_inv[0]
        self.data['order_id'] = invoice
        
        # === TRACKING ===
        self.data['tracking_id'] = self.extract_field(
            'tracking_id',
            [r'HAWB\s*Number', r'AWB\s*Number', r'Tracking'],
            [
                r'HAWB\s*Number\s*:?\s*(\d{10,15})',
                r'AWB\s*(?:No\.?|Number)\s*:?\s*([A-Z0-9]+)',
                r'Tracking\s*(?:ID|No\.?)\s*:?\s*([A-Z0-9]+)',
            ]
        )
        
        # === FINANCIAL ===
        fob = self.extract_field(
            'declared_value',
            [r'FOB\s*Value.*INR', r'Invoice\s*Value.*INR', r'Total\s*Value'],
            [
                r'FOB\s*Value.*INR.*:?\s*([\d,\.]+)',
                r'Invoice\s*Value.*INR.*:?\s*([\d,\.]+)',
                r'Declared\s*Value\s*:?\s*([\d,\.]+)',
            ],
            processor=to_decimal
        )
        # Also check Total Item Value
        if not fob:
            fob = self.extract_field(
                'total_item_value',
                [r'Total\s*Item\s*Value', r'Total\s*Invoice\s*Value'],
                [r'Total\s*(?:Item|Invoice)\s*Value.*:?\s*([\d,\.]+)'],
                processor=to_decimal
            )
        self.data['declared_value'] = fob
        
        # === CURRENCY (exhaustive search) ===
        currency = self.extract_field(
            'currency',
            [r'Currency', r'FOB\s*Currency'],
            [r'\b(USD|INR|EUR|GBP|AED|SGD|CNY|JPY)\b']
        )
        if currency:
            self.data['currency'] = currency[:3].upper()
        
        # === PARTIES ===
        self.data['exporter_name'] = self.extract_field(
            'exporter_name',
            [r'Name\s*of\s*(?:the\s*)?Consignor', r'Exporter', r'Shipper'],
            [
                r'Name\s*of\s*(?:the\s*)?Consignor\s*:?\s*([^\n]{10,150})',
                r'Exporter\s*(?:Name)?\s*:?\s*([^\n]{10,150})',
            ]
        )
        
        self.data['exporter_address'] = self.extract_field(
            'exporter_address',
            [r'Address\s*of\s*(?:the\s*)?Consignor'],
            [r'Address\s*of\s*(?:the\s*)?Consignor\s*:?\s*([^\n]{15,250})']
        )
        
        self.data['consignee_name'] = self.extract_field(
            'consignee_name',
            [r'Name\s*of\s*(?:the\s*)?Consignee', r'Buyer', r'Importer'],
            [
                r'Name\s*of\s*(?:the\s*)?Consignee\s*:?\s*([^\n]{2,150})',
                r'Consignee\s*:?\s*([^\n]{2,150})',
            ]
        )
        
        consignee_addr = self.extract_field(
            'consignee_address',
            [r'Address\s*of\s*(?:the\s*)?Consignee'],
            [r'Address\s*of\s*(?:the\s*)?Consignee\s*:?\s*([^\n]{15,300})']
        )
        self.data['consignee_address'] = consignee_addr
        
        # === COUNTRY (CRITICAL - extract from address or find separately) ===
        country = None
        if consignee_addr:
            # Look for country keywords in address
            addr_upper = consignee_addr.upper()
            countries = {
                'AUSTRALIA': 'Australia',
                'UNITED STATES': 'United States',
                'USA': 'United States',
                'CANADA': 'Canada',
                'UK': 'United Kingdom',
                'UNITED KINGDOM': 'United Kingdom',
                'SINGAPORE': 'Singapore',
                'UAE': 'UAE',
                'GERMANY': 'Germany',
                'FRANCE': 'France',
                'CHINA': 'China',
                'JAPAN': 'Japan',
                'HONG KONG': 'Hong Kong',
                'THAILAND': 'Thailand',
                'MALAYSIA': 'Malaysia',
                'NETHERLANDS': 'Netherlands',
                'BELGIUM': 'Belgium',
                'ITALY': 'Italy',
                'SPAIN': 'Spain',
            }
            for keyword, name in countries.items():
                if keyword in addr_upper:
                    country = name
                    break
        
        # Fallback: search for "Country" field
        if not country:
            country = self.extract_field(
                'consignee_country',
                [r'Country', r'Destination\s*Country'],
                [
                    r'Country\s*(?:of\s*Destination)?\s*:?\s*([A-Za-z\s]+)',
                    r'Destination\s*:?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
                ]
            )
        self.data['consignee_country'] = country
        
        # === PORTS ===
        self.data['port_of_loading'] = self.extract_field(
            'port_of_loading',
            [r'Port\s*of\s*Loading', r'POL'],
            [
                r'Port\s*of\s*Loading\s*:?\s*([A-Z0-9\s]+)',
                r'POL\s*:?\s*([A-Z0-9]+)',
            ]
        )
        
        pod = self.extract_field(
            'port_of_discharge',
            [r'Airport\s*of\s*Destination', r'Port\s*of\s*Discharge', r'POD'],
            [
                r'Airport\s*of\s*Destination\s*:?\s*([A-Z0-9]+)',
                r'Port\s*of\s*Discharge\s*:?\s*([A-Z0-9\s]+)',
                r'POD\s*:?\s*([A-Z0-9]+)',
            ]
        )
        self.data['port_of_discharge'] = pod
        
        # === CARGO ===
        packages = self.extract_field(
            'total_packages',
            [r'Number\s*of\s*Packages', r'No\.?\s*of\s*Packages', r'Packages'],
            [
                r'Number\s*of\s*Packages.*:?\s*(\d+)',
                r'(?:No\.?\s*of\s*)?Packages\s*:?\s*(\d+)',
            ]
        )
        if packages:
            packages = re.search(r'(\d+)', packages).group(1) if re.search(r'(\d+)', packages) else packages
        self.data['total_packages'] = packages
        
        # Quantity (separate from packages)
        qty = self.extract_field(
            'quantity',
            [r'Quantity'],
            [r'Quantity\s*:?\s*(\d+)']
        )
        if qty:
            qty = re.search(r'(\d+)', qty).group(1) if re.search(r'(\d+)', qty) else qty
        self.data['quantity'] = qty
        
        self.data['gross_weight'] = self.extract_field(
            'gross_weight',
            [r'Declared\s*Weight', r'Gross\s*Weight'],
            [
                r'Declared\s*Weight.*:?\s*([\d\.]+(?:\s*(?:kg|Kg|KG|kgs))?)',
                r'Gross\s*Weight\s*:?\s*([\d\.]+(?:\s*(?:kg|Kg|KG|kgs))?)',
            ]
        )
        
        # === HS CODE ===
        hs = self.extract_field(
            'hs_code',
            [r'CTSH', r'HS\s*Code', r'Tariff'],
            [
                r'CTSH\s*:?\s*(\d{4,10})',
                r'HS\s*Code\s*:?\s*(\d{4,10})',
                r'Tariff\s*:?\s*(\d{4,10})',
            ]
        )
        if hs:
            hs = re.search(r'(\d{4,10})', hs).group(1) if re.search(r'(\d{4,10})', hs) else hs
        self.data['hs_code'] = hs
        
        # === ITEM DESCRIPTION ===
        self.data['item_description'] = self.extract_field(
            'item_description',
            [r'Goods\s*Description', r'Description', r'Item'],
            [
                r'Goods\s*Description\s*:?\s*([^\n]{5,200})',
                r'Description\s*:?\s*([^\n]{5,200})',
            ]
        )
        
        # === SKU (if present) ===
        sku = self.extract_field(
            'sku',
            [r'SKU', r'SKU\s*NO'],
            [r'SKU\s*(?:NO|Number)?\s*:?\s*([A-Z0-9\-]+)']
        )
        self.data['sku'] = sku
        
        # === COMPLIANCE ===
        self.data['iec_code'] = self.extract_field(
            'iec_code',
            [r'Import\s*Export\s*Code', r'IEC'],
            [
                r'Import\s*Export\s*Code.*:?\s*([A-Z0-9]{10})',
                r'IEC\s*:?\s*([A-Z0-9]{10})',
            ]
        )
        
        self.data['ad_code'] = self.extract_field(
            'ad_code',
            [r'AD\s*Code'],
            [r'AD\s*Code\s*:?\s*(\d{7,10})']
        )
        
        gstin = self.extract_field(
            'gstin',
            [r'GSTIN', r'KYC\s*ID', r'GST\s*No'],
            [
                r'GSTIN.*:?\s*([A-Z0-9]{15})',
                r'KYC\s*ID\s*:?\s*([A-Z0-9]{15})',
                r'GST\s*No\.?\s*:?\s*([A-Z0-9]{15})',
            ]
        )
        self.data['gstin'] = gstin
        
        # === SCHEME ===
        scheme = self.extract_field(
            'scheme',
            [r'Under\s*MEIS', r'Scheme'],
            [
                r'Under\s*MEIS\s*Scheme\s*:?\s*([A-Z]+)',
                r'Scheme\s*:?\s*([^\n]{3,50})',
            ]
        )
        if scheme and scheme.upper() in ['NO', 'N']:
            scheme = None
        elif scheme and scheme.upper() in ['YES', 'Y']:
            scheme = 'MEIS'
        self.data['scheme'] = scheme
        
        # === TRANSPORT ===
        self.data['airline'] = self.extract_field(
            'airline',
            [r'Airlines', r'Airline'],
            [r'Airlines?\s*:?\s*([A-Z\s]+(?:ASIA|AIRWAYS|AIR|CARGO|EXPRESS)[A-Z\s]*)']
        )
        
        self.data['flight_number'] = self.extract_field(
            'flight_number',
            [r'Flight\s*Number'],
            [r'Flight\s*Number\s*:?\s*([A-Z0-9\s]+)']
        )
        
        # Mode of transport
        if 'flight' in self.full_text.lower() or 'airline' in self.full_text.lower() or 'airport' in self.full_text.lower():
            self.data['mode_of_transport'] = 'AIR'
        elif 'vessel' in self.full_text.lower() or 'ship' in self.full_text.lower() or 'sea' in self.full_text.lower():
            self.data['mode_of_transport'] = 'SEA'
        else:
            self.data['mode_of_transport'] = None
        
        return self.data

@app.get("/")
async def root():
    return {
        "service": "pdfduck API",
        "version": "3.0.0",
        "extraction": "Exhaustive multi-method extraction engine",
        "features": [
            "Tries ALL methods per field (tables + regex + context)",
            "Handles CSB-V, invoices, BoL, any structured form",
            "Zero metadata in CSV output",
            "Production-ready"
        ]
    }

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.post("/extract")
async def extract_pdf(file: UploadFile = File(...)):
    """
    Extract ONE clean summary row per PDF.
    No metadata fields in output.
    """
    
    if not file.filename.endswith('.pdf'):
        raise HTTPException(status_code=400, detail="File must be a PDF")
    
    try:
        pdf_bytes = await file.read()
        
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            engine = ExtractionEngine(pdf)
            data = engine.extract()
        
        # Remove any None values for cleaner CSV
        data = {k: v for k, v in data.items() if v is not None}
        
        return JSONResponse({
            "success": True,
            "method": "exhaustive_extraction",
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