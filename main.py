from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pdfplumber
import io
import re
from typing import Dict, Any, List, Optional
from datetime import datetime
from decimal import Decimal

app = FastAPI(title="pdfduck API", version="4.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def clean(text: str) -> Optional[str]:
    """Clean text, return None if empty/useless."""
    if not text:
        return None
    text = re.sub(r'\s+', ' ', str(text)).strip()
    return text if text and text not in ['N/A', 'NA', '-', '', 'None', 'null'] else None


def parse_date(date_str: str) -> Optional[str]:
    """Parse any common date format → YYYY-MM-DD."""
    if not date_str:
        return None
    date_str = clean(date_str)
    if not date_str:
        return None
    formats = ['%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y', '%Y-%m-%d',
               '%d/%m/%y', '%d-%m-%y', '%d%m%Y']
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except Exception:
            continue
    return date_str


def to_decimal(val: str) -> Optional[str]:
    """Extract clean decimal number from string."""
    if not val:
        return None
    try:
        cleaned = re.sub(r'[^\d.]', '', str(val))
        if cleaned and cleaned not in ['.', '']:
            return str(Decimal(cleaned))
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Extraction engine
# ---------------------------------------------------------------------------

class ExtractionEngine:
    """
    Exhaustive multi-method field extractor for CSB-V and similar structured PDFs.

    Table layout note:
        CSB-V renders rows like:
            [key1 | value1 | key2 | value2]
        so we index ALL even columns as keys and the following odd column as value.
    """

    def __init__(self, pdf):
        self.pdf = pdf
        self.full_text = ""
        self.tables: List[List[List[Optional[str]]]] = []

        for page in pdf.pages:
            text = page.extract_text()
            if text:
                self.full_text += text + "\n"

            page_tables = page.extract_tables()
            if page_tables:
                self.tables.extend(page_tables)

        self.data: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Low-level search helpers
    # ------------------------------------------------------------------

    def find_in_tables(self, key_patterns: List[str],
                       value_col_offset: int = 1) -> Optional[str]:
        """
        Search every table row.
        Checks ALL even-indexed columns as potential keys (col 0, 2, 4…)
        so that multi-column CSB-V rows are handled correctly.
        """
        for table in self.tables:
            if not table:
                continue
            for row in table:
                if not row:
                    continue
                # Try each even column as a key cell
                for key_col in range(0, len(row), 2):
                    val_col = key_col + value_col_offset
                    if val_col >= len(row):
                        continue

                    key_text = clean(str(row[key_col])) if row[key_col] else ""
                    if not key_text:
                        continue

                    for pattern in key_patterns:
                        if re.search(pattern, key_text, re.IGNORECASE):
                            val = clean(str(row[val_col])) if row[val_col] else None
                            if val:
                                return val
        return None

    def find_in_text(self, patterns: List[str], group: int = 1) -> Optional[str]:
        """Search full concatenated text with regex patterns."""
        for pattern in patterns:
            match = re.search(pattern, self.full_text, re.IGNORECASE | re.MULTILINE)
            if match:
                try:
                    val = clean(match.group(group))
                    if val:
                        return val
                except IndexError:
                    continue
        return None

    def find_all_in_text(self, patterns: List[str]) -> List[str]:
        """Return all unique matches for given patterns."""
        results: List[str] = []
        for pattern in patterns:
            matches = re.findall(pattern, self.full_text, re.IGNORECASE)
            for m in matches:
                val = clean(m if isinstance(m, str) else m[0])
                if val and val not in results:
                    results.append(val)
        return results

    def extract_field(self, field_name: str,
                      table_keys: List[str],
                      text_patterns: List[str],
                      processor=None) -> Optional[str]:
        """Try tables first, then regex on full text; optionally post-process."""
        val = self.find_in_tables(table_keys)
        if not val:
            val = self.find_in_text(text_patterns)
        if val and processor:
            val = processor(val)
        return val

    # ------------------------------------------------------------------
    # Main extraction
    # ------------------------------------------------------------------

    def extract(self) -> Dict[str, Any]:

        # ── SHIPPING BILL (CSB) NUMBER ──────────────────────────────────
        # Keep the FULL CSB number (e.g. CSBV_DEL_2024-2025_2012_16454)
        csb_raw = self.find_in_tables([r'CSB\s*Number', r'Shipping\s*Bill\s*No'])
        if not csb_raw:
            csb_raw = self.find_in_text([
                r'CSB\s*Number\s*[:\|]?\s*([A-Z0-9_\-/\s]+)',
                r'CSBV[_\-][A-Z0-9_\-]+',
            ])
        if csb_raw:
            # Normalise internal whitespace but keep full alphanumeric structure
            self.data['shipping_bill_no'] = re.sub(r'\s+', '', csb_raw)

        # ── DATES ───────────────────────────────────────────────────────
        self.data['filling_date'] = self.extract_field(
            'filling_date',
            [r'Fill(?:ing)?\s*Date', r'SB\s*Date', r'Bill\s*Date'],
            [
                r'Fill(?:ing)?\s*Date\s*[:\|]?\s*([\d/\-\.]+)',
                r'SB\s*Date\s*[:\|]?\s*([\d/\-\.]+)',
            ],
            processor=parse_date
        )

        self.data['invoice_date'] = self.extract_field(
            'invoice_date',
            [r'Invoice\s*Date'],
            [r'Invoice\s*Date\s*[:\|]?\s*([\d/\-\.]+)'],
            processor=parse_date
        )

        self.data['date_of_departure'] = self.extract_field(
            'date_of_departure',
            [r'Date\s*of\s*Departure'],
            [r'Date\s*of\s*Departure\s*[:\|]?\s*([\d/\-\.]+)'],
            processor=parse_date
        )

        self.data['egm_date'] = self.extract_field(
            'egm_date',
            [r'EGM\s*Date'],
            [r'EGM\s*Date\s*[:\|]?\s*([\d/\-\.]+)'],
            processor=parse_date
        )

        leo_raw = self.extract_field(
            'leo_date',
            [r'LEO\s*DATE', r'LEO\s*Date'],
            [r'LEO\s*DATE?\s*[:\|]?\s*([\d/\-\.]+)'],
            processor=parse_date
        )
        self.data['leo_date'] = leo_raw

        # ── INVOICE NUMBER (critical) ────────────────────────────────────
        invoice = self.extract_field(
            'invoice_number',
            [r'Invoice\s*Number', r'Invoice\s*No\.?'],
            [
                r'Invoice\s*(?:Number|No\.?)\s*[:\|]?\s*([A-Za-z0-9\-/]+)',
                r'Inv\.?\s*(?:No\.?|#)\s*[:\|]?\s*([A-Za-z0-9\-/]+)',
            ]
        )
        if not invoice:
            # Pattern: 2–8 alpha chars followed by hyphen + 6–12 digits/alphanums
            candidates = self.find_all_in_text([r'\b([A-Za-z]{2,8}[-/]\d{6,12})\b'])
            if candidates:
                invoice = candidates[0]
        self.data['invoice_number'] = invoice

        # ── TRACKING / HAWB ─────────────────────────────────────────────
        self.data['hawb_number'] = self.extract_field(
            'hawb_number',
            [r'HAWB\s*Number', r'AWB\s*Number'],
            [
                r'HAWB\s*Number\s*[:\|]?\s*(\d{8,15})',
                r'AWB\s*(?:No\.?|Number)\s*[:\|]?\s*([A-Z0-9]+)',
            ]
        )

        # ── FINANCIAL ───────────────────────────────────────────────────
        # FOB Value (INR)
        fob = self.extract_field(
            'fob_value_inr',
            [r'FOB\s*Value.*INR', r'FOB\s*Value\s*\(In\s*INR\)'],
            [
                r'FOB\s*Value\s*\(?In\s*INR\)?\s*[:\|]?\s*([\d,\.]+)',
                r'FOB\s*Value\s*[:\|]?\s*([\d,\.]+)',
            ],
            processor=to_decimal
        )
        self.data['fob_value_inr'] = fob

        # FOB Value (Foreign Currency)
        fob_fc = self.extract_field(
            'fob_value_fc',
            [r'FOB\s*Value.*Foreign\s*Cur', r'FOB\s*Value\s*\(In\s*Foreign'],
            [r'FOB\s*Value\s*\(?In\s*Foreign\s*Cur[^:]*\)?\s*[:\|]?\s*([\d,\.]+)'],
            processor=to_decimal
        )
        self.data['fob_value_fc'] = fob_fc

        # FOB Currency
        currency = self.extract_field(
            'fob_currency',
            [r'FOB\s*Currency'],
            [
                r'FOB\s*Currency\s*[:\|]?\s*([A-Z]{3})',
                r'\b(USD|INR|EUR|GBP|AED|SGD|CNY|JPY|AUD)\b',
            ]
        )
        if currency:
            self.data['fob_currency'] = currency[:3].upper()

        # FOB Exchange Rate
        self.data['fob_exchange_rate'] = self.extract_field(
            'fob_exchange_rate',
            [r'FOB\s*Exchange\s*Rate'],
            [r'FOB\s*Exchange\s*Rate[^:]*[:\|]?\s*([\d\.]+)'],
            processor=to_decimal
        )

        # Invoice Value
        self.data['invoice_value_inr'] = self.extract_field(
            'invoice_value_inr',
            [r'Invoice\s*Value.*INR', r'Invoice\s*Value\s*\(in\s*INR\)'],
            [
                r'Invoice\s*Value\s*\(?in\s*INR\)?\s*[:\|]?\s*([\d,\.]+)',
                r'Invoice\s*Value\s*[:\|]?\s*([\d,\.]+)',
            ],
            processor=to_decimal
        )

        # Unit Price
        self.data['unit_price'] = self.extract_field(
            'unit_price',
            [r'Unit\s*Price'],
            [r'Unit\s*Price\s*[:\|]?\s*([\d,\.]+)'],
            processor=to_decimal
        )

        # Unit Price Currency
        upc = self.extract_field(
            'unit_price_currency',
            [r'Unit\s*Price\s*Currency'],
            [r'Unit\s*Price\s*Currency\s*[:\|]?\s*([A-Z]{3})']
        )
        if upc:
            self.data['unit_price_currency'] = upc[:3].upper()

        # Total Item Value
        self.data['total_item_value'] = self.extract_field(
            'total_item_value',
            [r'Total\s*Item\s*Value'],
            [r'Total\s*Item\s*Value\s*[:\|]?\s*([\d,\.]+)'],
            processor=to_decimal
        )

        # Total Item Value (INR)
        self.data['total_item_value_inr'] = self.extract_field(
            'total_item_value_inr',
            [r'Total\s*Item\s*Value\s*\(?In\s*INR\)?'],
            [r'Total\s*Item\s*Value\s*\(?In\s*INR\)?\s*[:\|]?\s*([\d,\.]+)'],
            processor=to_decimal
        )

        # Total Taxable Value
        self.data['total_taxable_value'] = self.extract_field(
            'total_taxable_value',
            [r'Total\s*Taxable\s*Value'],
            [r'Total\s*Taxable\s*Value\s*[:\|]?\s*([\d,\.]+)'],
            processor=to_decimal
        )

        # Total IGST Paid
        self.data['total_igst_paid'] = self.extract_field(
            'total_igst_paid',
            [r'Total\s*IGST\s*Paid'],
            [r'Total\s*IGST\s*Paid\s*[:\|]?\s*([\d,\.]+)'],
            processor=to_decimal
        )

        # Total CESS Paid
        self.data['total_cess_paid'] = self.extract_field(
            'total_cess_paid',
            [r'Total\s*CESS\s*Paid'],
            [r'Total\s*CESS\s*Paid\s*[:\|]?\s*([\d,\.]+)'],
            processor=to_decimal
        )

        # Exchange Rate (item level)
        self.data['exchange_rate'] = self.extract_field(
            'exchange_rate',
            [r'^Exchange\s*Rate$'],
            [r'Exchange\s*Rate\s*[:\|]?\s*([\d\.]+)'],
            processor=to_decimal
        )

        # ── PARTIES ─────────────────────────────────────────────────────
        self.data['exporter_name'] = self.extract_field(
            'exporter_name',
            [r'Name\s*of\s*(?:the\s*)?Consignor', r'Exporter', r'Shipper'],
            [
                r'Name\s*of\s*(?:the\s*)?Consignor\s*[:\|]?\s*([^\n]{5,150})',
                r'Exporter\s*(?:Name)?\s*[:\|]?\s*([^\n]{5,150})',
            ]
        )

        self.data['exporter_address'] = self.extract_field(
            'exporter_address',
            [r'Address\s*of\s*(?:the\s*)?Consignor'],
            [r'Address\s*of\s*(?:the\s*)?Consignor\s*[:\|]?\s*([^\n]{10,300})']
        )

        self.data['consignee_name'] = self.extract_field(
            'consignee_name',
            [r'Name\s*of\s*(?:the\s*)?Consignee', r'Buyer', r'Importer'],
            [
                r'Name\s*of\s*(?:the\s*)?Consignee\s*[:\|]?\s*([^\n]{2,150})',
                r'Consignee\s*[:\|]?\s*([^\n]{2,150})',
            ]
        )

        consignee_addr = self.extract_field(
            'consignee_address',
            [r'Address\s*of\s*(?:the\s*)?Consignee'],
            [r'Address\s*of\s*(?:the\s*)?Consignee\s*[:\|]?\s*([^\n]{10,300})']
        )
        self.data['consignee_address'] = consignee_addr

        # ── COUNTRY (from address or dedicated field) ────────────────────
        country = None
        if consignee_addr:
            addr_upper = consignee_addr.upper()
            country_map = {
                'AUSTRALIA': 'Australia',
                'UNITED STATES': 'United States',
                'USA': 'United States',
                'CANADA': 'Canada',
                'UNITED KINGDOM': 'United Kingdom',
                'UK': 'United Kingdom',
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
                'NEW ZEALAND': 'New Zealand',
                'SOUTH AFRICA': 'South Africa',
            }
            for keyword, name in country_map.items():
                if keyword in addr_upper:
                    country = name
                    break
        if not country:
            country = self.extract_field(
                'consignee_country',
                [r'Country', r'Destination\s*Country'],
                [
                    r'Country\s*(?:of\s*Destination)?\s*[:\|]?\s*([A-Za-z\s]+)',
                    r'Destination\s*[:\|]?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
                ]
            )
        self.data['consignee_country'] = country

        # ── PORTS ───────────────────────────────────────────────────────
        self.data['port_of_loading'] = self.extract_field(
            'port_of_loading',
            [r'Port\s*of\s*Loading', r'POL'],
            [
                r'Port\s*of\s*Loading\s*[:\|]?\s*([A-Z0-9\s]{2,30})',
                r'POL\s*[:\|]?\s*([A-Z0-9]{2,10})',
            ]
        )

        self.data['port_of_discharge'] = self.extract_field(
            'port_of_discharge',
            [r'Airport\s*of\s*Destination', r'Port\s*of\s*Discharge', r'POD'],
            [
                r'Airport\s*of\s*Destination\s*[:\|]?\s*([A-Z0-9]{2,10})',
                r'Port\s*of\s*Discharge\s*[:\|]?\s*([A-Z0-9\s]{2,30})',
            ]
        )

        self.data['custom_station'] = self.extract_field(
            'custom_station',
            [r'Custom\s*Station\s*Name'],
            [r'Custom\s*Station\s*Name\s*[:\|]?\s*([A-Z0-9]{2,10})']
        )

        # ── CARGO ───────────────────────────────────────────────────────
        packages = self.extract_field(
            'total_packages',
            [r'Number\s*of\s*Packages', r'No\.?\s*of\s*Packages'],
            [
                r'Number\s*of\s*Packages[^:]*[:\|]?\s*(\d+)',
                r'(?:No\.?\s*of\s*)?Packages\s*[:\|]?\s*(\d+)',
            ]
        )
        if packages:
            m = re.search(r'(\d+)', packages)
            packages = m.group(1) if m else packages
        self.data['total_packages'] = packages

        qty = self.extract_field(
            'quantity',
            [r'^Quantity$'],
            [r'\bQuantity\s*[:\|]?\s*(\d+)\b']
        )
        if qty:
            m = re.search(r'(\d+)', qty)
            qty = m.group(1) if m else qty
        self.data['quantity'] = qty

        self.data['unit_of_measure'] = self.extract_field(
            'unit_of_measure',
            [r'Unit\s*Of\s*Measure', r'UOM'],
            [r'Unit\s*Of\s*Measure\s*[:\|]?\s*([A-Z]{2,10})']
        )

        self.data['gross_weight'] = self.extract_field(
            'gross_weight',
            [r'Declared\s*Weight', r'Gross\s*Weight'],
            [
                r'Declared\s*Weight[^:]*[:\|]?\s*([\d\.]+)',
                r'Gross\s*Weight\s*[:\|]?\s*([\d\.]+)',
            ]
        )

        # ── HS CODE ─────────────────────────────────────────────────────
        hs = self.extract_field(
            'hs_code',
            [r'CTSH', r'HS\s*Code', r'Tariff'],
            [
                r'CTSH\s*[:\|]?\s*(\d{4,10})',
                r'HS\s*Code\s*[:\|]?\s*(\d{4,10})',
                r'Tariff\s*[:\|]?\s*(\d{4,10})',
            ]
        )
        if hs:
            m = re.search(r'(\d{4,10})', hs)
            hs = m.group(1) if m else hs
        self.data['hs_code'] = hs

        # ── ITEM DESCRIPTION / SKU ───────────────────────────────────────
        self.data['item_description'] = self.extract_field(
            'item_description',
            [r'Goods\s*Description', r'Description\s*of\s*Goods'],
            [
                r'Goods\s*Description\s*[:\|]?\s*([^\n]{3,200})',
                r'Description\s*of\s*Goods\s*[:\|]?\s*([^\n]{3,200})',
            ]
        )

        sku = self.extract_field(
            'sku',
            [r'SKU\s*NO', r'SKU\b'],
            [r'SKU\s*(?:NO|Number)?\s*[:\|]?\s*([A-Za-z0-9\-/]+)']
        )
        self.data['sku'] = sku

        # ── COMPLIANCE / REFERENCE CODES ────────────────────────────────
        self.data['iec_code'] = self.extract_field(
            'iec_code',
            [r'Import\s*Export\s*Code\s*\(IEC\)', r'Import\s*Export\s*Code', r'^IEC$'],
            [
                r'Import\s*Export\s*Code\s*\(?IEC\)?\s*[:\|]?\s*([A-Z0-9]{10})',
                r'\bIEC\b\s*[:\|]?\s*([A-Z0-9]{10})',
            ]
        )

        self.data['iec_branch_code'] = self.extract_field(
            'iec_branch_code',
            [r'IEC\s*Branch\s*Code'],
            [r'IEC\s*Branch\s*Code\s*[:\|]?\s*(\d+)']
        )

        self.data['ad_code'] = self.extract_field(
            'ad_code',
            [r'AD\s*Code'],
            [r'AD\s*Code\s*[:\|]?\s*(\d{5,10})']
        )

        self.data['account_no'] = self.extract_field(
            'account_no',
            [r'Account\s*No'],
            [r'Account\s*No\s*[:\|]?\s*(\d{8,18})']
        )

        gstin = self.extract_field(
            'gstin',
            [r'KYC\s*ID', r'GSTIN', r'GST\s*No'],
            [
                r'KYC\s*ID\s*[:\|]?\s*([A-Z0-9]{15})',
                r'GSTIN\s*[:\|]?\s*([A-Z0-9]{15})',
                r'GST\s*No\.?\s*[:\|]?\s*([A-Z0-9]{15})',
            ]
        )
        self.data['gstin'] = gstin

        kyc_doc = self.extract_field(
            'kyc_document',
            [r'KYC\s*Document'],
            [r'KYC\s*Document\s*[:\|]?\s*([^\n]{3,50})']
        )
        self.data['kyc_document'] = kyc_doc

        self.data['state_code'] = self.extract_field(
            'state_code',
            [r'State\s*Code'],
            [r'State\s*Code\s*[:\|]?\s*(\d{1,2})']
        )

        # ── REFERENCE NUMBERS ───────────────────────────────────────────
        self.data['mhbs_no'] = self.extract_field(
            'mhbs_no',
            [r'MHBS\s*No'],
            [r'MHBS\s*No\s*[:\|]?\s*([A-Z0-9\-]+)']
        )

        self.data['egm_number'] = self.extract_field(
            'egm_number',
            [r'EGM\s*Number'],
            [r'EGM\s*Number\s*[:\|]?\s*([A-Z0-9\-/]+)']
        )

        # CRN Numbers (there can be multiple)
        crn_numbers = self.find_all_in_text([r'\b(CRN[-\s]?\d{8,15})\b', r'\b(\d{11,15})\b'])
        # Use the HAWB-style numbers that repeat as CRNs
        hawb = self.data.get('hawb_number')
        if hawb:
            crn_matches = re.findall(
                rf'\b{re.escape(hawb)}\b', self.full_text, re.IGNORECASE
            )
            if len(crn_matches) > 1:
                # Extract MHBS Numbers paired with CRN
                mhbs_list = re.findall(r'(ARR-\d+)', self.full_text)
                unique_mhbs = list(dict.fromkeys(mhbs_list))  # dedup preserving order
                self.data['crn_mhbs_numbers'] = unique_mhbs if unique_mhbs else None
        self.data['crn_number'] = hawb  # CRN Number == HAWB Number in CSB-V

        # ── STATUS ──────────────────────────────────────────────────────
        self.data['status'] = self.extract_field(
            'status',
            [r'^Status$'],
            [r'\bStatus\s*[:\|]?\s*([A-Z]+)']
        )

        # ── SCHEME / FLAGS ──────────────────────────────────────────────
        scheme = self.extract_field(
            'scheme',
            [r'Under\s*MEIS\s*Scheme', r'Scheme'],
            [r'Under\s*MEIS\s*Scheme\s*[:\|]?\s*([A-Z]+)']
        )
        if scheme:
            if scheme.upper() in ('NO', 'N'):
                scheme = 'NO'
            elif scheme.upper() in ('YES', 'Y'):
                scheme = 'YES'
        self.data['under_meis_scheme'] = scheme

        self.data['nfei'] = self.extract_field(
            'nfei',
            [r'NFEI'],
            [r'\bNFEI\s*[:\|]?\s*([A-Z]+)']
        )

        govt = self.extract_field(
            'government_nongovernment',
            [r'Government.*Non.?Government', r'Govt.*Non.?Govt'],
            [r'(?:Government/Non-Government)\s*[:\|]?\s*([A-Z\-]+)']
        )
        self.data['government_nongovernment'] = govt

        ecommerce = self.extract_field(
            'export_using_ecommerce',
            [r'Export\s*Using\s*e.Commerce', r'e.Commerce'],
            [r'Export\s*Using\s*e.Commerce\s*[:\|]?\s*([YN])']
        )
        self.data['export_using_ecommerce'] = ecommerce

        bond_or_ut = self.extract_field(
            'bond_or_ut',
            [r'BOND\s*OR\s*UT'],
            [r'BOND\s*OR\s*UT\s*[:\|]?\s*([A-Z]+)']
        )
        self.data['bond_or_ut'] = bond_or_ut

        # ── COURIER DETAILS ─────────────────────────────────────────────
        self.data['courier_name'] = self.extract_field(
            'courier_name',
            [r'Courier\s*Name'],
            [r'Courier\s*Name\s*[:\|]?\s*([^\n]{3,80})']
        )

        self.data['courier_reg_no'] = self.extract_field(
            'courier_reg_no',
            [r'Courier\s*Registration\s*Num', r'Courier\s*Reg'],
            [r'Courier\s*Registration\s*Num(?:ber)?\s*[:\|]?\s*([A-Z0-9]+)']
        )

        # ── TRANSPORT ───────────────────────────────────────────────────
        self.data['airline'] = self.extract_field(
            'airline',
            [r'^Airlines?$'],
            [r'Airlines?\s*[:\|]?\s*([^\n]{3,60})']
        )

        self.data['flight_number'] = self.extract_field(
            'flight_number',
            [r'Flight\s*Number'],
            [r'Flight\s*Number\s*[:\|]?\s*([A-Z0-9\s]+)']
        )

        # Mode of transport (inferred)
        lower = self.full_text.lower()
        if any(k in lower for k in ('flight', 'airline', 'airport', 'hawb')):
            self.data['mode_of_transport'] = 'AIR'
        elif any(k in lower for k in ('vessel', 'ship', 'sea', 'bl no', 'bill of lading')):
            self.data['mode_of_transport'] = 'SEA'
        else:
            self.data['mode_of_transport'] = None

        return self.data


# ---------------------------------------------------------------------------
# FastAPI routes
# ---------------------------------------------------------------------------

@app.get("/")
async def root():
    return {
        "service": "pdfduck API",
        "version": "4.0.0",
        "description": "Ultimate CSB-V / shipping-bill PDF extractor",
        "fields_extracted": [
            "shipping_bill_no (full CSBV number)",
            "filling_date", "invoice_date", "date_of_departure",
            "egm_date", "leo_date",
            "invoice_number",
            "hawb_number", "crn_number", "crn_mhbs_numbers", "mhbs_no", "egm_number",
            "fob_value_inr", "fob_value_fc", "fob_currency", "fob_exchange_rate",
            "invoice_value_inr", "unit_price", "unit_price_currency",
            "total_item_value", "total_item_value_inr",
            "total_taxable_value", "total_igst_paid", "total_cess_paid", "exchange_rate",
            "exporter_name", "exporter_address",
            "consignee_name", "consignee_address", "consignee_country",
            "port_of_loading", "port_of_discharge", "custom_station",
            "total_packages", "quantity", "unit_of_measure", "gross_weight",
            "hs_code", "item_description", "sku",
            "iec_code", "iec_branch_code", "ad_code", "account_no",
            "gstin", "kyc_document", "state_code",
            "status", "under_meis_scheme", "nfei",
            "government_nongovernment", "export_using_ecommerce", "bond_or_ut",
            "courier_name", "courier_reg_no",
            "airline", "flight_number", "mode_of_transport",
        ]
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.post("/extract")
async def extract_pdf(file: UploadFile = File(...)):
    """
    Extract all fields from a CSB-V / shipping-bill PDF.
    Returns one clean row per PDF with all available fields populated.
    """
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="File must be a PDF")

    try:
        pdf_bytes = await file.read()

        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            engine = ExtractionEngine(pdf)
            data = engine.extract()

        # Strip None values for clean CSV output
        data = {k: v for k, v in data.items() if v is not None}

        return JSONResponse({
            "success": True,
            "method": "exhaustive_multi_column_extraction",
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