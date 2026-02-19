from __future__ import annotations

import asyncio
import io
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

import pdfplumber
from fastapi import BackgroundTasks, FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI(title="pdfduck API", version="5.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_JUNK = frozenset(["N/A", "NA", "-", "", "None", "null", "NONE"])


def clean(text: Any) -> Optional[str]:
    if text is None:
        return None
    s = re.sub(r"\s+", " ", str(text)).strip()
    return s if s and s not in _JUNK else None


def normalize_key(text: Any) -> str:
    if text is None:
        return ""
    return re.sub(r"[\s:\-/\(\)\[\]\.]+", "", str(text)).lower()


def is_data_value(val: str) -> bool:
    if not val:
        return False
    s = val.strip()
    if s.endswith(":"):
        return False
    nospace = re.sub(r"\s+", "", s)
    if len(nospace) > 15 and nospace == nospace.upper() and re.match(r"^[A-Z]+$", nospace):
        return False
    return True


def parse_date(raw: str) -> Optional[str]:
    s = clean(raw)
    if not s:
        return None
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y", "%Y-%m-%d",
                "%d/%m/%y", "%d-%m-%y", "%d%m%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return s


def to_decimal(raw: str) -> Optional[str]:
    if not raw:
        return None
    m = re.search(r"[\d,]+\.?\d*", str(raw))
    if not m:
        return None
    try:
        return str(Decimal(m.group().replace(",", "")))
    except InvalidOperation:
        return None


def first_int(s: str) -> Optional[str]:
    m = re.search(r"\d+", str(s))
    return m.group() if m else None


class ExtractionEngine:
    def __init__(self, pdf: pdfplumber.PDF) -> None:
        self.full_text: str = ""
        self.tables: List[List[List[Optional[str]]]] = []

        for page in pdf.pages:
            self.full_text += (page.extract_text() or "") + "\n"
            for tbl in page.extract_tables() or []:
                self.tables.append(tbl)

        self._kv: Dict[str, str] = {}
        self._build_kv_index()

    def _build_kv_index(self) -> None:
        for table in self.tables:
            if not table:
                continue
            n_rows = len(table)

            for r_idx, row in enumerate(table):
                if not row:
                    continue
                n_cols = len(row)

                # Strategy C (highest priority): header row + next data row
                if r_idx + 1 < n_rows:
                    next_row = table[r_idx + 1]
                    if next_row:
                        for col, cell in enumerate(row):
                            hdr = normalize_key(cell)
                            if not hdr or len(hdr) < 2:
                                continue
                            if col < len(next_row):
                                val = clean(next_row[col])
                                if val and is_data_value(val):
                                    self._kv[hdr] = val

                # Strategy A+B: same-row key-value at offset 1 or 2
                for k_col in range(n_cols):
                    raw_key = normalize_key(row[k_col])
                    if len(raw_key) < 2:
                        continue
                    for offset in (1, 2):
                        v_col = k_col + offset
                        if v_col < n_cols:
                            val = clean(row[v_col])
                            if val and is_data_value(val) and raw_key not in self._kv:
                                self._kv[raw_key] = val
                                break

    def _lookup(self, *key_variants: str) -> Optional[str]:
        for variant in key_variants:
            if variant.startswith("re:"):
                for k, v in self._kv.items():
                    if re.search(variant[3:], k, re.IGNORECASE):
                        return v
            else:
                nk = normalize_key(variant)
                if nk in self._kv:
                    return self._kv[nk]
        return None

    def _text(self, *patterns: str) -> Optional[str]:
        for pattern in patterns:
            m = re.search(pattern, self.full_text, re.IGNORECASE | re.MULTILINE)
            if m:
                try:
                    val = clean(m.group(1))
                    if val:
                        return val
                except IndexError:
                    pass
        return None

    def _text_all(self, *patterns: str) -> List[str]:
        seen: Dict[str, None] = {}
        for pattern in patterns:
            for m in re.findall(pattern, self.full_text, re.IGNORECASE):
                val = clean(m if isinstance(m, str) else m[0])
                if val:
                    seen[val] = None
        return list(seen)

    def _get(self, *key_variants: str, tp: Optional[List[str]] = None, fn=None) -> Optional[str]:
        val = self._lookup(*key_variants)
        if not val and tp:
            val = self._text(*tp)
        if val and fn:
            val = fn(val)
        return val

    def extract(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {}

        csb = self._lookup("CSBNumber", "CSB Number")
        if not csb:
            csb = self._text(r"CSB\s*Number\s*[:\|]?\s*([A-Z0-9_\-/\s]+?)(?=\s+Filling|\s*\n)")
        if csb:
            d["shipping_bill_no"] = re.sub(r"\s+", "", csb)

        d["filling_date"] = self._get("FillingDate", "Filling Date", tp=[r"Fill(?:ing)?\s*Date\s*[:\|]?\s*([\d/\-\.]+)"], fn=parse_date)
        d["date_of_departure"] = self._get("DateofDeparture", "Date of Departure", tp=[r"Date\s*of\s*Departure\s*[:\|]?\s*([\d/\-\.]+)"], fn=parse_date)
        d["egm_date"] = self._get("EGMDate", "EGM Date", tp=[r"EGM\s*Date\s*[:\|]?\s*([\d/\-\.]+)"], fn=parse_date)
        d["leo_date"] = self._get("LEODATE", "LEO DATE", tp=[r"LEO\s*DATE?\s*[:\|]?\s*([\d/\-\.]+)"], fn=parse_date)

        inv = self._lookup("InvoiceNumber", "Invoice Number", "InvoiceNo", "Invoice No")
        if inv and re.fullmatch(r"[\d/\-\.]+", inv):
            inv = None
        if inv and re.search(r"(invoicedate|invoicevalue|date|value)", inv, re.IGNORECASE):
            inv = None
        if not inv:
            inv = self._text(
                r"InvoiceNumber:\s*\n?\s*([A-Za-z][A-Za-z0-9/\-]{3,20})",
                r"Invoice\s*(?:Number|No\.?)\s*[:\|]?\s*([A-Za-z][A-Za-z0-9/\-]{3,20})",
            )
        if not inv:
            candidates = self._text_all(r"\b([A-Za-z]{2,8}[-/]\d{6,12})\b")
            inv = candidates[0] if candidates else None
        d["invoice_number"] = inv

        d["invoice_date"] = self._get("InvoiceDate", "Invoice Date", tp=[r"Invoice\s*Date\s*[:\|]?\s*([\d/\-\.]+)"], fn=parse_date)
        d["invoice_value_inr"] = self._get("InvoiceValue(inINR)", "Invoice Value (in INR)", "re:invoicevalue.*inr", tp=[r"Invoice\s*Value\s*\(?in\s*INR\)?\s*[:\|]?\s*([\d,\.]+)"], fn=to_decimal)
        d["hawb_number"] = self._get("HAWBNumber", "HAWB Number", tp=[r"HAWB\s*Number\s*[:\|]?\s*(\d{8,15})"])
        d["fob_value_inr"] = self._get("FOBValue(InINR)", "FOB Value (In INR)", "re:fobvalue.*inr", tp=[r"FOB\s*Value\s*\(?In\s*INR\)?\s*[:\|]?\s*([\d,\.]+)"], fn=to_decimal)
        d["fob_value_fc"] = self._get("FOBValue(InForeignCurrency)", "re:fobvalue.*foreigncur", tp=[r"FOB\s*Value\s*\(?In\s*Foreign[^:]*\)?\s*[:\|]?\s*([\d,\.]+)"], fn=to_decimal)

        raw_curr = self._get("FOBCurrency(InForeignCurrency)", "re:fobcurrency", tp=[r"FOB\s*Currency[^:]*[:\|]?\s*([A-Z]{3})"])
        d["fob_currency"] = raw_curr[:3].upper() if raw_curr else None

        d["fob_exchange_rate"] = self._get("FOBExchangeRate(InForeignCurrency)", "re:fobexchangerate", tp=[r"FOB\s*Exchange\s*Rate[^:]*[:\|]?\s*([\d\.]+)"], fn=to_decimal)
        d["unit_price"] = self._get("UnitPrice", "Unit Price", tp=[r"Unit\s*Price\s*[:\|]?\s*([\d,\.]+)"], fn=to_decimal)

        raw_upc = self._get("UnitPriceCurrency", "Unit Price Currency", tp=[r"Unit\s*Price\s*Currency\s*[:\|]?\s*([A-Z]{3})"])
        d["unit_price_currency"] = raw_upc[:3].upper() if raw_upc else None

        d["total_item_value"] = self._get("TotalItemValue", "Total Item Value", tp=[r"Total\s*Item\s*Value\b(?!\s*\(In)\s*[:\|]?\s*([\d,\.]+)"], fn=to_decimal)
        d["total_item_value_inr"] = self._get("TotalItemValue(InINR)", "re:totalitemvalue.*inr", tp=[r"Total\s*Item\s*Value\s*\(?In\s*INR\)?\s*[:\|]?\s*([\d,\.]+)"], fn=to_decimal)
        d["total_taxable_value"] = self._get("TotalTaxableValue", "Total Taxable Value", tp=[r"Total\s*Taxable\s*Value\s*[:\|]?\s*([\d,\.]+)"], fn=to_decimal)
        d["total_igst_paid"] = self._get("TotalIGSTPaid", "Total IGST Paid", tp=[r"Total\s*IGST\s*Paid\s*[:\|]?\s*([\d,\.]+)"], fn=to_decimal)
        d["total_cess_paid"] = self._get("TotalCESSPaid", "Total CESS Paid", tp=[r"Total\s*CESS\s*Paid\s*[:\|]?\s*([\d,\.]+)"], fn=to_decimal)
        d["exchange_rate"] = self._get("ExchangeRate", "Exchange Rate", tp=[r"\bExchange\s*Rate\s*[:\|]?\s*([\d\.]+)"], fn=to_decimal)
        d["exporter_name"] = self._get("NameoftheConsignor", "Name of the Consignor", tp=[r"Name\s*of\s*(?:the\s*)?Consignor\s*[:\|]?\s*([^\n]{5,150})"])
        d["exporter_address"] = self._get("AddressoftheConsignor", "Address of the Consignor", tp=[r"Address\s*of\s*(?:the\s*)?Consignor\s*[:\|]?\s*([^\n]{10,300})"])
        d["consignee_name"] = self._get("NameoftheConsignee", "Name of the Consignee", tp=[r"Name\s*of\s*(?:the\s*)?Consignee\s*[:\|]?\s*([^\n]{2,150})"])

        ca = self._get("AddressoftheConsignee", "Address of the Consignee", tp=[r"Address\s*of\s*(?:the\s*)?Consignee\s*[:\|]?\s*([^\n]{10,300})"])
        if ca and not re.search(r"[\d,]", ca):
            ca = self._text(r"Address\s*of\s*(?:the\s*)?Consignee\s*[:\|]?\s*([^\n]{10,300})")
        d["consignee_address"] = ca

        country = None
        if ca:
            for kw, name in [
                ("AUSTRALIA", "Australia"), ("NEW ZEALAND", "New Zealand"),
                ("UNITED STATES", "United States"), ("USA", "United States"),
                ("CANADA", "Canada"), ("UNITED KINGDOM", "United Kingdom"),
                ("UK", "United Kingdom"), ("SINGAPORE", "Singapore"),
                ("UAE", "UAE"), ("GERMANY", "Germany"), ("FRANCE", "France"),
                ("CHINA", "China"), ("JAPAN", "Japan"), ("HONG KONG", "Hong Kong"),
                ("THAILAND", "Thailand"), ("MALAYSIA", "Malaysia"),
                ("NETHERLANDS", "Netherlands"), ("BELGIUM", "Belgium"),
                ("ITALY", "Italy"), ("SPAIN", "Spain"), ("SOUTH AFRICA", "South Africa"),
            ]:
                if kw in ca.upper():
                    country = name
                    break
        d["consignee_country"] = country

        d["port_of_loading"] = self._get("PortofLoading", "Port of Loading", tp=[r"Port\s*of\s*Loading\s*[:\|]?\s*([A-Z0-9]{2,10})"])
        d["port_of_discharge"] = self._get("AirportofDestination", "Airport of Destination", "PortofDischarge", "Port of Discharge", tp=[r"Airport\s*of\s*Destination\s*[:\|]?\s*([A-Z0-9]{2,10})"])
        d["custom_station"] = self._get("CustomStationName", "Custom Station Name", tp=[r"Custom\s*Station\s*Name\s*[:\|]?\s*([A-Z0-9]{2,10})"])

        pkgs = self._get("NumberofPackagesPiecesBagsULD", "re:numberofpackages", tp=[r"Number\s*of\s*Packages[^:]*[:\|]?\s*(\d+)"])
        d["total_packages"] = first_int(pkgs) if pkgs else None

        qty = self._get("Quantity", tp=[r"\bQuantity\s*[:\|]?\s*(\d+)"])
        d["quantity"] = first_int(qty) if qty else None

        d["unit_of_measure"] = self._get("UnitOfMeasure", "Unit Of Measure", tp=[r"Unit\s*Of\s*Measure\s*[:\|]?\s*([A-Z]{2,10})"])
        d["gross_weight"] = self._get("DeclaredWeight(inKgs)", "Declared Weight(in Kgs)", tp=[r"Declared\s*Weight[^:]*[:\|]?\s*([\d\.]+)"])

        hs = self._get("CTSH", tp=[r"CTSH\s*[:\|]?\s*(\d{4,10})"])
        if hs:
            m = re.search(r"(\d{4,10})", hs)
            hs = m.group(1) if m else hs
        d["hs_code"] = hs

        d["item_description"] = self._get("GoodsDescription", "Goods Description", tp=[r"Goods\s*Description\s*[:\|]?\s*([^\n]{3,200})"])

        sku = self._get("(ii)SKUNO", "SKUNO", "SKU", tp=[r"SKU\s*(?:NO|Number)?\s*[:\|]?\s*([A-Za-z0-9][A-Za-z0-9\-/]{1,30})"])
        if sku and re.fullmatch(r"(YES|NO|Y|N|NA)", sku, re.IGNORECASE):
            sku = None
        d["sku"] = sku

        d["iec_code"] = self._get("ImportExportCode(IEC)", "re:importexportcode", tp=[r"Import\s*Export\s*Code\s*\(?IEC\)?\s*[:\|]?\s*([A-Z0-9]{10})"])
        d["iec_branch_code"] = self._get("IECBranchCode", "IEC Branch Code", tp=[r"IEC\s*Branch\s*Code\s*[:\|]?\s*(\d+)"])
        d["ad_code"] = self._get("ADCode", "AD Code", tp=[r"AD\s*Code\s*[:\|]?\s*(\d{5,10})"])
        d["account_no"] = self._get("AccountNo", "Account No", tp=[r"Account\s*No\s*[:\|]?\s*(\d{8,18})"])
        d["gstin"] = self._get("KYCID", "KYC ID", "GSTIN", tp=[r"KYC\s*ID\s*[:\|]?\s*([A-Z0-9]{15})", r"GSTIN\s*[:\|]?\s*([A-Z0-9]{15})"])
        d["kyc_document"] = self._get("KYCDocument", "KYC Document", tp=[r"KYC\s*Document\s*[:\|]?\s*([^\n]{3,50})"])
        d["state_code"] = self._get("StateCode", "State Code", tp=[r"State\s*Code\s*[:\|]?\s*(\d{1,2})"])
        d["mhbs_no"] = self._get("MHBSNo", "MHBS No", tp=[r"MHBS\s*No\s*[:\|]?\s*([A-Z0-9\-]+)"])

        egm = self._get("EGMNumber", "EGM Number", tp=[r"EGM\s*Number\s*[:\|]?\s*(\d{5,12})"])
        if egm and not re.fullmatch(r"\d+", egm):
            egm = self._text(r"EGM\s*Number\s*[:\|]?\s*(\d{5,12})")
        d["egm_number"] = egm

        d["crn_number"] = d.get("hawb_number")
        arr_all = self._text_all(r"\b(ARR-\d+)\b")
        d["crn_mhbs_numbers"] = arr_all if arr_all else None

        d["status"] = self._get("Status", tp=[r"\bStatus\s*[:\|]?\s*(EXPCLOSED|EXPOPEN|[A-Z]{4,12})"])
        d["under_meis_scheme"] = self._get("UnderMEISScheme", "Under MEIS Scheme", tp=[r"Under\s*MEIS\s*Scheme\s*[:\|]?\s*([A-Z]+)"])
        d["nfei"] = self._get("NFEI", tp=[r"\bNFEI\s*[:\|]?\s*([A-Z]+)"])
        d["government_nongovernment"] = self._get("GovernmentNonGovernment", "re:governmentnongovernment", tp=[r"(?:Government/Non-Government)\s*[:\|]?\s*(NON-GOVERNMENT|GOVERNMENT)"])
        d["export_using_ecommerce"] = self._get("ExportUsinge-Commerce", "Export Using e-Commerce", tp=[r"Export\s*Using\s*e.Commerce\s*[:\|]?\s*([YN])"])
        d["bond_or_ut"] = self._get("BONDORUT", "BOND OR UT", tp=[r"BOND\s*OR\s*UT\s*[:\|]?\s*([A-Z]+)"])
        d["courier_name"] = self._get("CourierName", "Courier Name", tp=[r"Courier\s*Name\s*[:\|]?\s*([^\n]{3,80})"])
        d["courier_reg_no"] = self._get("CourierRegistrationNumber", "re:courierregistrationnumber", tp=[r"Courier\s*Registration\s*Num[^\s:]*\s*[:\|]?\s*([A-Z0-9]+)"])
        d["airline"] = self._get("Airlines", "Airline", tp=[r"Airlines?\s*[:\|]?\s*([A-Z][A-Z\s]+?)(?=\s+Flight|\s+Port|\n)"])
        d["flight_number"] = self._get("FlightNumber", "Flight Number", tp=[r"Flight\s*Number\s*[:\|]?\s*([A-Z0-9\s]{2,12})"])

        lower = self.full_text.lower()
        if any(k in lower for k in ("flight", "airline", "airport", "hawb")):
            d["mode_of_transport"] = "AIR"
        elif any(k in lower for k in ("vessel", "ship", "sea", "bill of lading")):
            d["mode_of_transport"] = "SEA"

        return d


def _parse_pdf_sync(pdf_bytes: bytes) -> Dict[str, Any]:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        return ExtractionEngine(pdf).extract()


async def _parse_pdf_async(pdf_bytes: bytes) -> Dict[str, Any]:
    return await asyncio.to_thread(_parse_pdf_sync, pdf_bytes)


@app.get("/")
async def root():
    return {"service": "pdfduck API", "version": "5.0.0",
            "endpoints": ["/extract", "/extract/batch", "/health"]}


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.post("/extract")
async def extract_pdf(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="File must be a PDF.")
    pdf_bytes = await file.read()
    if not pdf_bytes:
        raise HTTPException(status_code=400, detail="Empty file.")
    try:
        raw = await _parse_pdf_async(pdf_bytes)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Extraction failed: {exc}") from exc
    data = {k: v for k, v in raw.items() if v is not None}
    return JSONResponse({"success": True, "rows": 1, "data": [data]})


@app.post("/extract/batch")
async def extract_batch(files: List[UploadFile] = File(...)):
    if len(files) > 50:
        raise HTTPException(status_code=400, detail="Max 50 files per batch.")

    async def _process(f: UploadFile) -> Dict[str, Any]:
        if not f.filename.lower().endswith(".pdf"):
            return {"file": f.filename, "error": "Not a PDF"}
        pdf_bytes = await f.read()
        try:
            raw = await _parse_pdf_async(pdf_bytes)
            return {"file": f.filename, "success": True,
                    "data": {k: v for k, v in raw.items() if v is not None}}
        except Exception as exc:
            return {"file": f.filename, "success": False, "error": str(exc)}

    results = await asyncio.gather(*[_process(f) for f in files])
    return JSONResponse({"success": True, "rows": len(results), "data": list(results)})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, workers=4)