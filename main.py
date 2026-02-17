from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pdfplumber
import io
from typing import List, Dict, Any
import re

app = FastAPI(title="pdfduck API", version="1.0.0")

# CORS - allow Cloudflare Pages frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with your Cloudflare Pages URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def clean_text(text: str) -> str:
    """Clean extracted text - remove extra whitespace, normalize"""
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def extract_tables_from_pdf(pdf_bytes: bytes) -> List[Dict[str, Any]]:
    """
    Extract all tables from PDF using pdfplumber.
    Returns list of dicts where each dict is a row with column headers as keys.
    """
    all_rows = []
    
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page_num, page in enumerate(pdf.pages, 1):
            tables = page.extract_tables()
            
            for table_idx, table in enumerate(tables):
                if not table or len(table) < 2:
                    continue
                
                # First row is assumed to be headers
                headers = [clean_text(str(cell)) if cell else f"col_{i}" for i, cell in enumerate(table[0])]
                
                # Make headers unique
                seen = {}
                unique_headers = []
                for h in headers:
                    if h in seen:
                        seen[h] += 1
                        unique_headers.append(f"{h}_{seen[h]}")
                    else:
                        seen[h] = 0
                        unique_headers.append(h)
                
                # Extract data rows
                for row_idx, row in enumerate(table[1:], 1):
                    if not any(row):  # skip empty rows
                        continue
                    
                    row_dict = {
                        "_source_page": page_num,
                        "_source_table": table_idx + 1,
                        "_source_row": row_idx
                    }
                    
                    for header, cell in zip(unique_headers, row):
                        row_dict[header] = clean_text(str(cell)) if cell else None
                    
                    all_rows.append(row_dict)
    
    return all_rows

def extract_text_with_structure(pdf_bytes: bytes) -> Dict[str, Any]:
    """
    Extract structured text from PDF when no clear tables exist.
    Attempts to find key-value pairs using common patterns.
    Falls back to line-by-line extraction for general documents.
    """
    structured_data = {}
    
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        full_text = ""
        for page in pdf.pages:
            full_text += page.extract_text() or ""
        
        # Common patterns in shipping bills, invoices, resumes, etc.
        patterns = {
            "invoice_number": r"(?:invoice|bill|ref)(?:\s+no\.?|#|number)?:?\s*([A-Z0-9\-/]+)",
            "date": r"(?:date|dated|invoice date|bill date):?\s*(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})",
            "total_amount": r"(?:total|amount|grand total|net amount):?\s*(?:rs\.?|inr|usd|\$)?\s*([\d,]+\.?\d*)",
            "consignee": r"(?:consignee|ship to|deliver to):?\s*([^\n]{5,80})",
            "shipper": r"(?:shipper|exporter|from):?\s*([^\n]{5,80})",
            "email": r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})",
            "phone": r"(?:phone|mobile|tel|contact)(?:\s*:)?\s*([\d\s\+\-\(\)]{10,20})",
            "name": r"^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)",  # First line capitalized name
        }
        
        for field, pattern in patterns.items():
            match = re.search(pattern, full_text, re.IGNORECASE | re.MULTILINE)
            if match:
                structured_data[field] = clean_text(match.group(1))
        
        # If very little extracted, do line-by-line split as fallback
        if len(structured_data) < 2:
            lines = [clean_text(line) for line in full_text.split('\n') if clean_text(line)]
            
            # Extract first 20 meaningful lines as separate fields
            for idx, line in enumerate(lines[:20], 1):
                if line and len(line) > 2:
                    structured_data[f"line_{idx}"] = line
        
        structured_data["_full_text"] = full_text
        structured_data["_text_length"] = len(full_text)
    
    return structured_data

@app.get("/")
async def root():
    return {
        "service": "pdfduck API",
        "version": "1.0.0",
        "endpoints": {
            "/extract": "POST - Extract data from PDF",
            "/health": "GET - Health check"
        }
    }

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.post("/extract")
async def extract_pdf(file: UploadFile = File(...)):
    """
    Extract structured data from uploaded PDF.
    
    Returns:
    - JSON array of row objects if tables are found
    - JSON object with key-value pairs if no tables but structured text found
    - Error if PDF cannot be processed
    """
    
    if not file.filename.endswith('.pdf'):
        raise HTTPException(status_code=400, detail="File must be a PDF")
    
    try:
        pdf_bytes = await file.read()
        
        # Try table extraction first
        rows = extract_tables_from_pdf(pdf_bytes)
        
        if rows:
            return JSONResponse({
                "success": True,
                "method": "table_extraction",
                "rows": len(rows),
                "data": rows
            })
        
        # Fallback to text extraction with pattern matching
        structured = extract_text_with_structure(pdf_bytes)
        
        if structured and len(structured) > 0:  # return anything found, even if minimal
            return JSONResponse({
                "success": True,
                "method": "text_extraction",
                "rows": 1,
                "data": [structured]
            })
        
        # Truly empty PDF
        return JSONResponse({
            "success": False,
            "error": "PDF appears to be empty or unreadable",
            "suggestion": "Check if PDF contains selectable text"
        }, status_code=422)
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"PDF processing failed: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)