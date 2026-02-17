# pdfduck backend

FastAPI backend for PDF data extraction using pdfplumber.

---

### 3. configure frontend

paste the backend URL into the frontend settings panel:

```
https://backendurl.com
```

save and test. if the dot turns green, you're good.

---

## local development

```bash
# install dependencies
pip install -r requirements.txt

# run server
uvicorn main:app --reload

# test
curl -X POST http://localhost:8000/extract \
  -F "file=@test.pdf"
```

---

## endpoints

**GET /** — api info

**GET /health** — health check

**POST /extract** — extract data from PDF
- body: multipart form with `file` field
- returns: JSON with extracted rows

---

## how it works

1. **pdfplumber** extracts tables from PDFs
2. first row of each table becomes column headers
3. all data rows are returned as JSON objects
4. if no tables found, falls back to text pattern matching for common fields (invoice number, date, amounts, etc.)

---

## files

```
main.py              — FastAPI application
requirements.txt     — Python dependencies
Procfile             — Railway start command
railway.json         — Railway config
.gitignore          — Git ignore rules
```
