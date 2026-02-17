# pdfduck backend

FastAPI backend for PDF data extraction using pdfplumber.

---

## deploy to railway

### 1. push to github

```bash
cd backend
git init
git add .
git commit -m "initial commit"
git remote add origin https://github.com/yourusername/pdfduck-backend.git
git push -u origin main
```

### 2. deploy on railway

1. go to [railway.app](https://railway.app)
2. click **"New Project"** → **"Deploy from GitHub repo"**
3. select your backend repo
4. railway auto-detects Python and deploys
5. once deployed, go to **Settings** → **Networking** → **Generate Domain**
6. copy the domain (e.g., `pdfshift-backend-production.up.railway.app`)

### 3. configure frontend

paste the railway backend URL into the frontend settings panel:

```
https://pdfshift-backend-production.up.railway.app
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

---

## environment variables

none required — works out of the box.

---

## scaling

railway free tier:
- 500 hours/month
- $5 credit
- enough for internal tooling

if you need more:
- upgrade to hobby plan ($5/month)
- or use railway's usage-based pricing