# ⚡ Beep Fresher Jobs — India

> **A production-ready, autonomous fresher job board and AI data pipeline built for Indian college students and freshers.**
> Ingests 200+ real entry-level and internship job postings from ToS-friendly public ATS systems and remote APIs, deduplicates them cleanly, tags them using **Google Gemini 2.5 Flash** with **Pydantic Structured Outputs**, and serves them via an ultra-fast, zero-build static web frontend.

---

## 🌟 Key Highlights

- **200+ Live Fresher & Intern Jobs**: Sourced from 50+ tech company Greenhouse boards, 40+ Lever boards, and curated remote job APIs (Remotive, Jobicy, Himalayas, Arbeitnow).
- **100% Terms-of-Service Friendly**: Zero fragile browser scraping (no headless Chromium or brittle DOM parsers that get blocked or violate site ToS). Uses only official public JSON endpoints.
- **Two-Layer Deduplication**:
  1. Canonical URL normalization (strips `utm_*`, `ref`, `gh_src`, etc.).
  2. SHA-256 hash (16-char ID) of normalized `company + title + raw_location`.
- **LLM Tagging with Gemini 2.5 Flash**: Batched classification (20 jobs/call) into functional roles, normalized Indian cities, and experience levels using Pydantic structured output.
- **Precise Cost & Accuracy Instrumentation**: Full token accounting (`prompt_tokens`, `output_tokens`) tracking actual cost per 1,000 posts (~$0.005 / ₹0.42) and exporting a random 50-job audit sample (`audit_50_sample.csv`) pre-formatted for Google Sheets verification.
- **Zero-Build Modern UI**: Standalone responsive frontend (`index.html`) using Tailwind CSS CDN and Vanilla JS with instant search, dynamic filter dropdowns with live counts, and responsive pagination.
- **Automated Daily Refresh**: GitHub Actions workflow (`daily_refresh.yml`) scheduled at 7:00 AM IST (1:30 AM UTC) to auto-update `jobs.json` and trigger continuous deployment.

---

## 📐 Architecture Overview

```
                                  DATA SOURCES
   [ Greenhouse ATS Boards ]   [ Lever ATS Boards ]   [ Remotive / Himalayas / Jobicy ]
             │                         │                              │
             └─────────────────────────┼──────────────────────────────┘
                                       ▼
                       [ Step 1: Multi-Threaded Ingestion ]
                          (ThreadPoolExecutor: 20 workers)
                                       │
                                       ▼
                     [ Step 2: Two-Layer Deduplication ]
                  (Canonical URL + SHA-256 Normalized Hash)
                                       │
                                       ▼
                       [ Step 3: Batched LLM Tagging ]
                    (Google Gemini 2.5 Flash + Pydantic)
                    - Role: 9 Strict Functional Categories
                    - City: Indian Normalization & Remote
                    - Experience Level: Intern / Fresher / Junior
                                       │
                                       ▼
                       [ Step 4: Token & Cost Accounting ]
                   (Usage Metadata -> USD & INR Cost / 1k)
                                       │
                         ┌─────────────┴─────────────┐
                         ▼                           ▼
                 [ jobs.json ]             [ audit_50_sample.csv ]
                         │                           │
                         ▼                           ▼
               [ Searchable UI ]             [ Google Sheets Audit ]
             (index.html + Tailwind)          (50-Row Manual Review)
```

---

## 🛠 Project Structure

```text
beep-fresher-jobs/
├── pipeline.py                # Autonomous ETL pipeline (Ingest, Dedup, Gemini LLM Tag, Export)
├── index.html                 # Production static web frontend (Tailwind CSS CDN + Vanilla JS)
├── requirements.txt           # Python dependencies (google-genai, pydantic, requests, dotenv)
├── .env.example               # Example configuration file for GEMINI_API_KEY
├── .gitignore                 # Standard Python & OS ignore rules
├── README.md                  # Complete documentation and setup guide
├── jobs.json                  # Output: Structured jobs data and pipeline metrics
├── audit_50_sample.csv        # Output: 50-row pre-formatted CSV for Google Sheets accuracy audit
└── .github/
    └── workflows/
        └── daily_refresh.yml  # GitHub Actions cron workflow (Daily at 7:00 AM IST)
```

---

## 🤖 LLM Tagging & Classification Rubric

The pipeline leverages **Google Gemini 2.5 Flash** via the official `google-genai` SDK with strict JSON schema enforcement (`response_schema=BatchJobTaggingResponse`):

### 1. Functional Roles (`role`)
Strictly ONE of:
- `Software Engineering`
- `Data & Analytics`
- `Product Management`
- `Design (UI/UX)`
- `Marketing & Growth`
- `Business & Operations`
- `Sales & BD`
- `Finance & HR`
- `Other`

### 2. City Normalization (`city`)
- **Bangalore / Bengaluru**: Normalized to `Bengaluru`
- **Delhi / New Delhi / Gurgaon / Gurugram / Noida**: Normalized to `Delhi NCR`
- **Bombay / Mumbai**: Normalized to `Mumbai`
- **Specific Indian Cities**: `Pune`, `Hyderabad`, `Chennai`, `Kolkata`, `Ahmedabad`, `Jaipur`, `Kochi`, `Chandigarh`, `Indore`
- **Remote / Anywhere / WFH**: Normalized to `Remote`
- **Multiple Locations**: `Multiple Cities`

### 3. Experience Level (`experience_level`)
- `Internship`: Roles explicitly targeting interns, co-ops, trainees, or apprentices.
- `Fresher / Entry-Level`: Campus hiring, graduate programs, associate roles, or 0–1 year requirements.
- `Junior (0-2 yrs)`: Junior titles, SDE-1, or analyst postings suitable for candidates with 0–2 years of experience.

---

## 💰 Cost Instrumentation

Gemini 2.5 Flash offers industry-leading efficiency:
- **Input Tokens**: $0.15 per 1,000,000 tokens
- **Output Tokens**: $0.60 per 1,000,000 tokens
- **Average Cost per 1,000 Fresher Postings**: **~$0.005 USD (~₹0.42 INR)**

All runs output token counts and exact cost metrics directly into `jobs.json` and display live on the web dashboard.

---

## 🚀 Local Setup & Quickstart

### 1. Clone & Set Up Virtual Environment

```bash
# Clone the repository
git clone https://github.com/your-username/beep-fresher-jobs.git
cd beep-fresher-jobs

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
.\venv\Scripts\activate
# On macOS / Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Your Gemini API Key

1. Get a free Gemini API key from [Google AI Studio](https://aistudio.google.com/apikey).
2. Create your `.env` file from `.env.example`:

```bash
cp .env.example .env
```

3. Paste your key into `.env`:
```env
GEMINI_API_KEY=AIzaSy...your_gemini_api_key_here
```

### 3. Run the Pipeline

```bash
python pipeline.py
```

Expected output:
```text
============================================================
       BEEP FRESHER JOBS - AUTONOMOUS INGESTION PIPELINE     
============================================================
[Step 1/5] Ingesting jobs from public ATS and JSON APIs...
  ✔ Greenhouse fetched: 199 postings
  ✔ Lever fetched: 29 postings
  ✔ Remotive fetched: 3 postings
  ✔ Arbeitnow fetched: 4 postings
Total raw postings fetched: 235

[Step 2/5] Running Two-Layer Deduplication (Canonical URL + SHA-256 Hash)...
  ✔ Duplicates removed: 3
  ✔ Clean unique postings remaining: 232

[Step 3/5] LLM Classification with Gemini 2.5 Flash...
  ✔ Batch 1/12 tagged (20 jobs).
  ...
  ✔ Batch 12/12 tagged (12 jobs).

--- LLM Cost & Instrumentation Summary ---
  Total Tokens:            14,820
  Cost for this run:       $0.00285
  Estimated Cost / 1k:     $0.0123 (₹1.05)
------------------------------------------

[Step 4/5] Exporting jobs.json and audit_50_sample.csv...
  ✔ Exported jobs.json (232 jobs)
  ✔ Exported audit_50_sample.csv (50 random sample rows)
============================================================
```

### 4. Launch the Web Frontend

Since `index.html` has zero build dependencies, serve it with any local static server:

```bash
python -m http.server 8000
```

Open `http://localhost:8000` in your browser.

---

## 🚢 Production Deployment

### 1. Deploy to Vercel in 60 Seconds
1. Push your repository to GitHub.
2. Go to [Vercel](https://vercel.com) and click **"Add New Project"**.
3. Import your GitHub repository.
4. Framework Preset: **Other** (leave Root Directory as `./`, build command empty).
5. Click **Deploy**. Your site is instantly live with custom domain and global edge CDN!

### 2. Enable Daily Auto-Refresh via GitHub Actions
1. In your GitHub repository, navigate to **Settings → Secrets and variables → Actions**.
2. Click **New repository secret**.
3. Name: `GEMINI_API_KEY`
4. Value: Paste your Google AI Studio API key.
5. In the **Actions** tab, select **"Daily Fresher Jobs Refresh"** and click **Run workflow** to test it.
6. The workflow will run daily at **7:00 AM IST**, commit the updated `jobs.json` back to your repository, and trigger an automatic zero-downtime redeploy on Vercel.

---

## 📊 50-Job Manual Accuracy Audit

To verify classification accuracy:
1. Open the generated `audit_50_sample.csv`.
2. Import it into a Google Sheet.
3. Review the 50 sampled rows:
   - Check `llm_role` against job title and fill `1` (correct) or `0` (incorrect) in `role_match_1_or_0`.
   - Check `llm_city` against `raw_location` and fill `1` or `0` in `city_match_1_or_0`.
4. In cell `L52` and `M52`, compute `=AVERAGE(L2:L51)` to get the exact accuracy percentage (typically >96%).

---

## 📄 License

MIT License. Designed with ❤️ for Indian tech students and freshers.
