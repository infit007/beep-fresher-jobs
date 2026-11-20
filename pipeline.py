#!/usr/bin/env python3
"""
Beep Fresher Jobs - Ingestion, Deduplication, LLM Tagging & Export Pipeline
Author: Antigravity
Description:
    Autonomous pipeline that ingests fresher and internship postings from
    public ATS job boards (Greenhouse, Lever) and public remote APIs (Remotive,
    Jobicy, Himalayas, Arbeitnow), cleanly deduplicates them, classifies them
    using Google Gemini 2.5 Flash with Pydantic Structured Outputs, tracks token
    costs, and exports jobs.json and audit_50_sample.csv.
"""

import os
import sys
import json
import csv
import re
import random
import time
import hashlib
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, parse_qs, urlunparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Literal

import requests
from dotenv import load_dotenv
from pydantic import BaseModel, Field

# Load environment variables from .env if present
load_dotenv()

# --- Config & Constants ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()

# Gemini Pricing (USD per 1M tokens) - gemini-2.5-flash
INPUT_COST_PER_1M = 0.15
OUTPUT_COST_PER_1M = 0.60
USD_TO_INR = 85.0
BATCH_SIZE = 20

# Public ATS Sources
GREENHOUSE_COMPANIES = [
    "rubrik", "slice", "databricks", "stripe", "groww", "druva", "coinbase",
    "inmobi", "datadog", "thoughtworks", "cloudflare", "elastic", "mongodb",
    "canonical", "affirm", "okta", "airtable", "samsara", "duolingo",
    "pagerduty", "checkr", "twilio", "gitlab", "flexport", "branch",
    "gusto", "brex", "ramp", "ripple", "instacart", "pinterest",
    "figma", "roblox", "instawork", "porter", "razorpay", "browserstack",
    "postman", "chargebee", "freshworks", "clevertap", "moengage",
    "darwinbox", "phonepe", "swiggy", "zepto", "zomato", "juspay",
    "setu", "hasura", "atlan", "sprinklr", "uber", "atlassian",
    "airtel", "urbancompany", "cars24", "spinny", "udaan", "lenskart",
    "shadowfax", "delhivery", "innovaccer", "harness", "confluent",
    "snowflake", "nutanix", "hashicorp", "box"
]

LEVER_COMPANIES = [
    "cred", "meesho", "groww", "wealthsimple", "paytm", "plaid", "revolut",
    "n26", "zoox", "upstox", "khatabook", "dunzo", "urbancompany", "cars24",
    "spinny", "kula", "famapp", "fampay", "jupiter", "mpl", "games24x7",
    "navi", "coindcx", "coinswitch", "palantir", "canva", "notion",
    "miro", "sourcegraph", "airbyte", "dbtlabs", "posthog", "anthropic",
    "openai", "superhuman", "scale", "together", "mistral", "perplexity"
]

ASHBY_COMPANIES = [
    "sentry", "resend", "linear", "perplexity", "cursor", "replit",
    "posthog", "langchain", "modal", "anyscale", "ramp", "supabase",
    "cohere", "llamaindex", "render", "railway", "clerk", "retool", "vanta"
]

INDIA_CITIES = [
    "india", "bengaluru", "bangalore", "mumbai", "bombay", "pune",
    "delhi", "new delhi", "noida", "gurgaon", "gurugram", "hyderabad",
    "chennai", "kolkata", "ahmedabad", "jaipur", "kochi", "cochin",
    "chandigarh", "indore", "coimbatore", "bhubaneswar", "trivandrum",
    "thiruvananthapuram", "mysore", "mysuru", "karnataka", "maharashtra",
    "telangana", "tamil nadu", "uttar pradesh", "haryana", "rajasthan",
    "gujarat", "kerala", "bihar", "west bengal", "punjab", "andhra pradesh",
    "madhya pradesh", "odisha", "patna", "vizag", "visakhapatnam", "surat",
    "vadodara", "nagpur", "nashik", "bhopal", "ludhiana", "guntur", "vijayawada"
]

EXCLUDE_TITLE_PATTERNS = [
    r"\bsenior\b", r"\bsr\.?\b", r"\blead\b", r"\bprincipal\b", r"\bstaff\b",
    r"\bdirector\b", r"\bvp\b", r"\bvice president\b", r"\bhead\b", r"\bmanager\b",
    r"\barchitect\b", r"\bchief\b", r"\bdistinguished\b", r"\bexpert\b",
    r"\bii\b", r"\biii\b", r"\biv\b", r"\bv\b",
    r"\bsde[-\s]?2\b", r"\bsde[-\s]?3\b", r"\bsde[-\s]?ii\b", r"\bsde[-\s]?iii\b",
    r"\bmid[-\s]?level\b", r"\bexperienced\b", r"\bexec\b", r"\bpartner\b"
]

FRESHER_TITLE_PATTERNS = [
    r"\bintern\b", r"\binternship\b", r"\bfresher\b", r"\bentry[-\s]?level\b",
    r"\bgraduate\b", r"\btrainee\b", r"\bapprentice\b", r"\bjunior\b", r"\bjr\.?\b",
    r"\bassociate\b", r"\bsde[-\s]?1\b", r"\bsde[-\s]?i\b", r"\bsoftware engineer[-\s]?1\b",
    r"\bsoftware engineer[-\s]?i\b", r"\banalyst\b", r"\bcoordinator\b", r"\bassistant\b",
    r"\b0[-–]1\b", r"\b0[-–]2\b", r"\bcampus\b", r"\bfellow\b", r"\bdevelopment representative\b",
    r"\bbdr\b", r"\bsdr\b", r"\bsupport engineer\b", r"\bqa engineer\b", r"\btest engineer\b",
    r"\bfrontend developer\b", r"\bbackend developer\b", r"\bsoftware engineer\b",
    r"\bengineer\b", r"\bdeveloper\b", r"\bdesigner\b", r"\bdata scientist\b",
    r"\bdata analyst\b", r"\bproduct specialist\b", r"\boperations executive\b",
    r"\bconsultant\b", r"\bresearcher\b", r"\bwriter\b", r"\bcreator\b"
]


# --- Pydantic Schemas for Gemini Structured Output ---
RoleType = Literal[
    "Software Engineering",
    "Data & Analytics",
    "Product Management",
    "Design (UI/UX)",
    "Marketing & Growth",
    "Business & Operations",
    "Sales & BD",
    "Finance & HR",
    "Other"
]

ExpLevelType = Literal[
    "Internship",
    "Fresher / Entry-Level",
    "Junior (0-2 yrs)"
]

class SingleJobTag(BaseModel):
    id: str = Field(description="The exact id of the job being classified")
    role: RoleType = Field(description="Strictly one of the allowed job role categories")
    city: str = Field(description="Normalized Indian city name (e.g., Bengaluru, Delhi NCR, Mumbai, Pune, Hyderabad, Chennai, Kolkata), 'Remote', 'Multiple Cities', or 'Other'")
    experience_level: ExpLevelType = Field(description="Experience level: Internship, Fresher / Entry-Level, or Junior (0-2 yrs)")

class BatchJobTaggingResponse(BaseModel):
    tagged_jobs: List[SingleJobTag]


# --- Utility Functions ---
NON_INDIA_COUNTRIES = [
    "usa", "united states", "us", "u.s.", "canada", "brazil", "germany",
    "uk", "united kingdom", "great britain", "england", "france", "spain",
    "australia", "japan", "singapore", "ireland", "poland", "netherlands",
    "mexico", "philippines", "romania", "south korea", "italy", "sweden",
    "switzerland", "colombia", "argentina", "chile", "peru", "emea",
    "latam", "north america", "europe"
]

def is_valid_location(loc: str) -> bool:
    if not loc:
        return False
    loc_lower = loc.lower()

    # 1. Check if explicitly India or an Indian city
    if any(city in loc_lower for city in INDIA_CITIES):
        return True

    # 2. Check if worldwide / global / anywhere
    if any(w in loc_lower for w in ["worldwide", "anywhere", "global", "home based - worldwide"]):
        return True

    # 3. If remote / wfh, ensure it is NOT bound to a foreign country
    if any(r in loc_lower for r in ["remote", "wfh", "work from home", "home based"]):
        for country in NON_INDIA_COUNTRIES:
            pattern = rf"(^|[,\-\(\s/]){re.escape(country)}($|[,\-\)\s/])"
            if re.search(pattern, loc_lower):
                return False
        return True

    return False

def is_excluded_title(title: str) -> bool:
    title_lower = title.lower()
    return any(re.search(pat, title_lower) for pat in EXCLUDE_TITLE_PATTERNS)

def is_fresher_title(title: str) -> bool:
    title_lower = title.lower()
    if is_excluded_title(title):
        return False
    return any(re.search(pat, title_lower) for pat in FRESHER_TITLE_PATTERNS)

def canonicalize_url(url: str) -> str:
    """Strip tracking and session query parameters from URLs."""
    try:
        parsed = urlparse(url)
        clean_query = []
        if parsed.query:
            q_dict = parse_qs(parsed.query)
            keep = {
                k: v for k, v in q_dict.items()
                if not k.lower().startswith("utm_") and k.lower() not in ["ref", "source", "gh_src", "lever-origin"]
            }
            clean_query = [(k, item) for k, items in keep.items() for item in items]
        new_q = "&".join(f"{k}={v}" for k, v in clean_query)
        clean_path = parsed.path.rstrip("/")
        return urlunparse((parsed.scheme, parsed.netloc, clean_path, parsed.params, new_q, ""))
    except Exception:
        return url.strip()

def compute_hash_id(company: str, title: str, raw_location: str) -> str:
    """Generate normalized SHA-256 (16-char) hash ID."""
    norm_c = re.sub(r"[^\w\s]", "", company.lower()).strip()
    norm_t = re.sub(r"[^\w\s]", "", title.lower()).strip()
    norm_l = re.sub(r"[^\w\s]", "", raw_location.lower()).strip()
    key = f"{norm_c}|{norm_t}|{norm_l}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


# --- Ingestion Fetchers ---
def fetch_greenhouse_board(company_slug: str) -> List[Dict[str, Any]]:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BeepFresherJobs/1.0"}
    url = f"https://boards-api.greenhouse.io/v1/boards/{company_slug}/jobs"
    results = []
    try:
        r = requests.get(url, headers=headers, timeout=8)
        if r.status_code == 200:
            data = r.json()
            jobs = data.get("jobs", [])
            for j in jobs:
                loc = (j.get("location") or {}).get("name", "")
                title = j.get("title", "")
                if is_valid_location(loc) and is_fresher_title(title):
                    results.append({
                        "title": title.strip(),
                        "company": company_slug.capitalize(),
                        "raw_location": loc.strip() or "India",
                        "url": j.get("absolute_url", "").strip(),
                        "source": f"Greenhouse ({company_slug})"
                    })
    except Exception:
        pass
    return results

def fetch_lever_board(company_slug: str) -> List[Dict[str, Any]]:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BeepFresherJobs/1.0"}
    url = f"https://api.lever.co/v0/postings/{company_slug}?mode=json"
    results = []
    try:
        r = requests.get(url, headers=headers, timeout=8)
        if r.status_code == 200:
            postings = r.json()
            if isinstance(postings, list):
                for p in postings:
                    cats = p.get("categories", {})
                    loc = cats.get("location", "")
                    title = p.get("text", "")
                    if is_valid_location(loc) and is_fresher_title(title):
                        results.append({
                            "title": title.strip(),
                            "company": company_slug.capitalize(),
                            "raw_location": loc.strip() or "India",
                            "url": (p.get("hostedUrl", "") or p.get("applyUrl", "")).strip(),
                            "source": f"Lever ({company_slug})"
                        })
    except Exception:
        pass
    return results

def fetch_ashby_board(company_slug: str) -> List[Dict[str, Any]]:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BeepFresherJobs/1.0"}
    url = f"https://api.ashbyhq.com/posting-api/job-board/{company_slug}"
    results = []
    try:
        r = requests.get(url, headers=headers, timeout=8)
        if r.status_code == 200:
            data = r.json()
            jobs = data.get("jobs", [])
            for j in jobs:
                loc = j.get("location", "")
                sec_locs = j.get("secondaryLocations", [])
                all_loc_str = loc + " " + " ".join(sec_locs) if sec_locs else loc
                title = j.get("title", "")
                if is_valid_location(all_loc_str) and is_fresher_title(title):
                    results.append({
                        "title": title.strip(),
                        "company": company_slug.capitalize(),
                        "raw_location": loc.strip() or "Remote",
                        "url": (j.get("jobUrl") or j.get("applyUrl") or "").strip(),
                        "source": f"Ashby ({company_slug})"
                    })
    except Exception:
        pass
    return results

def fetch_remotive_jobs() -> List[Dict[str, Any]]:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BeepFresherJobs/1.0"}
    url = "https://remotive.com/api/remote-jobs"
    results = []
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json()
            for j in data.get("jobs", []):
                loc = j.get("candidate_required_location", "")
                title = j.get("title", "")
                if is_valid_location(loc) and is_fresher_title(title):
                    results.append({
                        "title": title.strip(),
                        "company": j.get("company_name", "").strip(),
                        "raw_location": loc.strip() or "Remote",
                        "url": j.get("url", "").strip(),
                        "source": "Remotive"
                    })
    except Exception as e:
        print(f"  [Warn] Remotive fetch error: {e}", flush=True)
    return results

def fetch_jobicy_jobs() -> List[Dict[str, Any]]:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BeepFresherJobs/1.0"}
    url = "https://jobicy.com/api/v2/remote-jobs?count=100"
    results = []
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json()
            for j in data.get("jobs", []):
                loc = j.get("jobGeo", "")
                title = j.get("jobTitle", "")
                level = j.get("jobLevel", "")
                is_fresh = is_fresher_title(title) or ("entry" in level.lower()) or ("junior" in level.lower())
                if is_valid_location(loc) and is_fresh and not is_excluded_title(title):
                    results.append({
                        "title": title.strip(),
                        "company": j.get("companyName", "").strip(),
                        "raw_location": loc.strip() or "Remote",
                        "url": j.get("url", "").strip(),
                        "source": "Jobicy"
                    })
    except Exception as e:
        print(f"  [Warn] Jobicy fetch error: {e}", flush=True)
    return results

def fetch_himalayas_jobs(limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BeepFresherJobs/1.0"}
    url = f"https://himalayas.app/jobs/api?limit={limit}&offset={offset}"
    results = []
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json()
            for j in data.get("jobs", []):
                loc_list = j.get("locationRestrictions", [])
                loc_str = ", ".join(loc_list) if loc_list else "Worldwide"
                title = j.get("title", "")
                seniority = j.get("seniority", [])
                sen_str = " ".join(seniority).lower()
                is_fresh = is_fresher_title(title) or any(s in sen_str for s in ["entry", "junior", "intern"])
                if is_valid_location(loc_str) and is_fresh and not is_excluded_title(title):
                    results.append({
                        "title": title.strip(),
                        "company": j.get("companyName", "").strip(),
                        "raw_location": loc_str.strip(),
                        "url": j.get("applicationLink", "").strip(),
                        "source": "Himalayas"
                    })
    except Exception as e:
        print(f"  [Warn] Himalayas fetch error: {e}", flush=True)
    return results

def fetch_arbeitnow_jobs(page: int = 1) -> List[Dict[str, Any]]:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BeepFresherJobs/1.0"}
    url = f"https://www.arbeitnow.com/api/job-board-api?page={page}"
    results = []
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json()
            for j in data.get("data", []):
                loc = j.get("location", "")
                title = j.get("title", "")
                remote = j.get("remote", False)
                if remote:
                    loc = f"{loc} (Remote)" if loc else "Remote"
                if is_valid_location(loc) and is_fresher_title(title):
                    results.append({
                        "title": title.strip(),
                        "company": j.get("company_name", "").strip(),
                        "raw_location": loc.strip(),
                        "url": j.get("url", "").strip(),
                        "source": "Arbeitnow"
                    })
    except Exception as e:
        print(f"  [Warn] Arbeitnow fetch error: {e}", flush=True)
    return results


# --- Orchestrated Ingestion ---
def ingest_all_sources() -> (List[Dict[str, Any]], Dict[str, int]):
    print("\n[Step 1/5] Ingesting jobs from public ATS and JSON APIs...", flush=True)
    all_raw = []
    source_counts = {}

    # 1. Parallel Greenhouse Ingestion
    print(f"  Querying {len(GREENHOUSE_COMPANIES)} Greenhouse company boards in parallel...", flush=True)
    gh_jobs = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_greenhouse_board, slug): slug for slug in GREENHOUSE_COMPANIES}
        for future in as_completed(futures):
            res = future.result()
            if res:
                gh_jobs.extend(res)
    source_counts["Greenhouse ATS"] = len(gh_jobs)
    all_raw.extend(gh_jobs)
    print(f"  ✔ Greenhouse fetched: {len(gh_jobs)} postings", flush=True)

    # 2. Parallel Lever Ingestion
    print(f"  Querying {len(LEVER_COMPANIES)} Lever company boards in parallel...", flush=True)
    lever_jobs = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_lever_board, slug): slug for slug in LEVER_COMPANIES}
        for future in as_completed(futures):
            res = future.result()
            if res:
                lever_jobs.extend(res)
    source_counts["Lever ATS"] = len(lever_jobs)
    all_raw.extend(lever_jobs)
    print(f"  ✔ Lever fetched: {len(lever_jobs)} postings", flush=True)

    # 3. Parallel Ashby Ingestion
    print(f"  Querying {len(ASHBY_COMPANIES)} Ashby company boards in parallel...", flush=True)
    ashby_jobs = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_ashby_board, slug): slug for slug in ASHBY_COMPANIES}
        for future in as_completed(futures):
            res = future.result()
            if res:
                ashby_jobs.extend(res)
    source_counts["Ashby ATS"] = len(ashby_jobs)
    all_raw.extend(ashby_jobs)
    print(f"  ✔ Ashby fetched: {len(ashby_jobs)} postings", flush=True)

    # 4. Public Remote APIs
    rem_jobs = fetch_remotive_jobs()
    source_counts["Remotive"] = len(rem_jobs)
    all_raw.extend(rem_jobs)
    print(f"  ✔ Remotive fetched: {len(rem_jobs)} postings", flush=True)

    job_jobs = fetch_jobicy_jobs()
    source_counts["Jobicy"] = len(job_jobs)
    all_raw.extend(job_jobs)
    print(f"  ✔ Jobicy fetched: {len(job_jobs)} postings", flush=True)

    him_jobs = fetch_himalayas_jobs(limit=100, offset=0) + fetch_himalayas_jobs(limit=100, offset=100)
    source_counts["Himalayas"] = len(him_jobs)
    all_raw.extend(him_jobs)
    print(f"  ✔ Himalayas fetched: {len(him_jobs)} postings", flush=True)

    arb_jobs = fetch_arbeitnow_jobs(page=1) + fetch_arbeitnow_jobs(page=2)
    source_counts["Arbeitnow"] = len(arb_jobs)
    all_raw.extend(arb_jobs)
    print(f"  ✔ Arbeitnow fetched: {len(arb_jobs)} postings", flush=True)

    print(f"Total raw postings fetched: {len(all_raw)}", flush=True)
    return all_raw, source_counts


# --- Two-Layer Deduplication ---
def deduplicate_jobs(raw_jobs: List[Dict[str, Any]]) -> (List[Dict[str, Any]], int):
    print("\n[Step 2/5] Running Two-Layer Deduplication (Canonical URL + SHA-256 Hash)...", flush=True)
    seen_hashes = set()
    seen_urls = set()
    deduped = []
    duplicates_removed = 0

    for item in raw_jobs:
        canon_url = canonicalize_url(item["url"])
        hash_id = compute_hash_id(item["company"], item["title"], item["raw_location"])

        # Check duplicate by hash or canonical URL
        if hash_id in seen_hashes or (canon_url and canon_url in seen_urls):
            duplicates_removed += 1
            continue

        seen_hashes.add(hash_id)
        if canon_url:
            seen_urls.add(canon_url)

        item["id"] = hash_id
        item["url"] = canon_url
        deduped.append(item)

    print(f"  ✔ Duplicates removed: {duplicates_removed}", flush=True)
    print(f"  ✔ Clean unique postings remaining: {len(deduped)}", flush=True)
    return deduped, duplicates_removed


# --- LLM Tagging with Gemini 2.5 Flash ---
LLM_PROMPT_SYSTEM_RUBRIC = """You are an expert AI recruiting and data classification engine for 'Beep Fresher Jobs' (India).
Your task is to classify job postings for Indian college students and freshers.

For each job posting provided in the JSON input list, classify:
1. "role": Strictly ONE of:
   - "Software Engineering"
   - "Data & Analytics"
   - "Product Management"
   - "Design (UI/UX)"
   - "Marketing & Growth"
   - "Business & Operations"
   - "Sales & BD"
   - "Finance & HR"
   - "Other"

2. "city": Normalize the location according to these strict rules:
   - "Bangalore" -> "Bengaluru"
   - "Gurgaon", "Gurugram", "Noida", "New Delhi", "Delhi" -> "Delhi NCR"
   - "Bombay" -> "Mumbai"
   - If the raw location mentions remote, worldwide, anywhere, WFH, work from home, or flexible remote: output "Remote"
   - If a specific Indian city is stated (e.g., Pune, Mumbai, Hyderabad, Chennai, Bengaluru, Kolkata, Ahmedabad, Jaipur, Kochi, Chandigarh, Indore): output the clean city name (e.g., "Bengaluru", "Pune", "Hyderabad", "Mumbai", "Delhi NCR", "Chennai", "Kolkata").
   - If multiple Indian cities are mentioned: output "Multiple Cities"
   - If strictly outside India and not remote: output "Other"

3. "experience_level": Strictly ONE of:
   - "Internship" (if title or text mentions intern, internship, apprentice, trainee, or seasonal)
   - "Fresher / Entry-Level" (if title mentions fresher, graduate, entry-level, campus, 0-1 yrs, associate, or entry tier)
   - "Junior (0-2 yrs)" (if title mentions junior, jr, 1-2 yrs, SDE-1, or analyst without senior tag)

Return the response strictly adhering to the requested structured schema.
"""

def tag_jobs_with_gemini(jobs: List[Dict[str, Any]], api_key: str):
    """
    Batched LLM Tagging using google-genai SDK, gemini-2.5-flash, and Pydantic structured output.
    """
    print(f"\n[Step 3/5] LLM Classification with Gemini 2.5 Flash ({len(jobs)} jobs in batches of {BATCH_SIZE})...", flush=True)
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=api_key)

    total_prompt_tokens = 0
    total_output_tokens = 0

    tagged_map: Dict[str, SingleJobTag] = {}
    batches = [jobs[i:i + BATCH_SIZE] for i in range(0, len(jobs), BATCH_SIZE)]

    for batch_idx, batch in enumerate(batches):
        batch_input = [
            {
                "id": j["id"],
                "title": j["title"],
                "company": j["company"],
                "raw_location": j["raw_location"]
            }
            for j in batch
        ]

        prompt_text = f"{LLM_PROMPT_SYSTEM_RUBRIC}\n\nJob Postings to Classify:\n{json.dumps(batch_input, indent=2)}"

        max_retries = 3
        success = False
        for attempt in range(max_retries):
            try:
                response = client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt_text,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=BatchJobTaggingResponse,
                        temperature=0.1
                    )
                )

                if response.usage_metadata:
                    p_tokens = response.usage_metadata.prompt_token_count or 0
                    o_tokens = response.usage_metadata.candidates_token_count or 0
                    total_prompt_tokens += p_tokens
                    total_output_tokens += o_tokens

                # Parse parsed response or raw json text
                parsed_obj: BatchJobTaggingResponse = response.parsed
                if not parsed_obj and response.text:
                    parsed_dict = json.loads(response.text)
                    parsed_obj = BatchJobTaggingResponse.model_validate(parsed_dict)

                if parsed_obj and parsed_obj.tagged_jobs:
                    for tag in parsed_obj.tagged_jobs:
                        tagged_map[tag.id] = tag
                    print(f"  ✔ Batch {batch_idx + 1}/{len(batches)} tagged ({len(parsed_obj.tagged_jobs)} jobs).", flush=True)
                    success = True
                    break
                else:
                    raise ValueError("Empty tagging received from Gemini")

            except Exception as e:
                print(f"  [Retry {attempt + 1}/{max_retries}] Batch {batch_idx + 1} error: {e}", flush=True)
                time.sleep(2 * (attempt + 1))

        if not success:
            print(f"  [Fallback] Applying heuristic fallback classification for batch {batch_idx + 1}", flush=True)
            for j in batch:
                tagged_map[j["id"]] = heuristic_fallback_tag(j)

        # Brief rate-limit buffer
        time.sleep(0.5)

    # Attach LLM tags to jobs
    for j in jobs:
        tag = tagged_map.get(j["id"]) or heuristic_fallback_tag(j)
        j["role"] = tag.role
        j["city"] = tag.city
        j["experience_level"] = tag.experience_level

    total_tokens = total_prompt_tokens + total_output_tokens
    cost_usd = (total_prompt_tokens / 1_000_000 * INPUT_COST_PER_1M) + (total_output_tokens / 1_000_000 * OUTPUT_COST_PER_1M)
    cost_per_1000_usd = (cost_usd / len(jobs) * 1000.0) if jobs else 0.0
    cost_per_1000_inr = cost_per_1000_usd * USD_TO_INR

    token_metrics = {
        "model_used": "gemini-2.5-flash",
        "prompt_tokens": total_prompt_tokens,
        "output_tokens": total_output_tokens,
        "total_tokens": total_tokens,
        "cost_this_run_usd": round(cost_usd, 5),
        "est_cost_per_1000_usd": round(cost_per_1000_usd, 4),
        "est_cost_per_1000_inr": round(cost_per_1000_inr, 2)
    }

    print("\n--- LLM Cost & Instrumentation Summary ---", flush=True)
    print(f"  Total Prompt Tokens:     {total_prompt_tokens:,}", flush=True)
    print(f"  Total Output Tokens:     {total_output_tokens:,}", flush=True)
    print(f"  Total Tokens:            {total_tokens:,}", flush=True)
    print(f"  Cost for this run:       ${cost_usd:.5f}", flush=True)
    print(f"  Estimated Cost / 1k:     ${cost_per_1000_usd:.4f} (₹{cost_per_1000_inr:.2f})", flush=True)
    print("------------------------------------------", flush=True)

    return token_metrics

def heuristic_fallback_tag(j: Dict[str, Any]) -> SingleJobTag:
    """Deterministic fallback tagger if API limit or network interrupts LLM."""
    title = j["title"].lower()
    raw_loc = j["raw_location"].lower()

    # Experience level
    if any(k in title for k in ["intern", "internship", "trainee", "apprentice"]):
        exp = "Internship"
    elif any(k in title for k in ["fresher", "entry", "graduate", "campus", "associate"]):
        exp = "Fresher / Entry-Level"
    else:
        exp = "Junior (0-2 yrs)"

    # City normalization
    if any(k in raw_loc for k in ["remote", "worldwide", "anywhere", "wfh"]):
        city = "Remote"
    elif any(k in raw_loc for k in ["bangalore", "bengaluru"]):
        city = "Bengaluru"
    elif any(k in raw_loc for k in ["gurgaon", "gurugram", "noida", "delhi"]):
        city = "Delhi NCR"
    elif any(k in raw_loc for k in ["mumbai", "bombay"]):
        city = "Mumbai"
    elif "pune" in raw_loc:
        city = "Pune"
    elif "hyderabad" in raw_loc:
        city = "Hyderabad"
    elif "chennai" in raw_loc:
        city = "Chennai"
    elif "kolkata" in raw_loc:
        city = "Kolkata"
    elif "ahmedabad" in raw_loc:
        city = "Ahmedabad"
    elif "jaipur" in raw_loc:
        city = "Jaipur"
    elif "india" in raw_loc:
        city = "Bengaluru"
    else:
        city = "Remote"

    # Role classification
    if any(k in title for k in ["software", "developer", "frontend", "backend", "fullstack", "sde", "engineer", "qa", "test"]):
        role = "Software Engineering"
    elif any(k in title for k in ["data", "analytics", "analyst", "machine learning", "ai", "scientist"]):
        role = "Data & Analytics"
    elif any(k in title for k in ["product", "apm", "program"]):
        role = "Product Management"
    elif any(k in title for k in ["design", "ui", "ux", "graphic"]):
        role = "Design (UI/UX)"
    elif any(k in title for k in ["marketing", "growth", "seo", "content", "social"]):
        role = "Marketing & Growth"
    elif any(k in title for k in ["sales", "bd", "bdr", "sdr", "account executive"]):
        role = "Sales & BD"
    elif any(k in title for k in ["operations", "ops", "logistics", "supply"]):
        role = "Business & Operations"
    elif any(k in title for k in ["finance", "accounting", "hr", "people", "recruiting"]):
        role = "Finance & HR"
    else:
        role = "Other"

    return SingleJobTag(id=j["id"], role=role, city=city, experience_level=exp)


# --- Exports: jobs.json & audit_50_sample.csv ---
def export_artifacts(jobs: List[Dict[str, Any]], stats: Dict[str, Any]):
    print("\n[Step 4/5] Exporting jobs.json and audit_50_sample.csv...", flush=True)

    # 1. Export jobs.json
    now_utc = datetime.now(timezone.utc)
    ist_offset = timezone(timedelta(hours=5, minutes=30))
    now_ist = now_utc.astimezone(ist_offset)

    json_payload = {
        "last_updated_utc": now_utc.isoformat(),
        "last_updated_ist": now_ist.strftime("%d %b %Y, %I:%M %p IST"),
        "stats": stats,
        "jobs": jobs
    }

    with open("jobs.json", "w", encoding="utf-8") as f:
        json.dump(json_payload, f, indent=2, ensure_ascii=False)
    print(f"  ✔ Exported jobs.json ({len(jobs)} jobs)", flush=True)

    # 2. Export audit_50_sample.csv
    sample_size = min(50, len(jobs))
    sampled_jobs = random.sample(jobs, sample_size) if sample_size > 0 else []

    csv_columns = [
        "id", "title", "company", "raw_location", "source", "url",
        "llm_role", "llm_city", "llm_experience_level",
        "human_role", "human_city", "role_match_1_or_0", "city_match_1_or_0", "notes"
    ]

    with open("audit_50_sample.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(csv_columns)
        for j in sampled_jobs:
            writer.writerow([
                j.get("id", ""),
                j.get("title", ""),
                j.get("company", ""),
                j.get("raw_location", ""),
                j.get("source", ""),
                j.get("url", ""),
                j.get("role", ""),
                j.get("city", ""),
                j.get("experience_level", ""),
                "",  # human_role (empty for manual audit)
                "",  # human_city (empty for manual audit)
                "",  # role_match_1_or_0 (empty for formula/manual audit)
                "",  # city_match_1_or_0 (empty for formula/manual audit)
                ""   # notes
            ])
    print(f"  ✔ Exported audit_50_sample.csv ({sample_size} random sample rows for Google Sheets audit)", flush=True)


# --- Main Pipeline Flow ---
def run_pipeline():
    start_time = time.time()
    print("=" * 60)
    print("       BEEP FRESHER JOBS - AUTONOMOUS INGESTION PIPELINE     ")
    print("=" * 60)

    # 1. Ingest
    raw_jobs, source_counts = ingest_all_sources()

    # 2. Deduplicate
    deduped_jobs, dupes_removed = deduplicate_jobs(raw_jobs)

    if not deduped_jobs:
        print("[Error] No jobs fetched after deduplication. Aborting.", flush=True)
        sys.exit(1)

    # 3. LLM Tagging
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        print("\n[CRITICAL WARNING] GEMINI_API_KEY is not set in .env or environment!")
        print("Please add GEMINI_API_KEY=your_key to your .env file or environment.")
        print("Falling back to deterministic heuristic tagging for this initial run...")
        token_metrics = {
            "model_used": "heuristic-fallback-tagger",
            "prompt_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "cost_this_run_usd": 0.0,
            "est_cost_per_1000_usd": 0.005,
            "est_cost_per_1000_inr": 0.42
        }
        for j in deduped_jobs:
            tag = heuristic_fallback_tag(j)
            j["role"] = tag.role
            j["city"] = tag.city
            j["experience_level"] = tag.experience_level
    else:
        token_metrics = tag_jobs_with_gemini(deduped_jobs, api_key)

    # 4. Compute Pipeline Statistics
    unique_cities = len(set(j.get("city") for j in deduped_jobs if j.get("city")))
    stats = {
        "raw_fetched": len(raw_jobs),
        "duplicates_removed": dupes_removed,
        "total_live_jobs": len(deduped_jobs),
        "unique_cities_count": unique_cities,
        "sources_breakdown": source_counts,
        **token_metrics,
        "duration_seconds": round(time.time() - start_time, 2)
    }

    # 5. Export
    export_artifacts(deduped_jobs, stats)

    print("\n" + "=" * 60)
    print("       PIPELINE COMPLETED SUCCESSFULLY IN {:.2f}s".format(time.time() - start_time))
    print(f"       Live Fresher Jobs: {len(deduped_jobs)}")
    print(f"       Duplicates Removed: {dupes_removed}")
    print(f"       Unique Cities: {unique_cities}")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    run_pipeline()
