# IMDb Pro Movie Data Scraping Pipeline

## Overview

This repository contains a robust and fault-tolerant **IMDb Pro web scraping pipeline** built using Python.  
The pipeline extracts detailed movie-level information using IMDb IDs and produces multiple structured datasets suitable for analytics, reporting, and downstream modeling.

The solution is designed to handle:
- Large volumes of IMDb IDs
- Partial failures and crashes
- Inconsistent or missing HTML elements
- Re-runs without duplicate data

---

## Objectives

- Scrape structured movie metadata from IMDb Pro
- Extract summaries, synopses, and language information
- Ensure resumable execution using checkpoints
- Maintain data integrity across multiple runs
- Log errors without interrupting execution

---

## Input Data
Required column:
- `IMDb_ID`

Each IMDb ID is used to construct IMDb Pro URLs for scraping.

---

## Output Files

### 1. Movie Metadata Dataset
**File:**
Movie Model 3 - IMDb Data Scraping using IMDb Pro Links.xlsx
### IMDb ID Source File

**Key columns include:**
- IMDb_ID
- Original_URL
- Movie_Meter
- IMDB_Rating
- IMDBVOTES
- Year
- US_Release_Dates
- Genre
- Runtime
- Age_base_rating
- Awards
- Production_Company
- Distributor
- Budget
- Opening_weekend
- Gross_US_Canada
- Gross_World
- Star1 – Star6
- StarMeter1 – StarMeter6

---

### 2. Movie Summary & Synopsis Dataset
**File:**
Movie Model 3 - Movies Summary and Synopsis.xlsx

**Columns:**
- IMDb_ID
- Original_URL
- Title
- Summary
- Synopsis

---

### 3. Movie Language Dataset
**File:**
Movie Model 3 - Language.xlsx

Contains language-related metadata extracted separately to maintain normalization.

---

## Project Structure
```text
.
├── webscrap_movie.py
├── Main_Movie Model 3 - IMDb Data Scraping using IMDb Pro Links.xlsx
├── Movie Model 3 - IMDb Data Scraping using IMDb Pro Links.xlsx
├── Movie Model 3 - Movies Summary and Synopsis.xlsx
├── Movie Model 3 - Language.xlsx
├── Scrap_error.log
├── Checkpoint.txt
└── README.md
```

## Technology Stack

- Python 3.11
- requests
- BeautifulSoup (bs4)
- pandas
- numpy
- openpyxl
- logging

---

## Scraping Workflow

1. Load IMDb IDs from input Excel
2. Detect existing output files
3. Resume from checkpoint if available
4. Fetch IMDb Pro movie page
5. Extract movie metadata
6. Fetch details page for summary and synopsis
7. Store results in memory buffers
8. Write batch updates to Excel
9. Update checkpoint after each successful scrape
10. Log errors without stopping execution

---

## Checkpointing Mechanism

To handle crashes or unexpected interruptions, the pipeline uses:


- Stores the index of the last successfully processed IMDb ID
- Allows seamless resumption on re-run
- Automatically deleted after full completion

This prevents duplicate scraping and data loss.

---

## Logging and Error Handling

- All errors are logged to:
- Each log entry includes:
- IMDb ID
- Error message
- Full stack trace
- Failures for individual IMDb IDs do not stop the pipeline

---

## Performance and Stability Measures

- Request timeouts
- Randomized delays between requests
- Batch DataFrame updates
- Defensive HTML parsing
- Strict validation for numeric fields (ratings, votes, meters)

---

## Handled Edge Cases

- Missing IMDb ratings
- Non-numeric rating placeholders (e.g., “Need 5 Star Rating”)
- Missing box office sections
- Inconsistent cast tables
- Advertisement or consent text appearing in data fields
- Partial HTML loads

---

## Usage Notes

- IMDb Pro access is required
- Valid headers and cookies must be configured
- Excessive scraping may result in temporary blocking
- Recommended to run during off-peak hours

---

## Conclusion

This project provides a production-grade IMDb Pro scraping solution with strong reliability, resumability, and data integrity guarantees.  
It is suitable for large-scale data collection, analytics pipelines, and feature engineering workflows.

---
