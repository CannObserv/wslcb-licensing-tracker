# WSLCB Licensing Tracker

A web application that scrapes, archives, and provides searchable access to licensing activity published by the **Washington State Liquor and Cannabis Board (WSLCB)**.

The Board publishes a [rolling 30-day report](https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp) of new license applications, approvals, and discontinuances for alcohol, cannabis, tobacco, and vapor product businesses across Washington State. This tracker scrapes that report daily and preserves the data in a searchable database, building a historical archive that extends beyond the 30-day window.

**Live instance:** [https://wslcb-licensing-tracker.exe.xyz:8000/](https://wslcb-licensing-tracker.exe.xyz:8000/)

## Features

- **Daily automated scraping** of the WSLCB statewide licensing activity page
- **Full-text search** across business names, locations, applicants, license types, and license numbers
- **Normalized endorsements** — numeric license codes from approved/discontinued records are resolved to human-readable names via a managed code→endorsement mapping
- **Address standardization** — raw business addresses are parsed into structured components (street, suite, city, state, ZIP) via an external validation API, fixing ~6% of records with mis-parsed cities
- **Filterable results** by record type, application type, endorsement, city, and date range
- **Record detail pages** with related records for the same license number
- **CSV export** of any search result set
- **Historical archive** — the source only shows 30 days, but the database retains all data
- **License transfer tracking** — ASSUMPTION records capture both seller and buyer business names and applicants
- **Deduplication** — safe to re-scrape; duplicate records are automatically skipped

## Data

The tracker captures three categories of licensing activity:

| Section | Description |
|---|---|
| **New Applications** | Businesses that have submitted new license applications, renewals, changes of location, tradename changes, corporate officer changes, and other application types |
| **Approved** | Licenses recently approved by the Board |
| **Discontinued** | Licenses that have been discontinued |

Each record includes:

| Field | Description |
|---|---|
| Date | Notification, approval, or discontinuance date |
| Business Name | Registered business name (for ASSUMPTION records, the buyer's business name) |
| Business Location | Full street address including city, state, and ZIP (raw and standardized components) |
| Applicant(s) | Named applicants (new applications only; for ASSUMPTION records, the buyer's applicants) |
| Previous Business Name | Seller's business name (ASSUMPTION records only) |
| Previous Applicant(s) | Seller's applicants (ASSUMPTION records only) |
| Endorsements | One or more license/endorsement types (e.g., "CANNABIS RETAILER", "GROCERY STORE - BEER/WINE"), normalized from text names or numeric WSLCB codes |
| Application Type | RENEWAL, NEW APPLICATION, CHANGE OF LOCATION, ASSUMPTION, DISCONTINUED, etc. |
| License Number | WSLCB license number |
| Contact Phone | Business contact phone number |

## Architecture

| Component | Technology |
|---|---|
| Scraper | Python, [httpx](https://www.python-httpx.org/), [BeautifulSoup4](https://www.crummy.com/software/BeautifulSoup/) + lxml |
| Address validation | External API ([address-validator](https://address-validator.exe.xyz:8000/docs)) for USPS-standardized parsing |
| Database | SQLite with [FTS5](https://www.sqlite.org/fts5.html) full-text search |
| Web framework | [FastAPI](https://fastapi.tiangolo.com/) with [Jinja2](https://jinja.palletsprojects.com/) templates |
| Frontend | Server-rendered HTML, [HTMX](https://htmx.org/), [Tailwind CSS](https://tailwindcss.com/) (CDN) |
| Scheduling | systemd timer (daily) |

## Project Structure

```
wslcb-licensing-tracker/
├── app.py                  # FastAPI web application
├── database.py             # SQLite schema, queries, FTS5 full-text search
├── endorsements.py         # License endorsement normalization (code↔name mappings)
├── address_validator.py    # Address validation API client
├── scraper.py              # WSLCB page scraper
├── env                     # API keys (gitignored, 640 root:exedev)
├── templates/
│   ├── base.html           # Base layout template
│   ├── index.html          # Dashboard with stats
│   ├── search.html         # Search interface with filters
│   ├── detail.html         # Record detail page
│   └── partials/
│       └── results.html    # Search results partial (HTMX)
├── static/                 # Static assets
├── data/                   # Persistent data (gitignored)
│   ├── wslcb.db            # SQLite database
│   └── [yyyy]/             # Archived HTML snapshots by year
├── wslcb-web.service       # systemd service for the web app
├── wslcb-task@.service     # systemd template for oneshot tasks (scrape, refresh, backfill)
└── wslcb-scraper.timer     # systemd timer (daily at 6 AM Pacific)
```

## Setup

### Prerequisites

- Python 3.12+
- systemd (for scheduling; optional if running manually)

### Installation

```bash
git clone https://github.com/CannObserv/wslcb-licensing-tracker.git
cd wslcb-licensing-tracker

python3 -m venv venv
source venv/bin/activate
pip install fastapi uvicorn jinja2 httpx beautifulsoup4 lxml python-multipart
```

### Run the initial scrape

```bash
python scraper.py
```

This fetches the current 30-day report, populates the SQLite database (`data/wslcb.db`), and archives a copy of the source HTML under `data/`.

### Start the web application

```bash
uvicorn app:app --host 0.0.0.0 --port 8000
```

Then visit [http://localhost:8000](http://localhost:8000).

### Set up daily scraping (systemd)

```bash
sudo cp wslcb-web.service /etc/systemd/system/
sudo cp wslcb-task@.service /etc/systemd/system/
sudo cp wslcb-scraper.timer /etc/systemd/system/

sudo systemctl daemon-reload
sudo systemctl enable --now wslcb-web.service
sudo systemctl enable --now wslcb-scraper.timer
```

The scraper runs daily at 6:00 AM Pacific (14:00 UTC) with up to 5 minutes of random delay.

Check the timer status:

```bash
systemctl list-timers wslcb-scraper.timer
journalctl -u wslcb-task@scrape.service   # scraper logs
journalctl -u wslcb-web.service       # web app logs
```

### Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `ADDRESS_VALIDATOR_API_KEY` | *(none)* | API key for the address validation service (also read from `./env` file) |

The SQLite database and archived HTML snapshots are stored in `./data/` relative to the project root.

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Dashboard with summary statistics |
| `GET /search` | Search interface (HTML) or HTMX partial results |
| `GET /record/{id}` | Record detail page |
| `GET /export` | CSV export (accepts same query params as `/search`) |
| `GET /api/stats` | JSON summary statistics |

## License Type Normalization

The WSLCB source page uses two different representations for license types:

- **New applications** list endorsements as semicolon-separated text (e.g., `GROCERY STORE - BEER/WINE; SNACK BAR`)
- **Approved/discontinued** records use opaque numeric codes (e.g., `450,`)

The tracker normalizes both into a shared `license_endorsements` table, linked to records via a `record_endorsements` junction table. A seed mapping of 71 known codes is built into `endorsements.py`, and new mappings are automatically discovered by cross-referencing license numbers that appear in both sections.

## Address Standardization

Raw business addresses from the WSLCB page are standardized via an external address validation API into structured USPS-standard components:

| Field | Example |
|---|---|
| `address_line_1` | `1200 WESTLAKE AVE N` |
| `address_line_2` | `STE 100` |
| `std_city` | `SEATTLE` |
| `std_state` | `WA` |
| `std_zip` | `98109-3528` |

Addresses are validated at scrape time for new records. Existing records can be backfilled:

```bash
python scraper.py --backfill-addresses
```

To re-validate all addresses (e.g., after the validation service is updated):

```bash
python scraper.py --refresh-addresses
```

This is safe to interrupt — progress is committed in batches and each record's timestamp is updated individually.

The original raw `business_location` string is always preserved. If the validation service is unavailable, the scrape completes normally and standardized fields remain empty until a future backfill.

## ASSUMPTION Records

ASSUMPTION records represent one business assuming (purchasing) a license from another. They contain data about both the seller and buyer, using different field labels than standard records:

| Source Field | DB Column | Description |
|---|---|---|
| Current Business Name | `previous_business_name` | Seller's business name |
| Current Applicant(s) | `previous_applicants` | Seller's applicants (often empty) |
| New Business Name | `business_name` | Buyer's business name |
| New Applicant(s) | `applicants` | Buyer's applicants |
| Business Location | `business_location` | Single shared location |
| Contact Phone | `contact_phone` | Buyer's contact phone |

To backfill assumption data for records scraped before this feature was added:

```bash
python scraper.py --backfill-assumptions
```

This parses all archived HTML snapshots and updates existing ASSUMPTION records that have empty business names.

## Data Source

All data is sourced from the Washington State Liquor and Cannabis Board's public licensing activity page:

**[https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp](https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp)**

> *STATEWIDE New License Applications, Approvals, and Discontinuances (for past 30 days, in order of most recent date)*

This is an unofficial tracker and is not affiliated with or endorsed by the WSLCB.

## License

MIT
