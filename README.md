# Kaggle Dataset Generator

A plug‑and‑play Python script to fetch **public datasets** and package them for **Kaggle**.

Current connectors (no API keys required):

- **USGS Earthquakes** — global quake catalog with magnitude, time, depth, lat/lon.
- **Public Holidays (Nager.Date)** — country holiday calendars across years.
- **[Optional] Wikipedia Pageviews** — per‑article daily/monthly views.
- **Kaggle packaging helper** — builds `dataset-metadata.json` and bundles files for `kaggle datasets create`.

> If your copy of the script only includes Earthquakes + Holidays, you can add the Wikipedia connector later. The README covers both.

---

## Quick start

```bash
# 0) Clone and enter the repo
# git clone <your-repo-url>
# cd <your-repo>

# 1) Install Python deps
pip install -r requirements.txt  # or: pip install pandas requests

# 2) Pull data (examples below)
python dataset_generator.py earthquakes --start 2024-01-01 --end 2025-09-05 --out ./data
python dataset_generator.py holidays --country FR --years 2015:2025 --out ./data
# (Optional) Wikipedia pageviews
python dataset_generator.py wikipedia \
  --project en.wikipedia \
  --articles "Python (programming language), Pandas (software)" \
  --start 2025-01-01 --end 2025-06-30 \
  --out ./data

# 3) Package for Kaggle (creates dataset-metadata.json and copies files)
python dataset_generator.py package \
  --title "Earthquakes 2024–2025 (USGS)" \
  --owner YOUR_KAGGLE_USERNAME \
  --slug earthquakes-2024-2025 \
  --files ./data/earthquakes.csv ./data/data_card.md \
  --out ./kaggle_pkg

# 4) Publish to Kaggle with the CLI
pip install kaggle
# Place your Kaggle API token at ~/.kaggle/kaggle.json and chmod 600 it
kaggle datasets create -p ./kaggle_pkg
```

---

## Features

- **Zero‑key public sources**: fetches from stable, openly documented endpoints.
- **Incremental runs**: earthquakes support append + dedup by `usgs_id`.
- **Data card**: appends a human‑readable `data/data_card.md` summary on each run.
- **Kaggle‑ready**: one command to create a publishable folder with metadata.

---

## Repository structure (suggested)

```
.
├── dataset_generator.py         # main script (connectors + CLI)
├── requirements.txt             # pandas, requests
├── data/                        # outputs (CSV/README/data_card)
│   ├── earthquakes.csv
│   ├── public_holidays_FR.csv
│   └── data_card.md
└── kaggle_pkg/                  # build folder for Kaggle
    ├── dataset-metadata.json
    ├── earthquakes.csv
    └── README.md                # copied from data_card.md (or your own)
```

---

## Commands & options

### 1) Earthquakes (USGS)

```bash
python dataset_generator.py earthquakes \
  --start 2024-01-01 \
  --end 2025-09-05 \
  --minmag 4.5 \
  --bbox "-180,-60,180,85" \
  --out ./data
```

**Notes**
- `--start/--end` accept `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SS` (UTC).
- If `--start` is omitted, the script defaults to the last saved end time or **7 days ago**.
- `--bbox` is optional (`minlon,minlat,maxlon,maxlat`) for geographic filtering.
- Output file: `data/earthquakes.csv` (append + dedup by `usgs_id`).

**Schema (subset)**
| column | description |
|---|---|
| `usgs_id` | Stable event id |
| `time` | Origin time (ISO 8601, UTC) |
| `updated` | Last update (ISO 8601, UTC) |
| `mag` | Magnitude |
| `lon`, `lat`, `depth_km` | Location and depth (km) |
| `place` | Human‑readable location |
| `status`, `type`, `tsunami`, `sig`, `felt`, `cdi`, `mmi`, `alert` | Event metadata |
| `url`, `detail`, `title` | Source links/summary |

---

### 2) Public Holidays (Nager.Date)

```bash
python dataset_generator.py holidays \
  --country FR \
  --years 2015:2025 \
  --out ./data
```

**Notes**
- `--country` is **ISO 3166‑1 alpha‑2** (e.g., `FR`, `US`, `DE`).
- `--years` can be a single year (`2025`) or a span (`2015:2025`).
- Output file: `data/public_holidays_<COUNTRY>.csv`.

**Schema (subset)**
| column | description |
|---|---|
| `date` | Holiday date (YYYY‑MM‑DD) |
| `local_name` | Name in local language |
| `english_name` | English name |
| `countryCode` | Country code |
| `is_fixed` | Occurs on a fixed date each year |
| `is_global` | Applies nationwide |
| `types` | Category (e.g., Public, Bank) |
| `counties` | Pipe‑separated list if limited to counties |
| `year` | Convenience field added by the script |

---

### 3) [Optional] Wikipedia Pageviews (Wikimedia REST)

```bash
python dataset_generator.py wikipedia \
  --project en.wikipedia \
  --articles "Python (programming language), Pandas (software)" \
  --start 2025-01-01 --end 2025-06-30 \
  --access all-access --agent user --granularity daily \
  --out ./data
```

**Schema (subset)**
| column | description |
|---|---|
| `project` | e.g., `en.wikipedia` |
| `article` | Page title |
| `access` | `desktop`, `mobile-app`, `mobile-web`, or `all-access` |
| `agent` | `user`, `spider`, or `bot` |
| `granularity` | `daily` or `monthly` |
| `date` | Bucket date |
| `views` | Pageviews count |

---

## Data card

Each command appends a short section to `data/data_card.md` with:
- **Source**, **window/years**, **country/project**, **record count**, and field highlights.
- You can copy this into `kaggle_pkg/README.md` when publishing.

---

## Packaging for Kaggle

The `package` command creates a folder that the Kaggle CLI can publish.

```bash
python dataset_generator.py package \
  --title "Public Holidays (FR) 2015–2025" \
  --owner YOUR_KAGGLE_USERNAME \
  --slug public-holidays-fr-2015-2025 \
  --files ./data/public_holidays_FR.csv ./data/data_card.md \
  --out ./kaggle_pkg

# Then publish
kaggle datasets create -p ./kaggle_pkg
# For updates later
kaggle datasets version -p ./kaggle_pkg -m "Update through 2025-09-05"
```

**Kaggle token**: place API credentials at `~/.kaggle/kaggle.json` and `chmod 600 ~/.kaggle/kaggle.json`.

---

## Scheduling

Run regularly with cron (Linux/macOS):

```
# Every day at 02:00 UTC, append latest earthquakes and refresh FR holidays yearly
0 2 * * * cd /path/to/repo && \
  /usr/bin/python3 dataset_generator.py earthquakes --out ./data && \
  /usr/bin/python3 dataset_generator.py holidays --country FR --years 2015:2025 --out ./data >> cron.log 2>&1
```

(You can later push a new dataset version with the Kaggle CLI.)

---

## Troubleshooting

- **HTTP 429 / 5xx**: the script retries with exponential backoff; if it still fails, re‑run later.
- **Empty output**: check date ranges, `minmag`, and `bbox` filters; Wikipedia titles must match page names.
- **Windows paths**: use quotes around paths containing spaces.

---

## Licensing

- **Code**: MIT (or choose your license).
- **Data you publish**: recommend **CC0‑1.0** for maximum re‑use, or **CC‑BY‑4.0** if attribution is required. Check each upstream source’s terms of use.

---

## Sources & attribution

- **USGS Earthquake Catalog** — public FDSN event API.
- **Nager.Date Public Holidays** — open public holiday API.
- **Wikimedia REST (Pageviews)** — public metrics API.

Please attribute these sources in your Kaggle dataset description.

---

## Contributing / Extending

To add a new connector, follow the existing pattern:
1. Write a `*_fetch()` function that returns a tidy `pandas.DataFrame`.
2. Add a `*_command(args)` that handles CLI args, writes CSV, and updates `data_card.md`.
3. Register a new subcommand in `build_parser()`.

Ideas to add next: **OpenAQ air quality**, **Open‑Meteo weather features**, **EV charging stations (OpenChargeMap)**.

---

## Maintainer

- Script and README generated via ChatGPT; feel free to edit to match your project style.

