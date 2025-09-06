#!/usr/bin/env python3
"""
Kaggle Dataset Generator — plug‑and‑play

Two no‑API‑key connectors to get you shipping fast:
  1) USGS Earthquakes (incremental) — hourly/daily pulls, dedup by event id
  2) Public Holidays (Nager.Date) — multi‑year by country code

Also includes a simple Kaggle packaging helper that builds a dataset folder
with dataset‑metadata.json you can push via the Kaggle CLI.

Quick start:
  python dataset_generator.py earthquakes --start 2024-01-01 --end 2025-09-05 --out ./data
  python dataset_generator.py holidays --country FR --years 2015:2025 --out ./data
  python dataset_generator.py package --title "Earthquakes 2024-2025 (USGS)" \
      --owner YOUR_KAGGLE_USERNAME --slug earthquakes-2024-2025 \
      --files ./data/earthquakes.csv ./data/data_card.md --out ./kaggle_pkg

Requires: Python 3.9+, requests, pandas
  pip install requests pandas

Notes:
- Internet is required to fetch data but not for packaging.
- Always respect source terms of use. This script targets public, rate‑limited
  APIs that allow non‑commercial reuse with attribution.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import os
from pathlib import Path
import shutil
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests

# -----------------------------
# Utilities
# -----------------------------

USER_AGENT = "kaggle-dataset-generator/1.0 (+https://kaggle.com)"


def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def read_state(path: Path) -> Dict[str, Any]:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return {}
    return {}


def write_state(path: Path, state: Dict[str, Any]) -> None:
    path.write_text(json.dumps(state, indent=2, sort_keys=True))


class BackoffSession(requests.Session):
    """Requests session with basic exponential backoff for 429/5xx."""

    def __init__(self, max_retries: int = 5, backoff: float = 1.5):
        super().__init__()
        self.max_retries = max_retries
        self.backoff = backoff
        self.headers.update({"User-Agent": USER_AGENT})

    def get_json(self, url: str, **kwargs) -> Any:
        attempt = 0
        while True:
            attempt += 1
            resp = self.get(url, timeout=60, **kwargs)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                # Honor Retry-After when present
                ra = resp.headers.get("Retry-After")
                if ra is not None:
                    try:
                        delay = float(ra)
                    except ValueError:
                        delay = self.backoff ** attempt
                else:
                    delay = self.backoff ** attempt
                time.sleep(delay)
                continue
            resp.raise_for_status()


# -----------------------------
# Connector: USGS Earthquakes
# -----------------------------

USGS_BASE = "https://earthquake.usgs.gov/fdsnws/event/1/query"


def usgs_fetch(
    start: str,
    end: str,
    minmag: Optional[float] = None,
    bbox: Optional[Tuple[float, float, float, float]] = None,  # minlon, minlat, maxlon, maxlat
    limit: int = 20000,
) -> pd.DataFrame:
    """
    Fetch earthquakes between start and end (ISO YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS).
    Returns tidy DataFrame with one row per event.
    """
    params = {
        "format": "geojson",
        "starttime": start,
        "endtime": end,
        "limit": limit,
        "orderby": "time-asc",
    }
    if minmag is not None:
        params["minmagnitude"] = minmag
    if bbox is not None:
        params.update({
            "minlongitude": bbox[0],
            "minlatitude": bbox[1],
            "maxlongitude": bbox[2],
            "maxlatitude": bbox[3],
        })

    s = BackoffSession()
    js = s.get_json(USGS_BASE, params=params)

    feats = js.get("features", [])
    rows = []
    for f in feats:
        prop = f.get("properties", {})
        geom = f.get("geometry", {}) or {}
        coords = geom.get("coordinates", [None, None, None])
        rows.append({
            "usgs_id": f.get("id"),
            "time": dt.datetime.utcfromtimestamp((prop.get("time") or 0) / 1000.0).isoformat() + "Z" if prop.get("time") else None,
            "updated": dt.datetime.utcfromtimestamp((prop.get("updated") or 0) / 1000.0).isoformat() + "Z" if prop.get("updated") else None,
            "mag": prop.get("mag"),
            "place": prop.get("place"),
            "type": prop.get("type"),
            "status": prop.get("status"),
            "tsunami": prop.get("tsunami"),
            "sig": prop.get("sig"),
            "felt": prop.get("felt"),
            "cdi": prop.get("cdi"),
            "mmi": prop.get("mmi"),
            "alert": prop.get("alert"),
            "lon": coords[0],
            "lat": coords[1],
            "depth_km": coords[2],
            "url": prop.get("url"),
            "detail": prop.get("detail"),
            "title": prop.get("title"),
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df.sort_values("time", inplace=True, ignore_index=True)
    return df


def earthquakes_command(args: argparse.Namespace) -> None:
    out_dir = ensure_dir(Path(args.out))
    state_path = out_dir / "state_earthquakes.json"
    state = read_state(state_path)

    start = args.start
    if not start:
        # Incremental: default to the last seen end time or 7 days ago
        start = state.get("last_end") or (dt.datetime.utcnow() - dt.timedelta(days=7)).date().isoformat()
    end = args.end or dt.datetime.utcnow().date().isoformat()

    bbox = None
    if args.bbox:
        parts = [float(x) for x in args.bbox.split(",")]
        if len(parts) != 4:
            raise SystemExit("--bbox must be 'minlon,minlat,maxlon,maxlat'")
        bbox = (parts[0], parts[1], parts[2], parts[3])

    df = usgs_fetch(start=start, end=end, minmag=args.minmag, bbox=bbox)
    csv_path = out_dir / "earthquakes.csv"

    if csv_path.exists() and not args.overwrite:
        # Append & deduplicate on usgs_id
        old = pd.read_csv(csv_path)
        combined = pd.concat([old, df], ignore_index=True)
        combined.drop_duplicates(subset=["usgs_id"], inplace=True)
        combined.sort_values("time", inplace=True)
        combined.to_csv(csv_path, index=False)
    else:
        df.to_csv(csv_path, index=False)

    # Minimal data card update
    dc_path = out_dir / "data_card.md"
    append_data_card(dc_path, section_title="USGS Earthquakes", content=f"""
**Source:** USGS Earthquake Catalog (FDSN API)\
**Window:** {start} → {end}\
**Records added:** {len(df)}\
**Fields:** usgs_id, time, updated, mag, place, type, status, tsunami, sig, felt, cdi, mmi, alert, lon, lat, depth_km, url, detail, title.
""")

    state.update({"last_end": end, "last_run": dt.datetime.utcnow().isoformat() + "Z"})
    write_state(state_path, state)
    print(f"Wrote {csv_path} with {sum(1 for _ in open(csv_path, 'rb')) - 1} rows (excluding header).")


# -----------------------------
# Connector: Public Holidays (Nager.Date)
# -----------------------------

NAGER_BASE = "https://date.nager.at/api/v3/PublicHolidays/{year}/{country}"


def parse_years_span(span: str) -> List[int]:
    """Parse '2015:2025' or '2020' into list of ints."""
    span = str(span)
    if ":" in span:
        a, b = span.split(":", 1)
        return list(range(int(a), int(b) + 1))
    return [int(span)]


def holidays_fetch(country: str, years: Iterable[int]) -> pd.DataFrame:
    s = BackoffSession()
    frames = []
    for y in years:
        url = NAGER_BASE.format(year=y, country=country)
        js = s.get_json(url)
        # normalize
        df = pd.DataFrame(js)
        if df.empty:
            continue
        # Flatten 'counties' list into pipe-separated string
        if "counties" in df.columns:
            df["counties"] = df["counties"].apply(lambda x: "|".join(x) if isinstance(x, list) else None)
        df["year"] = y
        frames.append(df)
    if frames:
        out = pd.concat(frames, ignore_index=True)
        # Rename a few columns to snake_case
        rename = {
            "localName": "local_name",
            "englishName": "english_name",
            "global": "is_global",
            "fixed": "is_fixed",
        }
        for k, v in rename.items():
            if k in out.columns:
                out.rename(columns={k: v}, inplace=True)
        return out
    return pd.DataFrame()


def holidays_command(args: argparse.Namespace) -> None:
    out_dir = ensure_dir(Path(args.out))
    years = parse_years_span(args.years) if args.years else [dt.datetime.utcnow().year]
    df = holidays_fetch(args.country, years)

    csv_path = out_dir / f"public_holidays_{args.country.upper()}.csv"
    if csv_path.exists() and not args.overwrite:
        old = pd.read_csv(csv_path)
        combined = pd.concat([old, df], ignore_index=True)
        combined.drop_duplicates(subset=["date", "countryCode"], inplace=True)
        combined.sort_values(["date", "countryCode"], inplace=True)
        combined.to_csv(csv_path, index=False)
    else:
        df.to_csv(csv_path, index=False)

    dc_path = out_dir / "data_card.md"
    append_data_card(dc_path, section_title=f"Public Holidays — {args.country.upper()}", content=f"""
**Source:** Nager.Date Public Holidays API\
**Years:** {min(years)}–{max(years)}\
**Country:** {args.country.upper()}\
**Records added:** {len(df)}\
**Fields (subset):** date, local_name, english_name, countryCode, fixed, is_global, types, counties.
""")

    print(f"Wrote {csv_path} with {sum(1 for _ in open(csv_path, 'rb')) - 1} rows (excluding header).")


# -----------------------------
# Data card helper
# -----------------------------

def append_data_card(path: Path, section_title: str, content: str) -> None:
    header = f"# Data Card\n\n" if not path.exists() else ""
    section = f"\n## {section_title}\n\n{content.strip()}\n"
    with open(path, "a", encoding="utf-8") as f:
        if header:
            f.write(header)
        f.write(section)


# -----------------------------
# Kaggle packaging helper
# -----------------------------

def build_kaggle_package(
    out_dir: Path,
    title: str,
    owner: str,
    slug: str,
    files: List[Path],
    description_md: Optional[Path] = None,
    license_name: str = "CC0-1.0",
) -> Path:
    """
    Create a folder with dataset-metadata.json and copy files in.
    Use Kaggle CLI to push:
      kaggle datasets create -p <folder>
    For updates later:
      kaggle datasets version -p <folder> -m "Update"
    """
    pkg = ensure_dir(out_dir)
    meta = {
        "title": title,
        "id": f"{owner}/{slug}",
        "licenses": [{"name": license_name}],
    }
    (pkg / "dataset-metadata.json").write_text(json.dumps(meta, indent=2))

    # Copy files next to metadata
    for src in files:
        dst = pkg / Path(src).name
        shutil.copy2(src, dst)

    # If a description file is provided, ensure it's named README.md per Kaggle UX
    if description_md and Path(description_md).exists():
        shutil.copy2(description_md, pkg / "README.md")

    return pkg


def package_command(args: argparse.Namespace) -> None:
    out_dir = ensure_dir(Path(args.out))
    files = [Path(f) for f in args.files]
    for f in files:
        if not f.exists():
            raise SystemExit(f"File not found: {f}")
    desc = Path(args.description) if args.description else None
    pkg_path = build_kaggle_package(
        out_dir=out_dir,
        title=args.title,
        owner=args.owner,
        slug=args.slug,
        files=files,
        description_md=desc,
        license_name=args.license,
    )
    print(f"Kaggle package prepared at: {pkg_path.resolve()}")
    print("Next steps:\n  1) pip install kaggle\n  2) Place your Kaggle API token at ~/.kaggle/kaggle.json (chmod 600)\n  3) Run: kaggle datasets create -p", str(pkg_path))


# -----------------------------
# CLI
# -----------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generate and package public datasets for Kaggle.")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_eq = sub.add_parser("earthquakes", help="Fetch earthquakes from USGS")
    p_eq.add_argument("--start", type=str, default=None, help="Start time (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)")
    p_eq.add_argument("--end", type=str, default=None, help="End time (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)")
    p_eq.add_argument("--minmag", type=float, default=None, help="Minimum magnitude filter")
    p_eq.add_argument("--bbox", type=str, default=None, help="minlon,minlat,maxlon,maxlat")
    p_eq.add_argument("--out", type=str, default="./data", help="Output folder")
    p_eq.add_argument("--overwrite", action="store_true", help="Overwrite existing CSV instead of append+dedup")
    p_eq.set_defaults(func=earthquakes_command)

    p_h = sub.add_parser("holidays", help="Fetch public holidays by country (Nager.Date)")
    p_h.add_argument("--country", type=str, required=True, help="ISO 3166-1 alpha-2 (e.g., FR, US, DE)")
    p_h.add_argument("--years", type=str, default=None, help="Year or span like 2015:2025")
    p_h.add_argument("--out", type=str, default="./data", help="Output folder")
    p_h.add_argument("--overwrite", action="store_true", help="Overwrite existing CSV instead of append+dedup")
    p_h.set_defaults(func=holidays_command)

    p_pkg = sub.add_parser("package", help="Assemble a Kaggle dataset folder")
    p_pkg.add_argument("--title", type=str, required=True, help="Dataset title")
    p_pkg.add_argument("--owner", type=str, required=True, help="Your Kaggle username (owner slug)")
    p_pkg.add_argument("--slug", type=str, required=True, help="Dataset slug (lowercase-dash)")
    p_pkg.add_argument("--files", nargs="+", required=True, help="Files to include (CSV/Parquet/README)")
    p_pkg.add_argument("--description", type=str, default=None, help="Path to a Markdown README to include")
    p_pkg.add_argument("--license", type=str, default="CC0-1.0", help="License short name (e.g., CC0-1.0, CC-BY-4.0)")
    p_pkg.add_argument("--out", type=str, default="./kaggle_pkg", help="Output folder for the package")
    p_pkg.set_defaults(func=package_command)

    return p


def main(argv: Optional[List[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()

