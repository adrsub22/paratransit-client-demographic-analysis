# Paratransit Client Block Group Aggregation

A privacy-first geospatial pipeline that turns sensitive paratransit client data into planner-ready map layers — aggregated to Census block groups and automatically published to ArcGIS Online.

> Built for a public transit agency's service planning department. This is a sanitized version of the working pipeline; server names, file paths, and database identifiers have been replaced with placeholders.

---

## What this project does

Public transit agencies that run paratransit services need to understand **where** their riders live, **how often** they use the service, and whether they fall **inside or outside** the official service area. But client home addresses, dates of birth, and trip histories are highly sensitive — they can't just be dropped onto a map.

This pipeline handles the full workflow end-to-end:

1. Pulls client home records and completed trip history from an internal SQL Server database
2. Downloads the latest U.S. Census TIGER/Line block groups and filters to the relevant county
3. Geocodes each client home and assigns it to a service tier (e.g. 2-Day, 5-Day, 6-Day, 7-Day) based on the agency's authoritative service-area polygons
4. Aggregates everything to the block-group level — counts, rates, age distributions, active and scheduled-rider metrics
5. Applies small-count suppression so block groups with very few riders are excluded entirely
6. Strips out every identifier, coordinate, and date of birth before publishing
7. Publishes the cleaned layers as hosted feature services on ArcGIS Online

The output is something a planner can drop into a dashboard the next morning — without ever touching the underlying personal data.

---

## Why it matters

- **Planners get answers fast.** Ridership patterns are visible at a glance instead of waiting on ad-hoc analyst requests.
- **Service expansion is data-driven.** Outside-service-area concentrations highlight where the agency might consider expanding coverage or building partnerships.
- **It runs on a schedule.** Re-running the pipeline keeps the maps current with no manual intervention.
- **Privacy is built in, not bolted on.** Aggregation, suppression, and field validation are part of the pipeline itself.

---

## Sample outputs

*Map screenshots from the published AGOL layers:*

<!-- Drop your images into the /images folder and reference them here -->

| Layer | Description |
| --- | --- |
| ![Client distribution](images/client_distribution.png) | Total clients by block group, color-graduated |
| ![Service tier coverage](images/service_tier_coverage.png) | Service-area polygons dissolved by tier (2/5/6/7-Day) |
| ![Outside-service active riders](images/outside_service_active.png) | Block groups with active riders falling outside the service area |

---

## How privacy is enforced

Three layers of protection, applied in order:

1. **Aggregation** — every published metric is summarized to block groups, never individual addresses.
2. **Suppression** — block groups below a configurable minimum client count are dropped before publishing.
3. **Field validation** — a final automated check fails the pipeline if any blocked field (`ClientId`, `DOB`, `Latitude`, `Longitude`, raw address, etc.) is still present at publish time.

Temporary working layers that contain sensitive intermediate data are deleted automatically at the end of each run.

---

## Tech stack

- **Python 3** with **ArcPy** (ArcGIS Pro)
- **pandas** for data wrangling and aggregation
- **pyodbc** for SQL Server connectivity
- **ArcGIS Online** for hosted feature service publishing
- **U.S. Census TIGER/Line** for block group geometry

---

## How it works (at a glance)

```
SQL Server (clients + trips)
        │
        ▼
  pandas QA + merge
        │
        ▼
  Temporary point feature class  ─────┐
        │                             │
        ▼                             │
  Service-area spatial join           │  (deleted after run)
        │                             │
        ▼                             │
  Block-group spatial join  ──────────┘
        │
        ▼
  Aggregate to block groups
        │
        ▼
  Suppress small counts + drop sensitive fields
        │
        ▼
  Publish to ArcGIS Online
```

---

## Configuration

The top of the script exposes everything a planner would want to adjust without touching the analysis logic:

- **Date window** for trip activity
- **Active client** definition (minimum completed trips in the window)
- **Scheduled rider** definition (average trips per week)
- **Privacy thresholds** for both the main and outside-service block group layers
- **County FIPS code** and TIGER/Line year
- **AGOL service names** and publishing toggle

---

## Repository contents

```
paratransit-block-group-analysis/
├── README.md
├── paratransit_block_group_aggregation.py   # the main pipeline
├── requirements.txt
├── .gitignore
├── LICENSE
└── images/                                  # map screenshots
```

---

## Note on the SQL queries

The queries reflect the data structure of a typical paratransit scheduling system (clients, addresses, bookings, scheduled events). They should be readable as a reference for the analytical approach, but the table and column names would need to be adapted to a specific environment.

---

## License

MIT — see [LICENSE](LICENSE).
