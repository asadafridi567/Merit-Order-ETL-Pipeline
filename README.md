# Merit Order ETL Pipeline

Bronze → Silver → Gold pipeline on Databricks that tracks how plant-wise fuel costs on
Pakistan's power grid moved across May–August 2026, using NTDC's Merit Order / Economic
Merit Order (EMO) reports as the source.

**Author:** Muhammad Asad
**Built for:** Renewables First, Data Engineer technical assessment

## Pipeline overview

| Notebook | Layer | What it does |
|---|---|---|
| `bronze ingestion.ipynb` | Bronze | Discovers and downloads every Merit Order/MO/EMO PDF for May–Aug 2026 into a Unity Catalog Volume, then OCR-parses each PDF's table into `rf_assessment.bronze.merit_order_data` |
| `silver layer transformation.ipynb` | Silver | Runs data quality checks, cleans and standardizes types, computes cost-breakdown percentages, writes `rf_assessment.silver.merit_order_data` |
| `Gold Layer Insights.ipynb` | Gold | Aggregates plant-wise and fuel-type-wise cost variation, flags outliers, produces the charts and summary tables behind the findings |

Each layer writes a Delta table before the next layer reads it, so any stage can be
re-run independently and the raw PDFs are never touched again after Bronze — if a
transformation rule turns out wrong, it's fixed and re-run against Bronze, not
re-downloaded from NTDC.

## Design decisions

**Ingestion works around the site being a JS-rendered (Angular) app, not a scrapeable
HTML page.** NTDC's merit-order page doesn't expose report links in its raw HTML — the
links only exist inside the compiled Angular JS bundle. Rather than trying to render the
page or guess filenames, the pipeline downloads the bundle directly and regex-extracts
every `/services/meritorder/...pdf` path out of it, then filters down to the target
year/months. This is more reliable than pattern-guessing filenames, since it reads the
exact paths the site itself uses.

**PDF parsing uses Databricks' `ai_parse_document` rather than a plain-Python PDF
library.** The Merit Order reports are scanned/image-based tables, not text-layer PDFs,
so a library like `pdfplumber` can't extract them directly - `ai_parse_document` OCRs
each page and returns structured table elements, which are then parsed with
`pandas.read_html` into the actual 8-column merit order schema (`sr_no`, `plant_name`,
`fuel_type`, `other_cost`, `fuel_cost`, `vom_cost`, `specific_cost`,
`status_last_order`).

**Outlier detection is done per fuel type, not fleet-wide.** RFO, RLNG, and coal plants
sit at structurally different cost levels, so a single IQR threshold across the whole
fleet would flag most RFO plants as "outliers" purely from scale. Gold computes Q1/Q3
and the IQR bounds separately within each `fuel_type_clean` group, so a flagged plant is
actually unusual for its fuel type, not just expensive in absolute terms.

**Silver keeps every report snapshot rather than collapsing straight to one row per
plant per month.** NTDC doesn't publish once a month - the "Merit Order" is revised
several times (as an "EMO") whenever fuel prices are updated, giving roughly 6-7 report
files per month rather than one. Silver preserves every snapshot (one row per
plant/report file); Gold does the monthly aggregation on top of that, which keeps the
underlying data auditable back to a specific report date rather than baking an averaging
assumption into Silver where it can't be revisited later.

## Assumptions made

- **The Angular bundle's filename is a point-in-time snapshot.** It's referenced by its
  build hash, which changes whenever NTDC redeploys the site. If ingestion is re-run
  after a redeploy, the bundle URL will need to be re-discovered from the site's current
  `index.html` rather than reused as-is.
- **A table with exactly 8 columns, once OCR'd, is the merit order table.** Some report
  PDFs contain a small reference/notes table in addition to the main merit order table;
  column count is used to distinguish them. This is a pragmatic heuristic under the
  assessment deadline, not a guarantee - it assumes no other table in any report happens
  to also have 8 columns.
- **`specific_cost` is the total unit cost**, and `fuel_cost` / `vom_cost` / `other_cost`
  are its components - the cost-breakdown percentages in Silver are calculated on that
  basis.
- **Cost figures across all report files are already in the same unit** (PKR/kWh) and
  don't require conversion - no unit inconsistency was found across the parsed reports,
  but this wasn't independently verified against a second source.

## Data quality issues encountered, and how they were handled

| Issue | Where caught | Handling |
|---|---|---|
| Inconsistent filename dates: some reports use a 2-digit year (`EMO 19-04-26.pdf`), others 4-digit (`EMO 23-04-2026.pdf`) | Bronze, date extraction | Regex parses the date out of the filename; rows where extraction fails are logged rather than silently included with a blank date |
| Inconsistent month-folder naming on the source site (full month names vs. 3-letter abbreviations) | Bronze, discovery | The actual folder string found in the JS bundle path is used as-is for the download URL, rather than assuming one fixed format |
| Multiple report revisions per month (original Merit Order + several EMOs) | Silver / Gold | Every snapshot is kept as its own row rather than deduplicated away; monthly aggregation happens explicitly in Gold, where the choice of "average across revisions" vs. "latest revision in the month" is a visible, deliberate step |
| Missing/non-numeric cost values (e.g. `-` used for zero-cost fuel types like hydro) | Silver | Cleaned to `0.0` explicitly rather than dropped, since a `-` here means "no fuel cost applies," not "unknown" |
| Possible duplicate rows | Silver | Checked on the full set of key fields (plant, date, file, fuel type, all four cost columns) rather than plant+date alone, since the same plant legitimately appears more than once per report if it runs on more than one fuel type |
| Negative cost values | Silver | Explicitly checked for and flagged (none were found in this run, but the check runs every time rather than assuming it's unnecessary) |
| Year/month columns disagreeing with the parsed `effective_date` | Silver | Cross-validated against each other as a consistency check, to catch a bad date parse before it reaches the analysis layer |

## Known gaps / next steps

- Outlier and variation figures are only as good as the OCR extraction - a misread digit
  in a scanned table would currently pass through as a valid number rather than being
  flagged; a sanity-range check per fuel type (e.g. an implausible Rs/kWh value) would
  catch this and is a natural next addition.
- The Angular bundle URL should be discovered dynamically from the site's current HTML
  rather than hardcoded, so re-running ingestion after a site redeploy doesn't require a
  manual update.
