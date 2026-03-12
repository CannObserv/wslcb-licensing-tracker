# Source Page Reference

Reference for anyone working on `parser.py` or ingestion logic.

**URL:** `https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp`

## Structure

- Classic ASP page, ~12 MB of HTML
- Three sections, each a `<table>` with `<th>` header containing "STATEWIDE ..."
- Records: key-value `<tr>` pairs with 2 `<td>` cells (label + value)
- Records separated by blank rows (2 cells, whitespace only)

## Field label quirks

| Record type | Field | Label used (vs. standard) |
|---|---|---|
| New application | Date | `Notification Date:` |
| Approved | Date | `Approved Date:` |
| Discontinued | Date | `Discontinued Date:` |
| ASSUMPTION | Business name | `Current Business Name:` / `New Business Name:` |
| ASSUMPTION | Applicants | `Current Applicant(s):` / `New Applicant(s):` |
| CHANGE OF LOCATION | Location | `Current Business Location:` / `New Business Location:` |
| CHANGE OF LOCATION | Application type | `\Application Type:` (**leading backslash**) |

- New applications include `Applicant(s):`; approved/discontinued do not
- ASSUMPTION records store current/new applicants; standard records use `Business Name:` / `Applicant(s):`
- CHANGE OF LOCATION stores locations via `previous_location_id` / `location_id` FKs

## License type formats

- Approved/discontinued (current): bare numeric code, e.g. `349,`
- Historical (pre-2025): `CODE, NAME` format, e.g. `450, GROCERY STORE - BEER/WINE`
- `process_record()` handles both formats

## Known issues

- Page carries a banner about "known data transfer issues" — expect occasional anomalies
- Source page itself contains duplicate records (especially approved/discontinued) — expected
