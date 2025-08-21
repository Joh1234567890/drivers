import pandas as pd
import re
import json
from fuzzywuzzy import process, fuzz
from datetime import datetime
import pytz

# == CONFIG ==
DRIVER_JSON_PATH = "estates.drivers.live.json"
EXCEL_PATH = "JOB CARD CURRENT.xlsx"   # <-- updated filename here
JSON_OUTPUT_PATH = "driver_loan_output.json"
JS_OUTPUT_PATH = "driver_loan_output.js"
MAPPING_XLSX_PATH = "driver_name_matches.xlsx"
MISSING_DRIVERS_TXT = "missing_drivers.txt"
CREATED_BY = "System"
CREATED_BY_ID = "111111111111111111111111"
CATEGORY = "loan"
KIND = "drivers"
TYPE_INCOME = "income"
TYPE_EXPENSE = "expense"
TZ = pytz.timezone("Africa/Dar_es_Salaam")
DEFAULT_DATE = "2025-07-01T00:00:00.000Z"

# == LOAD DRIVER DATA ==
with open(DRIVER_JSON_PATH, "r", encoding="utf-8") as f:
    drivers_json = json.load(f)
driver_lookup = {d['name'].strip().lower(): d['_id']['$oid'] for d in drivers_json}
driver_names = list(driver_lookup.keys())

# == LOAD EXCEL DATA ==
df = pd.read_excel(EXCEL_PATH, sheet_name=0, header=None)

date_pattern = r"^(\d{2})[.\-/](\d{2})[.\-/](\d{2,4})$"
current_driver = None
current_driver_id = None

json_objects = []
js_objects = []
driver_match_map = []
missing_drivers = set()

# Modified fuzzy matching function with first name 80% + full name 60%
def get_driver_id_fuzzy(driver_name, driver_lookup, first_name_threshold=70, full_name_threshold=50):
    driver_name_lower = driver_name.lower().replace(" ", "").strip()
    input_first_name = driver_name_lower.split()[0] if driver_name_lower else ""

    best_match = None
    best_full_score = 0

    for candidate in driver_lookup.keys():
        candidate_lower = candidate.lower().replace(" ", "").strip()
        candidate_first_name = candidate_lower.split()[0] if candidate_lower else ""

        # Check first name similarity
        first_name_score = fuzz.ratio(input_first_name, candidate_first_name)
        
        if first_name_score >= first_name_threshold:
            # Check full name similarity only if first names match enough
            full_name_score = fuzz.ratio(driver_name_lower, candidate_lower)
            if full_name_score >= full_name_threshold and full_name_score > best_full_score:
                best_match = candidate
                best_full_score = full_name_score

    if best_match:
        return driver_lookup[best_match], best_match, best_full_score
    else:
        return None, None, 0

def parse_date_cell(val):
    match = re.match(date_pattern, str(val).strip())
    if match:
        dd, mm, yy = match.groups()
        if len(yy) == 2:
            yy = "20" + yy
        try:
            # Create datetime object in Tanzania timezone
            dt = datetime(int(yy), int(mm), int(dd), tzinfo=TZ)
            return dt.replace(microsecond=0).isoformat()
        except ValueError:
            return None
    return None

def now_tz_iso():
    # Return ISO format with timezone offset (e.g., 2025-08-21T12:34:56+03:00)
    return datetime.now(TZ).replace(microsecond=0).isoformat()

def is_number(val):
    if pd.isna(val) or val is None:
        return False
    try:
        float(str(val).replace(",", "").replace(" ", ""))
        return True
    except:
        return False

def parse_number(val):
    try:
        return float(str(val).replace(",", "").replace(" ", ""))
    except:
        return 0

def extract_date_from_description(description):
    # Try to find a date at the end, or in the middle, with formats like dd.mm.yy, dd-mm-yyyy, dd/mm/yy, etc.
    date_regex = r'(\d{2})[.\-/](\d{2})[.\-/](\d{2,4})'
    matches = list(re.finditer(date_regex, description))
    if matches:
        last_match = matches[-1]
        dd, mm, yy = last_match.groups()
        if len(yy) == 2:
            yy = "20" + yy
        try:
            # Create datetime object in Tanzania timezone
            dt = datetime(int(yy), int(mm), int(dd), tzinfo=TZ)
            clean_desc = (description[:last_match.start()]).rstrip(" -").strip()
            return dt.replace(microsecond=0).isoformat(), clean_desc
        except ValueError:
            pass
    # If no valid date found, use DEFAULT_DATE but convert to Tanzania timezone
    try:
        y, m, d = map(int, DEFAULT_DATE[:10].split('-'))
        dt = datetime(y, m, d, tzinfo=TZ)
        return dt.replace(microsecond=0).isoformat(), description.strip()
    except:
        return DEFAULT_DATE, description.strip()

def record_transaction(driver_name, driver_id, date_iso, description, amount, tx_type):
    # Find the driver's name by itemID (driver_id)
    item_name = None
    for name, _id in driver_lookup.items():
        if _id == driver_id:
            item_name = name
            break
    # Capitalize item_name (all uppercase)
    item_name_cap = (item_name if item_name else driver_name).upper()
    obj = {
        "itemID": driver_id,
        "itemName": item_name_cap,
        "date": date_iso,
        "description": description,
        "amount": amount,
        "type": tx_type,
        "kind": KIND,
        "category": CATEGORY,
        "createdBy": CREATED_BY,
        "createdByID": CREATED_BY_ID,
        "createdAt": now_tz_iso()
    }
    json_objects.append(obj)
    js_objects.append(
        f'{{driver: "{driver_name}", itemID: ObjectId("{driver_id}"), itemName: "{item_name_cap}", date: new Date("{date_iso}"), description: "{description}", amount: {amount}, type: "{tx_type}", kind: "{KIND}", category: "{CATEGORY}", createdBy: "{CREATED_BY}", createdByID: ObjectId("{CREATED_BY_ID}"), createdAt: new Date("{obj["createdAt"]}")}}'
    )

# == MAIN PROCESSING ==
for idx, row in df.iterrows():
    # Detect driver name row (third column not empty, rest empty/nan)
    if (
        isinstance(row[2], str)
        and row[2].strip() != ""
        and all(pd.isna(row[c]) for c in range(3, len(row)))
    ):
        current_driver = row[2].strip()
        current_driver_id, matched_name, score = get_driver_id_fuzzy(current_driver, driver_lookup)
        driver_match_map.append({"excel": current_driver, "json": matched_name if matched_name else "NOT FOUND", "score": score})
        if not current_driver_id:
            missing_drivers.add(current_driver.lower())
        continue

    if current_driver_id:
        # DATE-ONLY ROW: treat as income for salary and/or mileage
        date_iso = parse_date_cell(row[2])
        salary = row[3] if len(row) > 3 else None
        mileage = row[4] if len(row) > 4 else None
        balance = row[5] if len(row) > 5 else None

        if date_iso:
            if is_number(salary):
                record_transaction(current_driver, current_driver_id, date_iso, "salary", parse_number(salary), TYPE_INCOME)
            if is_number(mileage):
                record_transaction(current_driver, current_driver_id, date_iso, "mileage", parse_number(mileage), TYPE_INCOME)
            continue

        # DEBT ROW: if third column is not a date, treat as expense
        description = row[2]
        if isinstance(description, str) and description.strip() != "" and not parse_date_cell(description):
            if is_number(balance):
                expense_date, desc_cleaned = extract_date_from_description(description)
                record_transaction(current_driver, current_driver_id, expense_date, desc_cleaned, parse_number(balance), TYPE_EXPENSE)

# == EXPORT OUTPUTS ==
with open(JSON_OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(json_objects, f, indent=2, ensure_ascii=False)

with open(JS_OUTPUT_PATH, "w", encoding="utf-8") as f:
    f.write("[\n" + ",\n".join(js_objects) + "\n]\n")

pd.DataFrame(driver_match_map).to_excel(MAPPING_XLSX_PATH, index=False)

with open(MISSING_DRIVERS_TXT, "w", encoding="utf-8") as f:
    for d in sorted(missing_drivers):
        f.write(d + "\n")

print("\n= PROCESSING COMPLETE =")
print(f"Exported {len(json_objects)} transaction objects.")
print(f"Exported JS and JSON to: {JS_OUTPUT_PATH}, {JSON_OUTPUT_PATH}")
print(f"Exported driver name matches to: {MAPPING_XLSX_PATH}")
print(f"Exported missing drivers list to: {MISSING_DRIVERS_TXT}")
if missing_drivers:
    print(f"\nDrivers not found in JSON (fuzzy < thresholds):\n{', '.join(missing_drivers)}")
else:
    print("All drivers were matched.")
