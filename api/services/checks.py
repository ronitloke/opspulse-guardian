import pandas as pd
from typing import List, Dict, Any
from api.schemas.models import Issue

SENSITIVE_KEYS = {"email", "phone", "mobile_phone", "date_of_birth", "zip_code", "address"}

def mask_row(r: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in r.items():
        if k and k.strip().lower() in SENSITIVE_KEYS:
            out[k] = "***"
        else:
            out[k] = v
    return out

def sample_rows(df: pd.DataFrame, mask: pd.Series, n: int = 3) -> List[Dict[str, Any]]:
    """
    Returns up to n sample rows as dicts for debugging in UI,
    with sensitive fields masked.
    """
    try:
        rows = df.loc[mask].head(n).to_dict(orient="records")
        return [mask_row(r) for r in rows]
    except Exception:
        return []


def run_basic_quality_checks(df: pd.DataFrame, meta: dict) -> List[Issue]:
    issues: List[Issue] = []

    # 1) Required column: employee_id
    if not meta.get("id_col"):
        issues.append(Issue(
            code="MISSING_EMPLOYEE_ID",
            severity="high",
            message="Required column 'employee_id' not found (after normalization).",
        ))
        # If no id column, many other checks don't make sense
        return issues

    id_col = meta["id_col"]

# 2) Duplicate employee IDs
    dup_mask = df[id_col].duplicated(keep=False) & df[id_col].notna()
    dup_row_count = int(dup_mask.sum())

    if dup_row_count > 0:
        dup_ids = df.loc[dup_mask, id_col]
        dup_unique = int(dup_ids.nunique())

        # How many times each duplicated ID appears
        counts = dup_ids.value_counts()
        max_repeats = int(counts.max())
        avg_repeats = float(counts.mean())

        issues.append(Issue(
            code="DUPLICATE_EMPLOYEE_ID",
            severity="high",
            message=(
                f"{dup_unique} employee_id values are duplicated. "
                f"Total rows affected: {dup_row_count}. "
                f"Avg repeats per duplicated ID: {avg_repeats:.2f}, max repeats: {max_repeats}."
            ),
            count=dup_row_count,
            sample_rows=sample_rows(df, dup_mask)
        ))
    unique_ids = int(df[id_col].nunique(dropna=True))
    total_rows = int(len(df))
    ratio = unique_ids / total_rows if total_rows else 0

    if ratio < 0.98:
        issues.append(Issue(
            code="LOW_EMPLOYEE_ID_UNIQUENESS",
            severity="medium",
            message=f"employee_id uniqueness is {unique_ids}/{total_rows} ({ratio:.1%}).",
            count=total_rows - unique_ids
        ))

    # 3) Null employee IDs
    null_id_mask = df[id_col].isna() | (df[id_col].astype(str).str.strip() == "")
    null_id_count = int(null_id_mask.sum())
    if null_id_count > 0:
        issues.append(Issue(
            code="NULL_EMPLOYEE_ID",
            severity="high",
            message="Some rows have empty/NULL employee_id.",
            count=null_id_count,
            sample_rows=sample_rows(df, null_id_mask)
        ))

    # 4) Date column parsing (if we found a date column)
    date_col = meta.get("date_col")
    if date_col:
        parsed = pd.to_datetime(df[date_col], errors="coerce", infer_datetime_format=True)
        invalid_date_mask = parsed.isna() & df[date_col].notna()
        invalid_count = int(invalid_date_mask.sum())
        if invalid_count > 0:
            issues.append(Issue(
                code="INVALID_HIRE_DATE",
                severity="medium",
                message=f"Found values in '{date_col}' that could not be parsed as dates.",
                count=invalid_count,
                sample_rows=sample_rows(df, invalid_date_mask)
            ))
    else:
        issues.append(Issue(
            code="MISSING_DATE_COLUMN",
            severity="medium",
            message="No hire/start date column found. (Tried: hire_date/start_date/date_of_hire/...)",
        ))

    # 5) Null rate check for important columns (if present)
    for col_name, code in [
        (meta.get("status_col"), "MISSING_STATUS"),
        (meta.get("country_col"), "MISSING_COUNTRY")
    ]:
        if col_name and col_name in df.columns:
            null_rate = float(df[col_name].isna().mean())
            if null_rate > 0.2:
                issues.append(Issue(
                    code=code,
                    severity="low",
                    message=f"High null rate in '{col_name}' ({null_rate:.0%}).",
                    count=int(df[col_name].isna().sum())
                ))

    return issues