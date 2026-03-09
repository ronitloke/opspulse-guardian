import pandas as pd
from typing import List
from api.schemas.models import Anomaly


def run_hris_anomalies(df: pd.DataFrame, meta: dict) -> List[Anomaly]:
    anomalies: List[Anomaly] = []

    date_col = meta.get("date_col")
    if not date_col:
        return anomalies

    parsed = pd.to_datetime(df[date_col], errors="coerce", infer_datetime_format=True)
    tmp = df.copy()
    tmp["_hire_date"] = parsed
    tmp = tmp[tmp["_hire_date"].notna()]

    if tmp.empty:
        return anomalies

    # Group by month of hire
    tmp["_month"] = tmp["_hire_date"].dt.to_period("M").astype(str)
    hires_per_month = tmp.groupby("_month").size().sort_index()

    if len(hires_per_month) < 3:
        return anomalies

    # Simple anomaly: last month vs median
    last_month = hires_per_month.index[-1]
    last_value = float(hires_per_month.iloc[-1])
    baseline = float(hires_per_month.median())

    if baseline > 0 and last_value > baseline * 2.0:
        anomalies.append(Anomaly(
            metric="HIRES_SPIKE",
            bucket=last_month,
            value=last_value,
            baseline=baseline,
            severity="medium",
            explanation=f"Hires in {last_month} ({int(last_value)}) are >2x the median monthly hires ({int(baseline)})."
        ))

    return anomalies