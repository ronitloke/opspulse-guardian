import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from pathlib import Path
from datetime import datetime
import uuid
import shutil

from api.schemas.models import RunRequest, RunResult, MetricsSummary
from api.adapters.hris import HRISAdapter
from api.services.checks import run_basic_quality_checks
from api.services.anomalies import run_hris_anomalies

app = FastAPI(title="OpsPulse Guardian", version="0.1.0")

UPLOAD_DIR = Path("api/storage/uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


@app.get("/health")
def health():
    return {"status": "ok"}


#@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    # Basic validation
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")

    ext = Path(file.filename).suffix.lower()
    if ext not in [".csv"]:
        raise HTTPException(status_code=400, detail="Only .csv files are supported for now")

    file_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_name = Path(file.filename).name.replace(" ", "_")
    saved_name = f"{timestamp}_{file_id}_{safe_name}"
    saved_path = UPLOAD_DIR / saved_name

    try:
        with saved_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {e}")

    return {
        "file_id": file_id,
        "filename": safe_name,
        "saved_path": str(saved_path),
    }

#@app.post("/run", response_model=RunResult)
def run(req: RunRequest):
    # 1) Find the uploaded file
    matches = list(UPLOAD_DIR.glob(f"*{req.file_id}*"))
    if not matches:
        raise HTTPException(status_code=404, detail="Uploaded file not found for this file_id")

    file_path = matches[0]

    # 2) Load CSV into pandas
    try:
        df = pd.read_csv(file_path, low_memory=False)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read CSV: {e}")

    # 3) Adapter selection (for now we implement HRIS first)
    if req.dataset_type != "hris":
        raise HTTPException(status_code=400, detail="Module 3B currently implemented for HRIS only. Pick dataset_type='hris'.")

    adapter = HRISAdapter()
    adapted = adapter.adapt(df)
    norm_df = adapted["df"]
    meta = adapted["meta"]

    # 4) Run checks + anomalies
    issues = run_basic_quality_checks(norm_df, meta)
    anomalies = run_hris_anomalies(norm_df, meta)

    # 5) Metrics summary
    metrics = MetricsSummary(
        row_count=int(norm_df.shape[0]),
        column_count=int(norm_df.shape[1]),
        key_metrics={}
    )

    return RunResult(
        dataset_type=req.dataset_type,
        file_id=req.file_id,
        metrics=metrics,
        issues=issues,
        anomalies=anomalies
    )