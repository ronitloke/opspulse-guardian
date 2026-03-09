import pandas as pd

# This adapter standardizes the HRIS dataset into a shape we can run checks on.
# It does NOT change the data; it just creates a consistent "view".
class HRISAdapter:
    REQUIRED_COLUMNS = ["employee_id"]

    # Candidate date columns in synthetic HR datasets vary, so we try a few.
    DATE_COLUMNS_CANDIDATES = ["hire_date", "start_date", "date_of_hire", "employment_start_date"]

    STATUS_COLUMNS_CANDIDATES = ["employment_status", "status"]

    COUNTRY_COLUMNS_CANDIDATES = ["country", "work_country", "location_country"]

    def adapt(self, df: pd.DataFrame) -> dict:
        """
        Returns:
          {
            "df": normalized_df,
            "meta": { "id_col": "...", "date_col": "...", "status_col": "...", "country_col": "..." }
          }
        """
        df = df.copy()

        # Normalize column names: lower-case, strip spaces
        df.columns = [c.strip().lower() for c in df.columns]

        id_col = "employee_id" if "employee_id" in df.columns else None
        date_col = next((c for c in self.DATE_COLUMNS_CANDIDATES if c in df.columns), None)
        status_col = next((c for c in self.STATUS_COLUMNS_CANDIDATES if c in df.columns), None)
        country_col = next((c for c in self.COUNTRY_COLUMNS_CANDIDATES if c in df.columns), None)

        meta = {
            "id_col": id_col,
            "date_col": date_col,
            "status_col": status_col,
            "country_col": country_col,
        }

        return {"df": df, "meta": meta}