from flask import Flask, request, send_file, jsonify, make_response
import pandas as pd
import io
import os
import re
import logging
from flask_cors import CORS
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("rmb_processing.log")
    ]
)
logger = logging.getLogger(__name__)

# ---------------- App ----------------
app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024

# Enable CORS for local + hosted UI
ui_origin = os.getenv("UI_ORIGIN", "*")
CORS(
    app,
    resources={r"/*": {"origins": [
        ui_origin,
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:5500"
    ]}},
    methods=["GET", "POST", "OPTIONS"],
    expose_headers=["Content-Disposition"]
)

# ---------------- Constants ----------------
SECTION_CODES = {"receivables": "240601", "orders": "110301"}
EXCHANGE_RATE = 7.10
DEFAULT_SHEET = "Chart of Accounts Status"
FILE_KEY = "data"
RMB_TAG_RE = re.compile(r"\(rmb\)|\brmb\b", flags=re.IGNORECASE)
CREDIT_LIMIT_RE = re.compile(r"credit\s*limit", flags=re.IGNORECASE)

# ---------------- Data Models ----------------
@dataclass
class ProcessingStats:
    no_rmb: int = 0
    no_amount: int = 0
    invalid_client: int = 0

@dataclass
class ClientData:
    client_id: str
    client_name: str
    code: str
    amount: float
    type: str

# ---------------- Processor ----------------
class ExcelProcessor:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.df_str = df.fillna("").astype(str).applymap(lambda x: str(x).strip().lower())
        self.stats = ProcessingStats()

    def find_section_indices(self, code: str) -> pd.Index:
        joined_rows = self.df_str.apply(" ".join, axis=1)
        return self.df[joined_rows.str.contains(code, na=False)].index

    def _coerce_numeric_series(self, s: pd.Series) -> pd.Series:
        s_clean = (
            s.astype(str)
             .str.replace(r"[\u00A0\s]", "", regex=True)
             .str.replace(",", "", regex=False)
             .str.replace(r"[\(\)]", "", regex=True)
             .str.replace(r"[^\d\.\-\+eE]", "", regex=True)
        )
        return pd.to_numeric(s_clean, errors="coerce")

    def extract_amount(self, row: pd.Series) -> Optional[float]:
        numeric_vals = self._coerce_numeric_series(row.iloc[2:])
        valid_vals = numeric_vals[(numeric_vals.notna()) & (numeric_vals != 0)]
        return float(valid_vals.iloc[0]) if not valid_vals.empty else None

    def clean_client_info(self, client_info: str) -> Tuple[str, str]:
        client_info = RMB_TAG_RE.sub("", client_info).strip()
        return client_info, client_info

    def process_section(self, section_type: str, start_indices: pd.Index) -> List[ClientData]:
        data: List[ClientData] = []
        code = SECTION_CODES[section_type]

        for start_idx in start_indices:
            remaining_df_str = self.df_str.iloc[start_idx + 1:]
            next_section_mask = remaining_df_str.apply(
                lambda row: any(c in " ".join(row) for c in SECTION_CODES.values()),
                axis=1
            )
            end_idx = next_section_mask.idxmax() if next_section_mask.any() else len(self.df)
            section_data = self.df.iloc[start_idx + 1:end_idx]

            rmb_mask = section_data.iloc[:, 1].astype(str).str.contains(r"rmb", case=False, na=False)
            rmb_rows = section_data[rmb_mask]
            self.stats.no_rmb += len(section_data) - len(rmb_rows)

            for _, row in rmb_rows.iterrows():
                client_info_raw = str(row.iloc[1]).strip()
                if not client_info_raw or client_info_raw.lower() == "nan":
                    self.stats.invalid_client += 1
                    continue

                amount = self.extract_amount(row)
                if amount is None or amount == 0:
                    self.stats.no_amount += 1
                    continue

                client_id, client_name = self.clean_client_info(client_info_raw)
                data.append(ClientData(client_id, client_name, code, amount, section_type))

        return data

    def extract_credit_limits(self) -> Dict[str, float]:
        credit_limits: Dict[str, float] = {}
        has_credit_limit = self.df_str.apply(
            lambda row: any(CREDIT_LIMIT_RE.search(str(cell)) for cell in row),
            axis=1
        )
        credit_rows = self.df[has_credit_limit]

        for _, row in credit_rows.iterrows():
            try:
                name = str(row.iloc[1]).strip()
                if not RMB_TAG_RE.search(name):
                    continue
                cleaned_name = RMB_TAG_RE.sub("", name).strip()
                amount = self.extract_amount(row)
                if amount is not None:
                    credit_limits[cleaned_name] = float(amount)
            except Exception as e:
                logger.warning(f"Credit limit extraction failed: {e}")

        return credit_limits

    def process(self) -> Tuple[List[ClientData], Dict[str, float], ProcessingStats]:
        all_data: List[ClientData] = []
        for section_type in SECTION_CODES:
            indices = self.find_section_indices(SECTION_CODES[section_type])
            all_data.extend(self.process_section(section_type, indices))
        credit_limits = self.extract_credit_limits()
        return all_data, credit_limits, self.stats

# ---------------- Writer ----------------
def dataframe_to_xlsx_bytes(result_df: pd.DataFrame) -> io.BytesIO:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        result_df.to_excel(writer, sheet_name="RMB_Report", index=False)
        workbook = writer.book
        worksheet = writer.sheets["RMB_Report"]
        num_fmt = workbook.add_format({"num_format": "#,##0.00"})
        worksheet.set_column("A:A", 14)
        worksheet.set_column("B:B", 28)
        worksheet.set_column("C:G", 14, num_fmt)
    output.seek(0)
    return output

# ---------------- Helpers ----------------
def load_excel_file(file) -> pd.DataFrame:
    try:
        return pd.read_excel(file, sheet_name=DEFAULT_SHEET, header=None, engine="openpyxl")
    except Exception:
        logger.info(f"Sheet '{DEFAULT_SHEET}' not found, using first sheet")
        return pd.read_excel(file, sheet_name=0, header=None, engine="openpyxl")

def create_result_dataframe(
    data: List[ClientData],
    credit_limits: Dict[str, float],
    only_full: bool = True
) -> pd.DataFrame:
    if not data:
        raise ValueError("No valid data found")

    df = pd.DataFrame([{
        "client_id": d.client_id,
        "client_name": d.client_name,
        "type": d.type,
        "amount": d.amount
    } for d in data])

    pivot = df.pivot_table(
        index=["client_id", "client_name"],
        columns="type",
        values="amount",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    pivot["total_rmb"] = pivot.get("receivables", 0) - pivot.get("orders", 0)
    pivot["usd_equivalent"] = (pivot["total_rmb"] / EXCHANGE_RATE).round(2)
    pivot["credit_limit"] = pivot["client_name"].map(credit_limits).apply(
        lambda x: f"{x:.2f}" if isinstance(x, (int, float)) else ""
    )

    if only_full:
        pivot = pivot[(pivot.get("receivables", 0) > 0) & (pivot.get("orders", 0) > 0)]
    else:
        pivot = pivot[(pivot.get("receivables", 0) != 0) | (pivot.get("orders", 0) != 0)]

    if pivot.empty:
        raise ValueError("No clients found matching criteria")

    return pivot.rename(columns={
        "client_id": "Client Code",
        "client_name": "Client Name",
        "receivables": "Receivables (RMB)",
        "orders": "Orders (RMB)",
        "total_rmb": "Total (RMB)",
        "usd_equivalent": "USD Equivalent",
        "credit_limit": "Credit Limit"
    })

# ---------------- Routes ----------------
@app.route("/", methods=["GET"])
def root():
    return jsonify({"message": "✅ Titus AI Agent is running. Use /process to upload Excel files."}), 200

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy"}), 200

@app.route("/process", methods=["POST"])
def process_excel():
    try:
        if FILE_KEY not in request.files or not request.files[FILE_KEY].filename:
            return jsonify({"error": f"No valid file uploaded under key '{FILE_KEY}'"}), 400

        file = request.files[FILE_KEY]
        df = load_excel_file(file).dropna(how="all").reset_index(drop=True)
        logger.info(f"/process received file; shape={df.shape}")

        processor = ExcelProcessor(df)
        data, credit_limits, stats = processor.process()
        if not data:
            return jsonify({"error": "No valid RMB entries found.", "debug": vars(stats)}), 400

        only_full = request.args.get("only_full", "true").strip().lower() == "true"
        result_df = create_result_dataframe(data, credit_limits, only_full)
        xlsx_bytes = dataframe_to_xlsx_bytes(result_df)

        response = make_response(send_file(
            xlsx_bytes,
            as_attachment=True,
            download_name="titus_excel_cleaned_rmb.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ))
        response.headers["Cache-Control"] = "no-store"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    except ValueError as ve:
        logger.warning(f"/process ValueError: {ve}")
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        logger.exception("Unhandled error in process_excel:")
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

# ---------------- Entrypoint ----------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
