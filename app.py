from flask import Flask, request, send_file, jsonify
import pandas as pd
import tempfile
import os
import re
import logging
from flask_cors import CORS
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from contextlib import contextmanager

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler('rmb_processing.log')]
)
logger = logging.getLogger(__name__)

# ---------------- App ----------------
app = Flask(__name__)

# Allow uploads up to 16MB (adjust if needed)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

# CORS: allow your static UI + localhost for testing
ui_origin = os.getenv("UI_ORIGIN", "*")  # e.g. set to "https://titus-ui.onrender.com" in Render env if you want to restrict
CORS(
    app,
    resources={r"/*": {"origins": [ui_origin, "http://localhost:3000", "http://localhost:5173", "http://127.0.0.1:5500"]}},
    methods=["GET", "POST", "OPTIONS"],
    expose_headers=["Content-Disposition"]
)

# ---------------- Constants ----------------
SECTION_CODES = {'receivables': '240601', 'orders': '110301'}
EXCHANGE_RATE = 7.10
DEFAULT_SHEET = "Chart of Accounts Status"
FILE_KEY = 'data'

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

# ---------------- Core Processor ----------------
class ExcelProcessor:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.df_str = df.fillna('').astype(str).applymap(lambda x: str(x).strip().lower())
        self.stats = ProcessingStats()

    def find_section_indices(self, code: str) -> pd.Index:
        joined_rows = self.df_str.apply(' '.join, axis=1)
        return self.df[joined_rows.str.contains(code, na=False)].index

    def extract_amount(self, row: pd.Series) -> Optional[float]:
        numeric_vals = pd.to_numeric(row.iloc[2:], errors='coerce')
        valid_vals = numeric_vals[numeric_vals != 0].dropna()
        return float(valid_vals.iloc[0]) if not valid_vals.empty else None

    def clean_client_info(self, client_info: str) -> Tuple[str, str]:
        client_info = re.sub(r'\(rmb\)|rmb', '', client_info, flags=re.IGNORECASE).strip()
        return client_info, client_info

    def process_section(self, section_type: str, start_indices: pd.Index) -> List[ClientData]:
        data = []
        code = SECTION_CODES[section_type]

        for start_idx in start_indices:
            remaining_df = self.df_str.iloc[start_idx + 1:]
            next_section_mask = remaining_df.apply(
                lambda row: any(c in ' '.join(row) for c in SECTION_CODES.values()), axis=1
            )
            end_idx = next_section_mask.idxmax() if next_section_mask.any() else len(self.df)
            section_data = self.df.iloc[start_idx + 1:end_idx]

            rmb_rows = section_data[section_data.iloc[:, 1].astype(str).str.lower().str.contains('rmb', na=False)]
            self.stats.no_rmb += len(section_data) - len(rmb_rows)

            for _, row in rmb_rows.iterrows():
                client_info = str(row.iloc[1]).strip()
                if not client_info or client_info.lower() == 'nan':
                    self.stats.invalid_client += 1
                    continue

                amount = self.extract_amount(row)
                if amount is None or amount == 0:
                    self.stats.no_amount += 1
                    continue

                client_id, client_name = self.clean_client_info(client_info)
                data.append(ClientData(client_id, client_name, code, amount, section_type))

        return data

    def extract_credit_limits(self) -> Dict[str, float]:
        credit_limits = {}
        credit_rows = self.df[self.df_str.apply(lambda row: any('credit limit' in cell for cell in row), axis=1)]

        for _, row in credit_rows.iterrows():
            try:
                name = str(row.iloc[1]).strip()
                if 'rmb' not in name.lower():
                    continue
                cleaned_name = re.sub(r'\(rmb\)|rmb', '', name, flags=re.IGNORECASE).strip()
                amount = self.extract_amount(row)
                if amount:
                    credit_limits[cleaned_name] = amount
            except Exception as e:
                logger.warning(f"Credit limit extraction failed: {e}")

        return credit_limits

    def process(self) -> Tuple[List[ClientData], Dict[str, float], ProcessingStats]:
        all_data = []
        for section_type, code in SECTION_CODES.items():
            indices = self.find_section_indices(code)
            all_data.extend(self.process_section(section_type, indices))
        credit_limits = self.extract_credit_limits()
        return all_data, credit_limits, self.stats

# ---------------- Excel Writer ----------------
@contextmanager
def create_temp_excel(result_df: pd.DataFrame):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        output_path = tmp.name
    try:
        with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
            result_df.to_excel(writer, sheet_name="RMB_Report", index=False)
            workbook = writer.book
            worksheet = writer.sheets['RMB_Report']
            fmt = workbook.add_format({'num_format': '#,##0.00'})
            worksheet.set_column('C:G', 12, fmt)
        yield output_path
    finally:
        if os.path.exists(output_path):
            try:
                os.unlink(output_path)
            except OSError:
                pass

# ---------------- Helpers ----------------
def load_excel_file(file) -> pd.DataFrame:
    try:
        # Be explicit about engine for xlsx
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
        'client_id': d.client_id,
        'client_name': d.client_name,
        'type': d.type,
        'amount': d.amount
    } for d in data])

    pivot = df.pivot_table(
        index=['client_id', 'client_name'],
        columns='type',
        values='amount',
        aggfunc='sum',
        fill_value=0
    ).reset_index()

    pivot['total_rmb'] = pivot.get('receivables', 0) - pivot.get('orders', 0)
    pivot['usd_equivalent'] = (pivot['total_rmb'] / EXCHANGE_RATE).round(2)
    pivot['credit_limit'] = pivot['client_name'].map(credit_limits).apply(
        lambda x: f"{x:.2f}" if isinstance(x, (int, float)) else ''
    )

    if only_full:
        pivot = pivot[(pivot.get('receivables', 0) > 0) & (pivot.get('orders', 0) > 0)]
    else:
        pivot = pivot[(pivot.get('receivables', 0) != 0) | (pivot.get('orders', 0) != 0)]

    if pivot.empty:
        raise ValueError("No clients found matching criteria")

    return pivot.rename(columns={
        'client_id': 'Client Code',
        'client_name': 'Client Name',
        'receivables': 'Receivables (RMB)',
        'orders': 'Orders (RMB)',
        'total_rmb': 'Total (RMB)',
        'usd_equivalent': 'USD Equivalent',
        'credit_limit': 'Credit Limit'
    })

# ---------------- Routes ----------------
@app.route("/", methods=["GET"])
def root():
    return jsonify({"message": "✅ Titus AI Agent is running. Use the /process route (POST) to upload Excel files."}), 200

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy"}), 200

@app.route("/process", methods=["POST"])
def process_excel():
    try:
        if FILE_KEY not in request.files or not request.files[FILE_KEY].filename:
            return jsonify({"error": f"No valid file uploaded under key '{FILE_KEY}'"}), 400

        file = request.files[FILE_KEY]
        df = load_excel_file(file).dropna(how='all').reset_index(drop=True)
        logger.info(f"/process received file; shape={df.shape}")

        processor = ExcelProcessor(df)
        data, credit_limits, stats = processor.process()

        if not data:
            return jsonify({
                "error": "No valid RMB entries found.",
                "debug": vars(stats)
            }), 400

        only_full = request.args.get('only_full', 'true').lower() == 'true'
        result_df = create_result_dataframe(data, credit_limits, only_full)

        with create_temp_excel(result_df) as output_path:
            # Flask will set Content-Disposition; CORS exposes it for the browser
            return send_file(
                output_path,
                as_attachment=True,
                download_name="titus_excel_cleaned_rmb.xlsx",
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )

    except ValueError as ve:
        logger.warning(f"/process ValueError: {ve}")
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        logger.exception("Unhandled error in process_excel:")
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

# ---------------- Entrypoint ----------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    # For local dev debug=True is fine. On Render use gunicorn start command instead.
    app.run(host="0.0.0.0", port=port, debug=False)
