from flask import Flask, request, send_file, jsonify
import pandas as pd
import tempfile
import os
import re
import logging
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from contextlib import contextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('rmb_processing.log')
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Constants
SECTION_CODES = {'receivables': '240601', 'orders': '110301'}
EXCHANGE_RATE = 7.10
DEFAULT_SHEET = "Chart of Accounts Status"
FILE_KEY = 'data'

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

class ExcelProcessor:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.df_str = df.astype(str).applymap(lambda x: str(x).strip().lower())
        self.stats = ProcessingStats()
        
    def find_section_indices(self, code: str) -> pd.Index:
        joined = self.df_str.apply(lambda row: ' '.join(row), axis=1)
        return self.df[joined.str.contains(code, na=False)].index
    
    def extract_amount(self, row: pd.Series) -> Optional[float]:
        try:
            numeric_vals = pd.to_numeric(row.iloc[2:], errors='coerce')
            non_zero = numeric_vals[numeric_vals != 0].dropna()
            return float(non_zero.iloc[0]) if len(non_zero) > 0 else None
        except (IndexError, ValueError):
            return None
    
    def clean_client_info(self, client_info: str) -> Tuple[str, str]:
        client_info = client_info.strip()
        client_id = re.sub(r'\(rmb\)', '', client_info, flags=re.IGNORECASE).strip()
        client_name = re.sub(r'rmb', '', client_id, flags=re.IGNORECASE).strip()
        return client_id, client_name
    
    def process_section(self, section_type: str, start_indices: pd.Index) -> List[ClientData]:
        data = []
        code = SECTION_CODES[section_type]
        
        for start_idx in start_indices:
            remaining_df = self.df_str.iloc[start_idx + 1:]
            next_section_mask = remaining_df.apply(
                lambda row: any(section_code in ' '.join(row) 
                              for section_code in SECTION_CODES.values()), axis=1
            )
            end_idx = next_section_mask.idxmax() if next_section_mask.any() else len(self.df)
            section_data = self.df.iloc[start_idx + 1:end_idx]
            
            rmb_mask = section_data.iloc[:, 1].astype(str).str.lower().str.contains('rmb', na=False)
            rmb_rows = section_data[rmb_mask]
            self.stats.no_rmb += len(section_data) - len(rmb_rows)
            
            for idx, row in rmb_rows.iterrows():
                client_info = str(row.iloc[1]).strip()
                if not client_info or client_info.lower() == 'nan':
                    self.stats.invalid_client += 1
                    continue
                amount = self.extract_amount(row)
                if amount is None or amount == 0:
                    self.stats.no_amount += 1
                    continue
                client_id, client_name = self.clean_client_info(client_info)
                data.append(ClientData(
                    client_id=client_id,
                    client_name=client_name,
                    code=code,
                    amount=amount,
                    type=section_type
                ))
        return data
    
    def extract_credit_limits(self) -> Dict[str, float]:
        credit_limits = {}
        credit_mask = self.df_str.apply(
            lambda row: any('credit limit' in cell for cell in row), axis=1
        )
        credit_rows = self.df[credit_mask]
        
        for _, row in credit_rows.iterrows():
            try:
                potential_name = str(row.iloc[1]).strip()
                if 'rmb' not in potential_name.lower():
                    continue
                cleaned_name = re.sub(r'rmb|\(rmb\)', '', potential_name, flags=re.IGNORECASE).strip()
                amount = self.extract_amount(row)
                if amount is not None and amount > 0:
                    credit_limits[cleaned_name] = amount
            except Exception as e:
                logger.warning(f"Credit limit extraction failed: {e}")
        return credit_limits
    
    def process(self) -> Tuple[List[ClientData], Dict[str, float], ProcessingStats]:
        all_data = []
        for section_type in SECTION_CODES:
            indices = self.find_section_indices(SECTION_CODES[section_type])
            section_data = self.process_section(section_type, indices)
            all_data.extend(section_data)
        credit_limits = self.extract_credit_limits()
        return all_data, credit_limits, self.stats

@contextmanager
def create_temp_excel(result_df: pd.DataFrame):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        output_path = tmp.name
    try:
        with pd.ExcelWriter(output_path, engine="xlsxwriter", options={'strings_to_numbers': True}) as writer:
            result_df.to_excel(writer, sheet_name="RMB_Report", index=False)
            workbook = writer.book
            worksheet = writer.sheets['RMB_Report']
            currency_format = workbook.add_format({'num_format': '#,##0.00'})
            worksheet.set_column('C:F', 12, currency_format)
        yield output_path
    finally:
        if os.path.exists(output_path):
            try:
                os.unlink(output_path)
            except OSError:
                pass

def load_excel_file(file) -> pd.DataFrame:
    try:
        return pd.read_excel(file, sheet_name=DEFAULT_SHEET, header=None)
    except Exception:
        logger.info(f"Sheet '{DEFAULT_SHEET}' not found, using first sheet")
        return pd.read_excel(file, sheet_name=0, header=None)

def create_result_dataframe(data: List[ClientData], credit_limits: Dict[str, float], only_full: bool = True) -> pd.DataFrame:
    if not data:
        raise ValueError("No valid data found")
    df_data = pd.DataFrame([
        {
            'client_id': item.client_id,
            'client_name': item.client_name,
            'type': item.type,
            'amount': item.amount
        } for item in data
    ])
    pivot = (df_data.groupby(['client_id', 'client_name', 'type'])['amount']
             .sum().unstack(fill_value=0).reset_index())
    for col in ['receivables', 'orders']:
        if col not in pivot.columns:
            pivot[col] = 0
    pivot['total_rmb'] = pivot['receivables'] - pivot['orders']
    pivot['usd_equivalent'] = (pivot['total_rmb'] / EXCHANGE_RATE).round(2)
    pivot['credit_limit'] = pivot['client_name'].map(credit_limits).fillna('')
    pivot['credit_limit'] = pivot['credit_limit'].apply(lambda x: f"{x:.2f}" if pd.notna(x) and x != 0 else "")
    if only_full:
        result = pivot[(pivot['receivables'] > 0) & (pivot['orders'] > 0)]
    else:
        result = pivot[(pivot['receivables'] != 0) | (pivot['orders'] != 0)]
    if result.empty:
        raise ValueError("No clients found matching criteria")
    column_mapping = {
        'client_id': 'Client Code',
        'client_name': 'Client Name',
        'receivables': 'Receivables (RMB)',
        'orders': 'Orders (RMB)',
        'total_rmb': 'Total (RMB)',
        'usd_equivalent': 'USD Equivalent',
        'credit_limit': 'Credit Limit'
    }
    return result[list(column_mapping.keys())].rename(columns=column_mapping)

@app.route("/", methods=["GET"])
def root():
    return jsonify({
        "message": "✅ Titus AI Agent is running. Use the /process route (POST) to upload Excel files."
    }), 200

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy"}), 200

@app.route("/process", methods=["POST"])
def process_excel():
    try:
        if FILE_KEY not in request.files or request.files[FILE_KEY].filename == '':
            return jsonify({"error": f"No valid file uploaded under key '{FILE_KEY}'"}), 400
        file = request.files[FILE_KEY]
        df = load_excel_file(file)
        df = df.dropna(how='all').reset_index(drop=True)
        processor = ExcelProcessor(df)
        data, credit_limits, stats = processor.process()
        if not data:
            return jsonify({
                "error": "No valid RMB entries found.",
                "debug": {
                    "no_rmb": stats.no_rmb,
                    "no_amount": stats.no_amount,
                    "invalid_client": stats.invalid_client
                }
            }), 400
        only_full = request.args.get('only_full', 'true').lower() == 'true'
        result_df = create_result_dataframe(data, credit_limits, only_full)
        with create_temp_excel(result_df) as output_path:
            return send_file(
                output_path,
                as_attachment=True,
                download_name="titus_excel_cleaned_rmb.xlsx",
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.exception("Unhandled error in process_excel:")
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)

