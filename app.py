from flask import Flask, request, send_file
import pandas as pd
import tempfile
import os

app = Flask(__name__)

@app.route("/process", methods=["POST"])
def process_excel():
    if 'data' not in request.files:
        return {"error": "No file uploaded under key 'data'"}, 400

    file = request.files['data']
    df = pd.read_excel(file, sheet_name="Chart of Accounts Status", header=None)

    header_row = None
    for i in range(min(100, len(df))):
        row = df.iloc[i].astype(str).str.lower()
        if any("code" in cell for cell in row.values) and any("amount" in cell for cell in row.values):
            header_row = i
            break

    if header_row is None:
        return {"error": "Could not detect header row automatically."}, 400

    file.stream.seek(0)
    df = pd.read_excel(file, sheet_name="Chart of Accounts Status", skiprows=header_row)
    df.columns = [str(col).strip().lower().replace(" ", "_") for col in df.columns]

    required_cols = ['code', 'customer/vendor_code', 'customer/vendor_name', 'amount']
    if not all(col in df.columns for col in required_cols):
        return {"error": f"Missing required columns in sheet: {df.columns.tolist()}"}, 400

    df = df[
        (df['code'].astype(str) == '240601') &
        (~df['customer/vendor_name'].str.contains(r'\(usd\)', case=False, na=False)) &
        (df['amount'].notna())
    ].copy()

    df = df[['customer/vendor_code', 'customer/vendor_name', 'amount']].rename(columns={
        'customer/vendor_code': 'client_id',
        'customer/vendor_name': 'client_name',
        'amount': 'rmb_amount'
    }).reset_index(drop=True)

    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        output_path = tmp.name
        df.to_excel(output_path, index=False, sheet_name="RMB_Report")

    return send_file(output_path, as_attachment=True, download_name="titus_cleaned_rmb.xlsx")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
