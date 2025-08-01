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
    df_raw = pd.read_excel(file, sheet_name="Chart of Accounts Status", header=None)

    # Extract rows that look like valid data (non-null and numeric "Amount")
    df = df_raw.copy()
    df.columns = range(df.shape[1])  # reset column indices just in case

    # Filter out rows where 'Amount' column is numeric
    df = df[df[6].apply(lambda x: isinstance(x, (int, float)))]

    # Filter out USD accounts
    df = df[~df[1].astype(str).str.contains(r'\(usd\)', case=False, na=False)]

    # Drop rows with missing essential values
    df = df[df[0].notna() & df[1].notna() & df[6].notna()]

    # Build final dataframe
    df_final = df[[0, 1, 6]].copy()
    df_final.columns = ['client_id', 'client_name', 'rmb_amount']

    # Save to Excel
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        output_path = tmp.name
        df_final.to_excel(output_path, index=False, sheet_name="RMB_Report")

    return send_file(output_path, as_attachment=True, download_name="titus_cleaned_rmb.xlsx")

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
