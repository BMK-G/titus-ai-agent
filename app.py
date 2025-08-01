from flask import Flask, request, send_file
import pandas as pd
import tempfile
import os

app = Flask(__name__)

@app.route("/process", methods=["POST"])
def process_excel():
    try:
        # Check if file was uploaded
        if 'data' not in request.files:
            return {"error": "No file uploaded under key 'data'"}, 400
        
        file = request.files['data']
        
        # Check if file is actually selected
        if file.filename == '':
            return {"error": "No file selected"}, 400
        
        # Read the Excel file
        df = pd.read_excel(file, sheet_name="Chart of Accounts Status", header=None)
        
        # Detect the header row dynamically - look for key identifying columns
        header_row = None
        for i in range(min(100, len(df))):
            row = df.iloc[i].astype(str).str.lower()
            # Look for customer/vendor columns and amount - more flexible than requiring 'code'
            if any("customer" in cell or "vendor" in cell for cell in row.values) and any("amount" in cell for cell in row.values):
                header_row = i
                break
        
        if header_row is None:
            return {"error": "Could not detect header row automatically. Looking for 'customer/vendor' and 'amount' columns."}, 400
        
        # Reset file pointer and read again with proper header
        file.stream.seek(0)
        df = pd.read_excel(file, sheet_name="Chart of Accounts Status", skiprows=header_row)
        
        # Clean column names
        df.columns = [str(col).strip().lower().replace(" ", "_").replace(".", "_") for col in df.columns]
        
        # Check what columns we actually have for debugging
        print(f"Available columns after cleaning: {df.columns.tolist()}")
        
        # More flexible column requirements - 'code' column might not exist
        required_cols = ['customer/vendor_code', 'customer/vendor_name', 'amount']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return {"error": f"Missing required columns: {missing_cols}. Available columns: {df.columns.tolist()}"}, 400
        
        # Since there's no 'code' column, we'll process all data (or you can add a different filter)
        # Remove the code filter since it doesn't exist in your data
        df_filtered = df[
            (~df['customer/vendor_name'].str.contains(r'\(usd\)', case=False, na=False)) &
            (df['amount'].notna())
        ].copy()
        
        # Check if any data remains after filtering
        if df_filtered.empty:
            return {"error": "No data found matching the filter criteria (non-USD, non-null amount)"}, 400
        
        # Select and rename columns
        result_df = df_filtered[['customer/vendor_code', 'customer/vendor_name', 'amount']].rename(columns={
            'customer/vendor_code': 'client_id',
            'customer/vendor_name': 'client_name',
            'amount': 'rmb_amount'
        }).reset_index(drop=True)
        
        # Create temporary file and write Excel
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            output_path = tmp.name
            result_df.to_excel(output_path, index=False, sheet_name="RMB_Report")
        
        # Clean up the temp file after sending (optional but good practice)
        def cleanup_file():
            try:
                os.unlink(output_path)
            except:
                pass
        
        return send_file(
            output_path, 
            as_attachment=True, 
            download_name="titus_cleaned_rmb.xlsx",
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
    except FileNotFoundError:
        return {"error": "Sheet 'Chart of Accounts Status' not found in the uploaded file"}, 400
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}, 500

# Health check endpoint (good practice for deployment)
@app.route("/health", methods=["GET"])
def health_check():
    return {"status": "healthy"}, 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
