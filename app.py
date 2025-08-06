from flask import Flask, request, send_file, jsonify
import pandas as pd
import tempfile
import os

app = Flask(__name__)

@app.route("/process", methods=["POST"])
def process_excel():
    try:
        # 1. Validate File Upload
        if 'data' not in request.files or request.files['data'].filename == '':
            return {"error": "No valid file uploaded under key 'data'"}, 400

        file = request.files['data']

        # Try reading specific sheet first, fallback to first sheet
        try:
            df = pd.read_excel(file, sheet_name="Chart of Accounts Status", header=None)
        except:
            df = pd.read_excel(file, sheet_name=0, header=None)

        # 2. Pre-filter rows to reduce iterations
        df = df.dropna(how='all').reset_index(drop=True)

        col_b_str = df[1].astype(str).str.strip().str.lower()

        rmb_mask = (
            col_b_str.str.contains('rmb', na=False) | 
            col_b_str.str.contains(r'\(rmb\)', na=False)
        )
        usd_only_mask = (
            col_b_str.str.contains('usd', na=False) & 
            ~col_b_str.str.contains('rmb', na=False)
        )

        candidate_rows = df[rmb_mask & ~usd_only_mask]

        data = []

        # 3. Iterate only candidate rows
        for idx, row in candidate_rows.iterrows():
            client_info = str(row[1]).strip()
            if not client_info or client_info.lower() == 'nan':
                continue

            client_info_lower = client_info.lower()

            if '(rmb)' in client_info_lower:
                client_id = client_info.replace('(RMB)', '').replace('(rmb)', '').strip()
                client_name = client_id.replace('RMB', '').strip()
            elif client_info_lower.endswith('rmb'):
                client_id = client_info.strip()
                client_name = client_id.replace('RMB', '').strip()
            elif 'rmb' in client_info_lower and len(client_info) <= 10:
                client_id = client_info
                client_name = client_info
            else:
                continue

            numeric_cols = row[2:].select_dtypes(include=['number'])
            if numeric_cols.empty:
                continue

            non_zero_amounts = numeric_cols[numeric_cols != 0]
            if non_zero_amounts.empty:
                continue

            amount = float(non_zero_amounts.iloc[0])
            code = str(row[0]).strip() if pd.notna(row[0]) else "Unknown"

            data.append({
                'client_id': client_id,
                'client_name': client_name,
                'code': code,
                'amount': amount,
                'type': 'receivables' if code == '240601' else 'orders'
            })

        if not data:
            return {"error": "No valid RMB entries found."}, 400

        # 4. Optimized pivot with single operation
        df_data = pd.DataFrame(data)

        pivot = df_data.groupby(['client_id', 'client_name', 'type'])['amount'].sum().unstack(fill_value=0).reset_index()

        for col in ['receivables', 'orders']:
            if col not in pivot.columns:
                pivot[col] = 0

        # 5. Vectorized calculations
        pivot['usd_equivalent'] = (pivot['orders'] / 7.10).round(2)
        pivot['credit_limit'] = ""
        pivot['credit_limit'] = pivot['credit_limit'].astype(str)

        # 6. Single filtering operation
        only_full = request.args.get('only_full', 'true').lower() == 'true'

        if only_full:
            result = pivot[(pivot['receivables'] > 0) & (pivot['orders'] > 0)].copy()
            error_msg = "No clients found with both receivables AND orders"
        else:
            result = pivot[(pivot['receivables'] != 0) | (pivot['orders'] != 0)].copy()
            error_msg = "No clients found with receivables or orders"

        if result.empty:
            return {"error": error_msg}, 400

        # ✅ Console debug log (summary of results)
        print(f"✅ Processed {len(result)} RMB clients. Receivables: {result['Receivables (RMB)'].sum():.2f}, Orders: {result['Orders (RMB)'].sum():.2f}")

        # 7. Select and rename columns
        result = result[['client_id', 'client_name', 'receivables', 'orders', 'usd_equivalent', 'credit_limit']].rename(columns={
            'client_id': 'Client Code',
            'client_name': 'Client Name',
            'receivables': 'Receivables (RMB)',
            'orders': 'Orders (RMB)',
            'usd_equivalent': 'USD Equivalent',
            'credit_limit': 'Credit Limit'
        })

        # 8. Excel writing
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            output_path = tmp.name
            with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
                result.to_excel(writer, sheet_name="RMB_Report", index=False)

        return send_file(
            output_path,
            as_attachment=True,
            download_name="titus_excel_cleaned_rmb.xlsx",
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}, 500


@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy"}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
