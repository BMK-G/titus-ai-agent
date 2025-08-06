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
        df = pd.read_excel(file, sheet_name="Chart of Accounts Status", header=None)

        data = []
        current_code = None

        # 2. Parse and extract data based on section titles (240601 or 110301)
        for _, row in df.iterrows():
            if row.isna().all():
                continue

            # Detect section headers like "240601"
            non_na_values = row.dropna().astype(str).str.strip().tolist()
            if len(non_na_values) == 1 and non_na_values[0] in ['240601', '110301']:
                current_code = non_na_values[0]
                continue

            if current_code not in ['240601', '110301']:
                continue

            # Extract and clean client info
            client_info = str(row[1]).strip() if pd.notna(row[1]) else ""
            if not client_info or client_info.lower() in ['nan', 'none']:
                continue

            if 'usd' in client_info.lower() and 'rmb' not in client_info.lower():
                continue  # USD client, skip

            # Detect RMB clients
            is_rmb_client = False
            if '(rmb)' in client_info.lower():
                is_rmb_client = True
                client_id = client_info.replace('(RMB)', '').replace('(rmb)', '').strip()
                client_name = client_id.replace('RMB', '').strip()
            elif client_info.lower().endswith('rmb'):
                is_rmb_client = True
                client_id = client_info.strip()
                client_name = client_id.replace('RMB', '').strip()
            elif 'rmb' in client_info.lower() and len(client_info) <= 10:
                is_rmb_client = True
                client_id = client_info
                client_name = client_info

            if not is_rmb_client:
                continue

            # Extract amount (search from column C onward)
            amount = None
            for j in range(2, len(row)):
                val = row[j]
                if pd.notna(val) and isinstance(val, (int, float)) and val != 0:
                    amount = float(val)
                    break

            if amount is None:
                continue

            data.append({
                'client_id': client_id,
                'client_name': client_name,
                'code': current_code,
                'amount': amount,
                'type': 'receivables' if current_code == '240601' else 'orders'
            })

        if not data:
            return {"error": "No valid RMB entries found."}, 400

        # 3. Pivot and compute totals
        df_data = pd.DataFrame(data)
        pivot = df_data.pivot_table(
            index=['client_id', 'client_name'],
            columns='type',
            values='amount',
            aggfunc='sum'
        ).fillna(0).reset_index()

        pivot['receivables'] = pivot.get('receivables', 0)
        pivot['orders'] = pivot.get('orders', 0)
        pivot['rmb_amount'] = pivot['receivables'] - pivot['orders']

        result = pivot[['client_id', 'client_name', 'receivables', 'orders', 'rmb_amount']]

        # 4. Filter based on query param
        only_full = request.args.get('only_full', 'true').lower() == 'true'
        if only_full:
            result = result[(result['receivables'] > 0) & (result['orders'] > 0)]
            if result.empty:
                return {"error": "No clients found with both receivables AND orders"}, 400
        else:
            result = result[(result['receivables'] != 0) | (result['orders'] != 0)]
            if result.empty:
                return {"error": "No clients found with receivables or orders"}, 400

        # 5. Rename columns
        result.rename(columns={
            'client_id': 'Client Code',
            'client_name': 'Client Name',
            'receivables': 'Receivables (RMB)',
            'orders': 'Orders (RMB)',
            'rmb_amount': 'Net Receivable'
        }, inplace=True)

        # 6. Write to Excel file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            output_path = tmp.name
            result.to_excel(output_path, index=False, sheet_name="RMB_Report", engine="xlsxwriter")

        return send_file(
            output_path,
            as_attachment=True,
            download_name="titus_excel_cleaned_rmb.xlsx",
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except FileNotFoundError:
        return {"error": "Sheet 'Chart of Accounts Status' not found"}, 400
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}, 500


@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy"}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
