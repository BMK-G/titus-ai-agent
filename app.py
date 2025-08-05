from flask import Flask, request, send_file
import pandas as pd
import tempfile
import os

app = Flask(__name__)

@app.route("/process", methods=["POST"])
def process_excel():
    try:
        if 'data' not in request.files:
            return {"error": "No file uploaded under key 'data'"}, 400

        file = request.files['data']
        if file.filename == '':
            return {"error": "No file selected"}, 400

        df = pd.read_excel(file, sheet_name="Chart of Accounts Status", header=None)
        data = []
        current_code = None

        for i, row in df.iterrows():
            # Skip empty rows
            if row.isna().all():
                continue

            # Detect a title row with only 240601 or 110301
            values = row.dropna().astype(str).str.strip().tolist()
            if len(values) == 1 and values[0] in ['240601', '110301']:
                current_code = values[0]
                continue

            # If not under a valid section, skip
            if current_code not in ['240601', '110301']:
                continue

            client_info = str(row[1]).strip() if pd.notna(row[1]) else ""
            if not client_info or client_info.lower() in ['nan', 'none', '']:
                continue

            # Skip pure USD clients
            if 'usd' in client_info.lower() and 'rmb' not in client_info.lower():
                continue

            is_rmb_client = False
            client_id = client_info
            client_name = client_info

            if '(rmb)' in client_info.lower():
                is_rmb_client = True
                client_id = client_info.replace('(RMB)', '').replace('(rmb)', '').strip()
                client_name = client_id.replace('RMB', '').strip()
            elif client_info.lower().endswith('rmb'):
                is_rmb_client = True
                client_id = client_info
                client_name = client_info.replace('RMB', '').strip()
            elif 'rmb' in client_info.lower() and len(client_info) <= 10:
                is_rmb_client = True

            if not is_rmb_client:
                continue

            # Find amount: prioritize column G (index 6) but fallback to any valid number
            amount = None
            if len(row) > 6 and pd.notna(row[6]) and isinstance(row[6], (int, float)) and row[6] != 0:
                amount = float(row[6])
            else:
                for j in range(2, len(row)):
                    cell_value = row[j]
                    if pd.notna(cell_value) and isinstance(cell_value, (int, float)) and cell_value != 0:
                        amount = float(cell_value)
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

        df_data = pd.DataFrame(data)
        pivot = df_data.pivot_table(
            index=['client_id', 'client_name'],
            columns='type',
            values='amount',
            aggfunc='sum'
        ).fillna(0).reset_index()

        if 'receivables' not in pivot.columns:
            pivot['receivables'] = 0
        if 'orders' not in pivot.columns:
            pivot['orders'] = 0

        pivot['rmb_amount'] = pivot['receivables'] - pivot['orders']

        result = pivot[['client_id', 'client_name', 'receivables', 'orders', 'rmb_amount']]
        only_full = request.args.get('only_full', 'true').lower() == 'true'

        if only_full:
            result = result[(result['receivables'] > 0) & (result['orders'] > 0)].reset_index(drop=True)
            error_msg = "No clients found with both receivables AND orders"
        else:
            result = result[(result['receivables'] != 0) | (result['orders'] != 0)].reset_index(drop=True)
            error_msg = "No clients found with receivables or orders"

        if result.empty:
            return {"error": error_msg}, 400

        result.rename(columns={
            'client_id': 'Client Code',
            'client_name': 'Client Name',
            'receivables': 'Receivables (RMB)',
            'orders': 'Orders (RMB)',
            'rmb_amount': 'Net Receivable'
        }, inplace=True)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            output_path = tmp.name
            result.to_excel(output_path, index=False, sheet_name="RMB_Report")

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
    return {"status": "healthy"}, 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)

