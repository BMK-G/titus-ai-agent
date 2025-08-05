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
        debug_info = {
            'total_rows': len(df),
            'codes_found': set(),
            'client_info_samples': [],
            'rmb_clients_found': 0,
            'valid_entries': 0
        }

        for i, row in df.iterrows():
            if row.isna().all():
                continue

            code = str(row[0]).strip() if pd.notna(row[0]) else ""
            if code and code != 'nan':
                debug_info['codes_found'].add(code)

            client_info = str(row[1]).strip() if pd.notna(row[1]) else ""
            if client_info and client_info != 'nan' and len(debug_info['client_info_samples']) < 10:
                debug_info['client_info_samples'].append(f"Row {i}: '{client_info}'")

            if code not in ['240601', '110301']:
                continue
            if not client_info or client_info.lower() in ['nan', 'none', '']:
                continue

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

            debug_info['rmb_clients_found'] += 1
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

            debug_info['valid_entries'] += 1

            data.append({
                'client_id': client_id,
                'client_name': client_id,
                'code': code,
                'amount': amount,
                'type': 'receivables' if code == '240601' else 'orders'
            })

        if not data:
            return {
                "error": "No valid RMB entries found with codes 240601 or 110301",
                "debug": {
                    "total_rows_processed": debug_info['total_rows'],
                    "codes_found_in_column_A": list(debug_info['codes_found'])[:20],
                    "client_info_samples": debug_info['client_info_samples'],
                    "rmb_clients_found": debug_info['rmb_clients_found'],
                    "valid_entries_with_amounts": debug_info['valid_entries']
                }
            }, 400

        df_data = pd.DataFrame(data)

        pivot = df_data.pivot_table(
            index=['client_id', 'client_name'], 
            columns='type', 
            values='amount', 
            aggfunc='sum'
        ).fillna(0)

        result = pivot.reset_index()

        if 'receivables' not in result.columns:
            result['receivables'] = 0
        if 'orders' not in result.columns:
            result['orders'] = 0

        result['rmb_amount'] = result['receivables'] - result['orders']

        final_result = result[['client_id', 'client_name', 'receivables', 'orders', 'rmb_amount']].copy()

        only_full = request.args.get('only_full', 'true').lower() == 'true'

        if only_full:
            final_result = final_result[
                (final_result['receivables'] > 0) & (final_result['orders'] > 0)
            ].reset_index(drop=True)
            error_msg = "No clients found with both receivables AND orders data"
        else:
            final_result = final_result[
                (final_result['receivables'] != 0) | (final_result['orders'] != 0)
            ].reset_index(drop=True)
            error_msg = "No clients found with receivables or orders data"

        if final_result.empty:
            return {"error": error_msg}, 400

        final_result.rename(columns={
            'client_id': 'Client Code',
            'client_name': 'Client Name', 
            'receivables': 'Receivables (RMB)',
            'orders': 'Orders (RMB)',
            'rmb_amount': 'Net Receivable'
        }, inplace=True)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            output_path = tmp.name
            final_result.to_excel(output_path, index=False, sheet_name="RMB_Report")

        return send_file(
            output_path, 
            as_attachment=True, 
            download_name="titus_excel_cleaned_rmb.xlsx",
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except FileNotFoundError:
        return {"error": "Sheet 'Chart of Accounts Status' not found in the uploaded file"}, 400
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}, 500


@app.route("/health", methods=["GET"])
def health_check():
    return {"status": "healthy"}, 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)

    return {"status": "healthy"}, 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
