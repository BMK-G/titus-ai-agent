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
        
        # Read the Excel file without headers first
        df = pd.read_excel(file, sheet_name="Chart of Accounts Status", header=None)
        
        # Process data row by row to handle the messy structure
        data = []
        debug_info = {
            'total_rows': len(df),
            'codes_found': set(),
            'client_info_samples': [],
            'rmb_clients_found': 0,
            'valid_entries': 0
        }
        
        for i, row in df.iterrows():
            # Skip empty rows
            if row.isna().all():
                continue
                
            # Column A should contain the code
            code = str(row[0]).strip() if pd.notna(row[0]) else ""
            if code and code != 'nan':
                debug_info['codes_found'].add(code)
            
            # Column B should contain client name/ID
            client_info = str(row[1]).strip() if pd.notna(row[1]) else ""
            if client_info and client_info != 'nan' and len(debug_info['client_info_samples']) < 10:
                debug_info['client_info_samples'].append(f"Row {i}: '{client_info}'")
            
            # Only process codes 240601 (receivables) and 110301 (orders)
            if code not in ['240601', '110301']:
                continue
            if not client_info or client_info.lower() in ['nan', 'none', '']:
                continue
            
            # Skip purely USD clients, but allow mixed currency mentions
            if 'usd' in client_info.lower() and 'rmb' not in client_info.lower():
                continue  # Skip ONLY if it's purely USD and not RMB
            
            # Detect if this is an RMB client
            is_rmb_client = False
            client_id = client_info
            client_name = client_info
            
            # Method 1: Check for (RMB) in the name/code
            if '(rmb)' in client_info.lower():
                is_rmb_client = True
                client_id = client_info.replace('(RMB)', '').replace('(rmb)', '').strip()
                client_name = client_id.replace('RMB', '').strip()  # Even cleaner for display
            
            # Method 2: Check if client code ends with RMB (like JKMRMB)
            elif client_info.lower().endswith('rmb'):
                is_rmb_client = True
                client_id = client_info  # Keep as is for codes like JKMRMB
                client_name = client_info.replace('RMB', '').strip()  # Remove RMB for cleaner display
            
            # Method 3: Contains 'rmb' but be more specific to avoid false positives
            elif 'rmb' in client_info.lower() and len(client_info) <= 10:  # Likely a code, not a description
                is_rmb_client = True
            
            # Skip non-RMB clients
            if not is_rmb_client:
                continue
            
            debug_info['rmb_clients_found'] += 1
            
            # Find the amount - prioritize column G (index 6) if it exists
            amount = None
            
            # First try column G (common location for amounts)
            if len(row) > 6 and pd.notna(row[6]) and isinstance(row[6], (int, float)) and row[6] != 0:
                amount = float(row[6])
            else:
                # Fallback: scan all columns after B for the first valid amount
                for j in range(2, len(row)):
                    cell_value = row[j]
                    if pd.notna(cell_value) and isinstance(cell_value, (int, float)) and cell_value != 0:
                        amount = float(cell_value)
                        break
            
            # Skip if no valid amount found
            if amount is None:
                continue
            
            debug_info['valid_entries'] += 1
            
            # Add to data
            data.append({
                'client_id': client_id,
                'client_name': client_id,  # Using same value for both
                'code': code,
                'amount': amount,
                'type': 'receivables' if code == '240601' else 'orders'
            })
        
        # Check if we found any data
        if not data:
            return {
                "error": "No valid RMB entries found with codes 240601 or 110301",
                "debug": {
                    "total_rows_processed": debug_info['total_rows'],
                    "codes_found_in_column_A": list(debug_info['codes_found'])[:20],  # First 20 codes
                    "client_info_samples": debug_info['client_info_samples'],
                    "rmb_clients_found": debug_info['rmb_clients_found'],
                    "valid_entries_with_amounts": debug_info['valid_entries']
                }
            }, 400
        
        # Convert to DataFrame
        df_data = pd.DataFrame(data)
        
        # Group by client and pivot to get receivables and orders
        pivot = df_data.pivot_table(
            index=['client_id', 'client_name'], 
            columns='type', 
            values='amount', 
            aggfunc='sum'
        ).fillna(0)
        
        # Reset index to make client_id and client_name regular columns
        result = pivot.reset_index()
        
        # Ensure we have both receivables and orders columns
        if 'receivables' not in result.columns:
            result['receivables'] = 0
        if 'orders' not in result.columns:
            result['orders'] = 0
        
        # Calculate net RMB amount
        result['rmb_amount'] = result['receivables'] - result['orders']
        
        # Select and order final columns
        final_result = result[['client_id', 'client_name', 'receivables', 'orders', 'rmb_amount']].copy()
        
        # Get user preference for filtering (default: only clients with both)
        only_full = request.args.get('only_full', 'true').lower() == 'true'
        
        if only_full:
            # Only include clients who appear in BOTH receivables AND orders
            final_result = final_result[
                (final_result['receivables'] > 0) & (final_result['orders'] > 0)
            ].reset_index(drop=True)
            error_msg = "No clients found with both receivables AND orders data"
        else:
            # Include clients with either receivables OR orders
            final_result = final_result[
                (final_result['receivables'] != 0) | (final_result['orders'] != 0)
            ].reset_index(drop=True)
            error_msg = "No clients found with receivables or orders data"
        
        if final_result.empty:
            return {"error": error_msg}, 400
        
        # Rename columns to match professional format
        final_result.rename(columns={
            'client_id': 'Client Code',
            'client_name': 'Client Name', 
            'receivables': 'Receivables (RMB)',
            'orders': 'Orders (RMB)',
            'rmb_amount': 'Net Receivable'
        }, inplace=True)
        
        # Create temporary file and write Excel
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

# Health check endpoint
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
