from flask import Flask, request, send_file, jsonify
import pandas as pd
import tempfile
import os
import re
import logging

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

@app.route("/process", methods=["POST"])
def process_excel():
    try:
        if 'data' not in request.files or request.files['data'].filename == '':
            return {"error": "No valid file uploaded under key 'data'"}, 400

        file = request.files['data']

        try:
            df = pd.read_excel(file, sheet_name="Chart of Accounts Status", header=None)
        except:
            df = pd.read_excel(file, sheet_name=0, header=None)

        df = df.dropna(how='all').reset_index(drop=True)
        df_str = df.astype(str).apply(lambda x: x.str.lower().str.strip())

        section_240601 = df_str.apply(lambda row: '240601' in ' '.join(row), axis=1)
        section_110301 = df_str.apply(lambda row: '110301' in ' '.join(row), axis=1)

        receivables_start = df[section_240601].index
        orders_start = df[section_110301].index

        data = []
        credit_limits = {}
        skipped_rows = {'no_rmb': 0, 'no_amount': 0, 'invalid_client': 0}

        for section_type, section_indices in [('receivables', receivables_start), ('orders', orders_start)]:
            for start_idx in section_indices:
                remaining_df = df_str.iloc[start_idx+1:]
                next_section_mask = remaining_df.apply(
                    lambda row: any(code in ' '.join(row) for code in ['240601', '110301']), axis=1
                )
                if next_section_mask.any():
                    end_idx = next_section_mask.idxmax()
                else:
                    end_idx = len(df)

                section_data = df.iloc[start_idx+1:end_idx]
                logger.info(f"Processing {section_type} section: rows {start_idx+1} to {end_idx} ({len(section_data)} rows)")

                col_b_str = section_data[1].astype(str).str.lower()
                rmb_mask = col_b_str.str.contains('rmb', na=False)
                rmb_rows = section_data[rmb_mask]

                non_rmb_count = len(section_data) - len(rmb_rows)
                if non_rmb_count > 0:
                    skipped_rows['no_rmb'] += non_rmb_count

                for idx, row in rmb_rows.iterrows():
                    client_info = str(row[1]).strip()
                    if not client_info or client_info.lower() == 'nan':
                        skipped_rows['invalid_client'] += 1
                        continue

                    client_id = re.sub(r'\(rmb\)', '', client_info, flags=re.IGNORECASE).strip()
                    client_name = re.sub(r'rmb', '', client_id, flags=re.IGNORECASE).strip()

                    try:
                        numeric_vals = pd.to_numeric(row[2:], errors='coerce')
                        non_zero_amounts = numeric_vals[numeric_vals != 0].dropna()
                        if non_zero_amounts.empty:
                            skipped_rows['no_amount'] += 1
                            continue
                        amount = float(non_zero_amounts.iat[0])
                        if amount == 0:
                            skipped_rows['no_amount'] += 1
                            continue
                    except (IndexError, ValueError, TypeError):
                        skipped_rows['no_amount'] += 1
                        continue

                    data.append({
                        'client_id': client_id,
                        'client_name': client_name,
                        'code': '240601' if section_type == 'receivables' else '110301',
                        'amount': amount,
                        'type': section_type
                    })

        credit_limit_mask = df_str.apply(lambda row: any('credit limit' in cell for cell in row), axis=1)
        credit_limit_rows = df[credit_limit_mask]

        for idx, row in credit_limit_rows.iterrows():
            try:
                potential_name = str(row[1]).strip()
                if 'rmb' not in potential_name.lower():
                    continue
                cleaned_name = re.sub(r'rmb|\(rmb\)', '', potential_name, flags=re.IGNORECASE).strip()
                numeric_vals = pd.to_numeric(row[2:], errors='coerce')
                non_zero_amounts = numeric_vals[numeric_vals != 0].dropna()
                if len(non_zero_amounts) > 0:
                    credit_limits[cleaned_name] = float(non_zero_amounts.iat[0])
            except (IndexError, ValueError, TypeError):
                continue

        if not data:
            logger.error(f"No valid entries found. Skipped: {skipped_rows}")
            return {"error": "No valid RMB entries found.", "debug": skipped_rows}, 400

        logger.info(f"Successfully extracted {len(data)} entries. Skipped rows: {skipped_rows}")
        df_data = pd.DataFrame(data)

        pivot = df_data.groupby(['client_id', 'client_name', 'type'])['amount'].sum().unstack(fill_value=0)
        missing_cols = set(['receivables', 'orders']) - set(pivot.columns)
        for col in missing_cols:
            pivot[col] = 0
        pivot = pivot.reset_index()

        # ➕ NEW: Add total column and USD equivalent
        pivot['total'] = (pivot['receivables'] - pivot['orders']).round(2)
        pivot['usd_equivalent'] = (pivot['total'] / 7.10).round(2)

        pivot['credit_limit'] = pivot['client_name'].map(credit_limits)
        pivot['credit_limit'] = pivot['credit_limit'].apply(lambda x: f"{x:.2f}" if pd.notna(x) and x != 0 else "")

        only_full = request.args.get('only_full', 'true').lower() == 'true'
        if only_full:
            result = pivot[(pivot['receivables'] > 0) & (pivot['orders'] > 0)]
            error_msg = "No clients found with both receivables AND orders"
        else:
            result = pivot[(pivot['receivables'] != 0) | (pivot['orders'] != 0)]
            error_msg = "No clients found with receivables or orders"

        if result.empty:
            return {"error": error_msg}, 400

        total_receivables = result['receivables'].sum()
        total_orders = result['orders'].sum()
        net_amount = total_receivables - total_orders
        logger.info(f"✅ Processed {len(result)} RMB clients | Receivables: {total_receivables:.2f} | Orders: {total_orders:.2f} | Net: {net_amount:.2f}")

        column_mapping = {
            'client_id': 'Client Code',
            'client_name': 'Client Name',
            'receivables': 'Receivables (RMB)',
            'orders': 'Orders (RMB)',
            'total': 'Total (RMB)',  # ✅ NEW COLUMN
            'usd_equivalent': 'USD Equivalent',
            'credit_limit': 'Credit Limit'
        }

        result = result[list(column_mapping.keys())].rename(columns=column_mapping)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            output_path = tmp.name
            with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
                result.to_excel(writer, sheet_name="RMB_Report", index=False)
                workbook = writer.book
                worksheet = writer.sheets['RMB_Report']
                currency_format = workbook.add_format({'num_format': '#,##0.00'})
                worksheet.set_column('C:F', 14, currency_format)  # Currency columns

        return send_file(
            output_path,
            as_attachment=True,
            download_name="titus_excel_cleaned_rmb.xlsx",
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        logger.exception("Unhandled error:")
        return {"error": f"An error occurred: {str(e)}"}, 500

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy"}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)

