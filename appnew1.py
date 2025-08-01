from flask import Flask, request, send_file
import pandas as pd

app = Flask(__name__)

@app.route('/process', methods=['POST'])
def process_excel():
    uploaded_file = request.files['file']
    input_path = 'input.xlsx'
    output_path = 'output.xlsx'
    
    uploaded_file.save(input_path)

    xls = pd.ExcelFile(input_path)
    df_all = pd.concat([xls.parse(sheet) for sheet in xls.sheet_names], ignore_index=True)

    df_all.dropna(how='all', inplace=True)
    df_all.dropna(axis=1, how='all', inplace=True)
    df_all.columns = df_all.columns.str.strip().str.lower().str.replace(" ", "_")

    df_all['code'] = df_all['code'].astype(str)
    df_all['currency'] = df_all['currency'].str.upper().str.strip()
    df_all['amount'] = pd.to_numeric(df_all['amount'], errors='coerce')

    filtered = df_all[df_all['code'].isin(['240601', '110301'])].copy()
    summary = filtered.groupby(['client_id', 'code', 'currency'])['amount'].sum().unstack(fill_value=0).reset_index()

    results = []

    for (client, currency), group in summary.groupby(['client_id', 'currency']):
        receivable = group.get(240601, 0.0) if 240601 in group else 0.0
        order = group.get(110301, 0.0) if 110301 in group else 0.0
        net = receivable - order
        usd_equiv = net / 7.25 if currency == 'RMB' else 0.0
        status = 'Neutral'
        if currency == 'RMB':
            if net > 0:
                status = 'Positive'
            elif net < 0:
                status = 'Negative'

        results.append({
            'Client ID': client,
            'Currency': currency,
            'Receivables': receivable,
            'Orders': order,
            'Net': net,
            'USD Equivalent': usd_equiv,
            'Status': status if currency == 'RMB' else ''
        })

    df_summary = pd.DataFrame(results)
    rmb_df = df_summary[df_summary['Currency'] == 'RMB'].drop(columns=['Currency'])
    usd_df = df_summary[df_summary['Currency'] == 'USD'][['Client ID', 'Net']].rename(columns={'Net': 'Total USD'})
    final_df = pd.merge(rmb_df, usd_df, how='left', on='Client ID')
    final_df = final_df[['Client ID', 'Receivables', 'Orders', 'Net', 'USD Equivalent', 'Status', 'Total USD']]
    final_df.rename(columns={
        'Receivables': 'Receivables (RMB)',
        'Orders': 'Orders (RMB)',
        'Net': 'Net RMB'
    }, inplace=True)
    final_df.sort_values(by='Net RMB', ascending=False, inplace=True)

    final_df.to_excel(output_path, sheet_name='Summary', index=False)
    return send_file(output_path, as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
