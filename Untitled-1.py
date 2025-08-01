
from flask import Flask, request, send_file
import pandas as pd
import os
from datetime import datetime

app = Flask(__name__)

@app.route('/process', methods=['POST'])
def process_excel():
    uploaded_file = request.files['file']
    input_path = 'input.xlsx'
    output_path = 'output.xlsx'
    
    uploaded_file.save(input_path)

    # Load & clean Excel
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
        status = 'Neutral'
        if currency == 'RMB':
            status = 'Positive' if net > 0 else 'Negative' if net < 0 else 'Neutral'
        results.append({
            'Client ID': client,
            'Currency': currency,
            'Receivables': receivable,
            'Orders': order,
            'Net': net,
            'Status': status if currency == 'RMB' else ''
        })

    df_summary = pd.DataFrame(results)
    rmb_df = df_summary[df_summary['Currency'] == 'RMB'].drop(columns=['Currency'])
    usd_df = df_summary[df_summary_]()
