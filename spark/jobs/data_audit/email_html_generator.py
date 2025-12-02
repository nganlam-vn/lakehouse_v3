from datetime import datetime


def generate_email_body_html(html_table_preview, record_count, config_count, dimension_count, html_filename):
    """
    Generate HTML email body with summary and preview table
    
    Args:
        html_table_preview: HTML string with preview tables
        record_count: Total violation records
        config_count: Number of configurations with issues
        dimension_count: Number of dimensions affected
        html_filename: Name of attachment file
    
    Returns:
        Complete HTML email body as string
    """
    return f"""
    <html>
      <head>
        <style>
          body {{
            font-family: Arial, sans-serif;
            margin: 20px;
          }}
          h2 {{
            color: #d9534f;
          }}
          .summary {{
            background-color: #f8d7da;
            border: 1px solid #f5c6cb;
            border-radius: 4px;
            padding: 15px;
            margin-bottom: 20px;
          }}
          .config-section {{
            background-color: white;
            border: 2px solid #5bc0de;
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 25px;
          }}
          .config-section h3 {{
            margin-top: 0;
            color: #5bc0de;
          }}
          .config-info {{
            background-color: #e8f4f8;
            padding: 10px;
            border-radius: 4px;
            margin-bottom: 15px;
            font-size: 14px;
          }}
          .violation-count {{
            color: #d9534f;
            font-weight: bold;
            font-size: 16px;
          }}
          .audit-table {{
            border-collapse: collapse;
            width: 100%;
            margin-top: 10px;
          }}
          .audit-table th {{
            background-color: #5bc0de;
            color: white;
            padding: 10px;
            text-align: left;
            font-weight: bold;
            font-size: 13px;
          }}
          .audit-table td {{
            border: 1px solid #ddd;
            padding: 8px;
            font-size: 12px;
          }}
          .audit-table tr:nth-child(even) {{
            background-color: #f2f2f2;
          }}
          .audit-table tr:hover {{
            background-color: #e9ecef;
          }}
          .notice {{
            background-color: #fff3cd;
            border: 1px solid #ffeaa7;
            border-radius: 4px;
            padding: 10px;
            margin: 20px 0;
            color: #856404;
          }}
          .footer {{
            margin-top: 30px;
            font-size: 12px;
            color: #6c757d;
          }}
        </style>
      </head>
      <body>
        <h2>⚠️ Data Quality Alert</h2>
        
        <div class="summary">
          <strong>Summary:</strong><br>
          <ul>
            <li><strong>Total Failed Audits:</strong> {record_count}</li>
            <li><strong>Configurations with Issues:</strong> {config_count}</li>
            <li><strong>Dimensions Affected:</strong> {dimension_count}</li>
            <li><strong>Alert Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</li>
          </ul>
        </div>
        
        <h3>📊 Failed Audits by Configuration (Preview - Max 5 per config):</h3>
        {html_table_preview}
        
        <div class="notice">
          <strong>📎 Note:</strong> Showing maximum 5 violations per configuration. 
          <strong>Please open the attached HTML file ({html_filename}) to view all {record_count} violations.</strong>
        </div>
        
        <div class="footer">
          <p>This is an automated alert from the Data Quality Monitoring System.</p>
          <p>For questions, please contact the Data Engineering team.</p>
          <p>Lam Hoai Kim Ngan: lamhoaikimngan@gmail.com | Le Hoang Anh Duy: anhduy0969@gmail.com</p>
        </div>
      </body>
    </html>
    """


def generate_file_html(total_records, config_count, id_configs_sections):
    """
    Generate complete standalone HTML file for attachment
    
    Args:
        total_records: Total number of violations
        config_count: Number of configurations
        id_configs_sections: HTML string with all configuration sections
    
    Returns:
        Complete HTML document as string
    """
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Data Quality Audit Report</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 20px;
                background-color: #f5f5f5;
            }}
            h1 {{
                color: #d9534f;
                text-align: center;
            }}
            .info {{
                background-color: #fff;
                border: 1px solid #ddd;
                border-radius: 4px;
                padding: 15px;
                margin-bottom: 20px;
            }}
            .config-section {{
                background-color: white;
                border: 2px solid #5bc0de;
                border-radius: 8px;
                padding: 20px;
                margin-bottom: 30px;
            }}
            .config-section h3 {{
                margin-top: 0;
                color: #5bc0de;
            }}
            .config-info {{
                background-color: #e8f4f8;
                padding: 10px;
                border-radius: 4px;
                margin-bottom: 15px;
            }}
            .violation-count {{
                color: #d9534f;
                font-weight: bold;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                background-color: white;
            }}
            th {{
                background-color: #5bc0de;
                color: white;
                padding: 12px;
                text-align: left;
                font-weight: bold;
                position: sticky;
                top: 0;
                z-index: 10;
            }}
            td {{
                border: 1px solid #ddd;
                padding: 10px;
                word-wrap: break-word;
            }}
            tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
            tr:hover {{
                background-color: #e9ecef;
            }}
            .footer {{
                margin-top: 20px;
                text-align: center;
                font-size: 12px;
                color: #6c757d;
            }}
        </style>
    </head>
    <body>
        <h1>⚠️ Data Quality Audit Report</h1>
        
        <div class="info">
            <strong>Report Details:</strong><br>
            <ul>
                <li><strong>Total Failed Audits:</strong> {total_records}</li>
                <li><strong>Configurations with Issues:</strong> {config_count}</li>
                <li><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</li>
                <li><strong>Source:</strong> dataaudit.fact_dataaudit_* tables</li>
            </ul>
        </div>
        
        {id_configs_sections}
        
        <div class="footer">
            <p>This is an automated report from the Data Quality Monitoring System.</p>
            <p>For questions, please contact the Data Engineering team.</p>
            <p>Lam Hoai Kim Ngan: lamhoaikimngan@gmail.com | Le Hoang Anh Duy: anhduy0969@gmail.com</p>
        </div>
    </body>
    </html>
    """


def generate_config_section_html(id_config, dimension, schema, table, rule, total_violations, 
                                   columns, rows, is_preview=False):
    """
    Generate HTML section for a single configuration
    
    Args:
        id_config: Configuration ID
        dimension: Dimension name
        schema: Schema name
        table: Table name
        rule: Audit rule description
        total_violations: Total number of violations
        columns: List of column names
        rows: List of row data
        is_preview: If True, add "showing first 5" note
    
    Returns:
        HTML section as string
    """
    preview_note = f' (showing first 5)' if is_preview and total_violations > 5 else ''
    
    html = f'''
    <div class="config-section">
        <h3>🔧 Dimension:</strong> {dimension}</h3>
        <div class="config-info">
            <strong> Configuration #{id_config}<br>
            <strong>Table:</strong> {schema}.{table}<br>
            <strong>Rule:</strong> {rule}<br>
            <strong>Total Violations:</strong> <span class="violation-count">{total_violations}</span> records{preview_note}
        </div>
        
        <table class="audit-table">
            <thead>
                <tr>
    '''
    
    # Add column headers
    for col in columns:
        html += f'                    <th>{col}</th>\n'
    
    html += '''
                </tr>
            </thead>
            <tbody>
    '''
    
    # Add rows
    for row in rows:
        html += '                <tr>\n'
        for col in columns:
            value = row[col]
            display_value = '' if value is None else str(value)
            # Escape HTML
            display_value = display_value.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            html += f'                    <td>{display_value}</td>\n'
        html += '                </tr>\n'
    
    html += '''
            </tbody>
        </table>
    </div>
    '''
    
    return html


def escape_html(text):
    """Escape HTML special characters"""
    if text is None:
        return ''
    return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')