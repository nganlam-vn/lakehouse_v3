from pyspark.sql import SparkSession
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
import os


def source_to_send(spark: SparkSession):
    """
    Extract failed audit records from fact table for alerting
    """
    df_mandatory = spark.sql("""
        SELECT
            CASE
                WHEN ds_dimension = 'completeness_mandatory_column' THEN 'Completeness-Mandatory Columns'
                ELSE ds_dimension
            END AS `Dimension`,
            ds_rule_description AS `Audit Rule Description`,
            ds_violated_records AS `Violated Record`,
            nr_total_violated_records AS `Total Violated Records`,
            ds_audit_result AS `Result`,
            ds_table AS `Table`,
            ds_schema AS `Schema`,
            dt_checked_at AS `Checked At`,
            ds_mandatory_column_array AS `Mandatory Columns`,
            ds_additional_filter_condition AS `Additional Filter`,
            ds_pk AS `Primary Key Columns`,
            ds_timestamp_utc_column AS `Timestamp Column`,
            nr_id_configuration AS `ID Configuration`,
            ds_note AS `Note`,
            cd_fact_dataaudit_completeness_mandatory_column AS `Fact Table PK`
        FROM dataaudit.fact_dataaudit_completeness_mandatory_column
        WHERE ds_audit_result != 'PASS'
          AND dt_checked_at = (
              SELECT MAX(dt_checked_at) 
              FROM dataaudit.fact_dataaudit_completeness_mandatory_column
          )
        ORDER BY ds_violated_records
    """)
    
    print(f"📊 Extracted {df_mandatory.count()} failed audit records")
    
    if df_mandatory.count() > 0:
        print("\n=== Sample Failed Audits ===")
        df_mandatory.show(5, truncate=False)
    else:
        print("✅ No failed audits found!")
    
    return df_mandatory


def spark_df_to_html_table(df, limit=None):
    """
    Convert Spark DataFrame to HTML table (for email body)
    """
    columns = df.columns
    
    # Limit records if specified
    if limit:
        rows = df.limit(limit).collect()
    else:
        rows = df.collect()
    
    # Build HTML table
    html = '<table class="audit-table">\n'
    
    # Header
    html += '  <thead>\n    <tr>\n'
    for col in columns:
        html += f'      <th>{col}</th>\n'
    html += '    </tr>\n  </thead>\n'
    
    # Body
    html += '  <tbody>\n'
    for row in rows:
        html += '    <tr>\n'
        for col in columns:
            value = row[col]
            display_value = '' if value is None else str(value)
            # Escape HTML
            display_value = display_value.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            html += f'      <td>{display_value}</td>\n'
        html += '    </tr>\n'
    html += '  </tbody>\n'
    
    html += '</table>'
    
    return html


def spark_df_to_html_file(df, filepath):
    """
    Save complete Spark DataFrame as a standalone HTML file
    """
    columns = df.columns
    rows = df.collect()
    total_records = len(rows)
    
    # Build complete HTML document
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Data Quality Audit Report</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                margin: 20px;
                background-color: #f5f5f5;
            }
            h1 {
                color: #d9534f;
                text-align: center;
            }
            .info {
                background-color: #fff;
                border: 1px solid #ddd;
                border-radius: 4px;
                padding: 15px;
                margin-bottom: 20px;
            }
            table {
                width: 100%;
                border-collapse: collapse;
                background-color: white;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            th {
                background-color: #5bc0de;
                color: white;
                padding: 12px;
                text-align: left;
                font-weight: bold;
                position: sticky;
                top: 0;
                z-index: 10;
            }
            td {
                border: 1px solid #ddd;
                padding: 10px;
                word-wrap: break-word;
            }
            tr:nth-child(even) {
                background-color: #f9f9f9;
            }
            tr:hover {
                background-color: #e9ecef;
            }
            .footer {
                margin-top: 20px;
                text-align: center;
                font-size: 12px;
                color: #6c757d;
            }
        </style>
    </head>
    <body>
        <h1>Data Quality Audit Report</h1>
        
        <div class="info">
            <strong>Report Details:</strong><br>
            <ul>
                <li><strong>Total Failed Audits:</strong> """ + str(total_records) + """</li>
                <li><strong>Generated:</strong> """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """</li>
                <li><strong>Source:</strong> dataaudit.fact_dataaudit_completeness_mandatory_column</li>
            </ul>
        </div>
        
        <table>
            <thead>
                <tr>
    """
    
    # Add headers
    for col in columns:
        html += f'                    <th>{col}</th>\n'
    
    html += """
                </tr>
            </thead>
            <tbody>
    """
    
    # Add all rows
    for row in rows:
        html += '                <tr>\n'
        for col in columns:
            value = row[col]
            display_value = '' if value is None else str(value)
            # Escape HTML characters
            display_value = display_value.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            html += f'                    <td>{display_value}</td>\n'
        html += '                </tr>\n'
    
    html += """
            </tbody>
        </table>
        
        <div class="footer">
            <p>This is an automated report from the Data Quality Monitoring System.</p>
            <p>For questions, please contact the Data Engineering team.</p>
            <p>Lam Hoai Kim Ngan: lamhoaikimngan@gmail.com</p>
            <p>Le Hoang Anh Duy: anhduy0969@gmail.com</p>
        </div>
    </body>
    </html>
    """
    
    # Write to file
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"✅ HTML file saved: {filepath} ({total_records} records)")


def send_email_with_attachment(df):
    """
    Send email with top 20 records in body + full HTML file attachment
    """
    sender_email = "watanabilinlin@gmail.com"
    app_password = "hrlm tsoh tkfc elxj"
    recipient_emails = ["watanabilinlin@gmail.com", "anhduy0969@gmail.com"]
    
    record_count = df.count()
    if record_count == 0:
        print("✅ No failed audits to send - skipping email")
        return
    
    print(f"📧 Preparing email with {record_count} failed audit records...")
    
    # Create HTML file with ALL records
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    html_filename = f"data_quality_alert_{timestamp}.html"
    html_filepath = f"/tmp/{html_filename}"
    
    spark_df_to_html_file(df, html_filepath)
    
    # Create email subject
    subject = f" Data Quality Alert - {record_count} Failed Audit(s) - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    
    # Create HTML email body with TOP 20 records
    html_table_preview = spark_df_to_html_table(df, limit=20)
    
    html_body = f"""
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
          .audit-table {{
            border-collapse: collapse;
            width: 100%;
            margin-top: 20px;
          }}
          .audit-table th {{
            background-color: #5bc0de;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: bold;
          }}
          .audit-table td {{
            border: 1px solid #ddd;
            padding: 10px;
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
        <h2> Data Quality Alert</h2>
        
        <div class="summary">
          <strong>Summary:</strong><br>
          <ul>
            <li><strong>Total Failed Audits:</strong> {record_count}</li>
            <li><strong>Alert Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</li>
            <li><strong>Source:</strong> dataaudit.fact_dataaudit_completeness_mandatory_column</li>
          </ul>
        </div>
        
        <h3>📊 Preview (Top 20 Records):</h3>
        {html_table_preview}
        
        <div class="notice">
          <strong>📎 Note:</strong> Showing top 20 records only. 
          <strong>Please open the attached HTML file ({html_filename}) to view all {record_count} records.</strong>
        </div>
        
        <div class="footer">
          <p>This is an automated alert from the Data Quality Monitoring System.</p>
          <p>For questions, please contact the Data Engineering team.</p>
          <p>Lam Hoai Kim Ngan: lamhoaikimngan@gmail.com</p>
          <p>Le Hoang Anh Duy: anhduy0969@gmail.com</p>
        </div>
      </body>
    </html>
    """
    
    # Create message
    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = sender_email
    message["To"] = ", ".join(recipient_emails)
    
    # Attach HTML body
    html_part = MIMEText(html_body, "html")
    message.attach(html_part)
    
    # Attach HTML file
    try:
        with open(html_filepath, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {html_filename}",
        )
        message.attach(part)
        
        print(f"✅ Attached full report: {html_filename}")
        
    except Exception as e:
        print(f"❌ Failed to attach file: {e}")
        raise
    
    # Send email
    try:
        print(f"📤 Sending email to {', '.join(recipient_emails)}...")
        
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, app_password)
            server.sendmail(sender_email, recipient_emails, message.as_string())
        
        print("✅ Email sent successfully!")
        
        # Clean up temp file
        if os.path.exists(html_filepath):
            os.remove(html_filepath)
            print(f"🗑️ Cleaned up temp file: {html_filepath}")
        
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        raise


def main():
    spark = SparkSession.builder.appName("SendDataAuditAlert").getOrCreate()
    
    df = source_to_send(spark)
    
    # Send email with preview + full attachment
    if df.count() > 0:
        send_email_with_attachment(df)
    else:
        print("✅ No failed audits - no email sent")
    
    spark.stop()


if __name__ == "__main__":
    main()