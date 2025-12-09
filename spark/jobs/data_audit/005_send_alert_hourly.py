from pyspark.sql import SparkSession
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
import os

from email_html_generator import (
    generate_email_body_html,
    generate_file_html,
    generate_config_section_html,
    escape_html
)


def get_failed_audits_by_dimension(spark: SparkSession, dimension: str, table_name: str):
    """
    Generic function to extract failed audit records for any dimension
    Handles different schemas for different dimensions
    """
    # Base columns that exist in all fact tables
    base_query = f"""
        SELECT
            nr_id_configuration AS `ID Configuration`,
            CASE
                WHEN ds_dimension = 'completeness_mandatory_column' THEN 'Completeness-Mandatory Columns'
                WHEN ds_dimension = 'validity' THEN 'Validity'
                WHEN ds_dimension = 'integrity' THEN 'Referential Integrity'
                WHEN ds_dimension = 'timeliness' THEN 'Timeliness'
                ELSE ds_dimension
            END AS `Dimension`,
            ds_rule_description AS `Audit Rule Description`,
            ds_table AS `Table`,
            ds_schema AS `Schema`,
    """
    
    # Dimension-specific columns
    if dimension == 'completeness_mandatory_column':
        specific_cols = """
            ds_mandatory_column_array AS `Mandatory Columns`,
            ds_violated_records AS `Violated Record`,
            nr_total_violated_records AS `Total Violated Records`,
            ds_audit_result AS `Result`,
            dt_checked_at AS `Checked At`,
            ds_additional_filter_condition AS `Additional Filter`,
            ds_pk AS `Primary Key Columns`,
            ds_timestamp_utc_column AS `Timestamp Column`,
            ds_note AS `Note`,
            cd_fact_dataaudit_completeness_mandatory_column AS `Fact Table PK`
        """
    elif dimension == 'validity':
        specific_cols = """
            ds_validation_rule AS `Validation Rule`,
            ds_violated_records AS `Violated Record`,
            nr_total_violated_records AS `Total Violated Records`,
            ds_audit_result AS `Result`,
            dt_checked_at AS `Checked At`,
            ds_pk AS `Primary Key Columns`,
            ds_timestamp_utc_column AS `Timestamp Column`,
            ds_note AS `Note`,
            cd_fact_dataaudit_validity AS `Fact Table PK`
        """
    else:
        # Fallback for unknown dimensions
        specific_cols = """
            ds_violated_records AS `Violated Record`,
            nr_total_violated_records AS `Total Violated Records`,
            ds_audit_result AS `Result`,
            dt_checked_at AS `Checked At`,
            ds_note AS `Note`,
            'unknown' AS `Fact Table PK`
        """
    
    full_query = base_query + specific_cols + f"""
        FROM dataaudit.{table_name}
        WHERE ds_audit_result != 'PASS'
          AND dt_checked_at = (
              SELECT MAX(dt_checked_at) 
              FROM dataaudit.{table_name}
          )
        ORDER BY nr_id_configuration
    """
    
    df = spark.sql(full_query)
    
    count = df.count()
    print(f"📊 [{dimension}] Extracted {count} failed audit records")
    
    if count > 0:
        print(f"\n=== [{dimension}] Sample Failed Audits ===")
        df.show(5, truncate=False)
    
    return df


def combine_dataframes(dfs_dict):
    """
    Combine multiple dimension DataFrames into one
    """
    non_empty_dfs = [(name, df) for name, df in dfs_dict.items() if df.count() > 0]
    
    if not non_empty_dfs:
        return None
    
    print("\n📦 Combining DataFrames:")
    for name, df in non_empty_dfs:
        print(f"  - {name}: {df.count()} records")
    
    # Union with allowMissingColumns to handle different schemas
    combined_df = non_empty_dfs[0][1]
    for name, df in non_empty_dfs[1:]:
        combined_df = combined_df.unionByName(df, allowMissingColumns=True)
    
    combined_df = combined_df.orderBy("`ID Configuration`", "`Dimension`")
    
    print(f"✅ Combined total: {combined_df.count()} records")
    return combined_df


def spark_df_to_html_table_grouped(df):
    """
    Convert Spark DataFrame to HTML table grouped by ID Configuration
    Shows max 5 rows per configuration in email body
    """
    from pyspark.sql.functions import row_number, coalesce, lit
    from pyspark.sql import Window
    
    # Handle missing Fact Table PK column
    if "`Fact Table PK`" not in df.columns:
        df = df.withColumn("`Fact Table PK`", lit(None))
    
    window = Window.partitionBy("`ID Configuration`").orderBy(
        coalesce("`Fact Table PK`", lit(0)).cast("long")
    )
    df_numbered = df.withColumn("row_num", row_number().over(window))
    
    id_configs = [row[0] for row in df.select("`ID Configuration`").distinct().orderBy("`ID Configuration`").collect()]
    
    html = ""
    
    for id_config in id_configs:
        df_config = df_numbered.filter(f"`ID Configuration` = {id_config}")
        total_count = df_config.count()
        
        df_config_limited = df_config.filter("row_num <= 5").drop("row_num")
        rows = df_config_limited.collect()
        columns = df_config_limited.columns
        
        first_row = rows[0] if rows else None
        
        # Skip metadata columns
        skip_cols = ['ID Configuration', 'Audit Rule Description', 'Schema', 'Table']
        display_cols = [c for c in columns if c not in skip_cols and c != 'row_num']
        
        # ✅ Use HTML generator
        html += generate_config_section_html(
            id_config=id_config,
            dimension=first_row['Dimension'],
            schema=first_row['Schema'],
            table=first_row['Table'],
            rule=first_row['Audit Rule Description'],
            total_violations=total_count,
            columns=display_cols,
            rows=rows,
            is_preview=True
        )
    
    return html


def spark_df_to_html_file(df, filepath):
    """
    Save complete Spark DataFrame as a standalone HTML file (all records)
    """
    from pyspark.sql.functions import lit
    
    # Handle missing Fact Table PK column
    if "`Fact Table PK`" not in df.columns:
        df = df.withColumn("`Fact Table PK`", lit(None))
    
    id_configs = [row[0] for row in df.select("`ID Configuration`").distinct().orderBy("`ID Configuration`").collect()]
    
    total_records = df.count()
    
    # Generate all config sections
    sections_html = ""
    
    for id_config in id_configs:
        df_config = df.filter(f"`ID Configuration` = {id_config}")
        rows = df_config.collect()
        columns = df_config.columns
        
        first_row = rows[0]
        
        # ✅ Use HTML generator
        sections_html += generate_config_section_html(
            id_config=id_config,
            dimension=first_row['Dimension'],
            schema=first_row['Schema'],
            table=first_row['Table'],
            rule=first_row['Audit Rule Description'],
            total_violations=len(rows),
            columns=columns,
            rows=rows,
            is_preview=False
        )
    
    # ✅ Generate complete file HTML
    html = generate_file_html(
        total_records=total_records,
        config_count=len(id_configs),
        id_configs_sections=sections_html
    )
    
    # Write to file
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"✅ HTML file saved: {filepath} ({total_records} records)")


def send_email_with_attachment(df):
    """
    Send email grouped by ID Configuration (max 5 rows per config) + full HTML attachment
    """
    sender_email = "anhduy0969@gmail.com"
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
    
    # Count unique configurations and dimensions
    config_count = df.select("`ID Configuration`").distinct().count()
    dimension_count = df.select("`Dimension`").distinct().count()
    
    # Create email subject
    subject = f"⚠️ Data Quality Alert - {config_count} Config(s), {dimension_count} Dimension(s), {record_count} Violation(s) - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    
    # Create HTML email body with grouped data (max 5 per config)
    html_table_preview = spark_df_to_html_table_grouped(df)
    
    # ✅ Use HTML generator for email body
    html_body = generate_email_body_html(
        html_table_preview=html_table_preview,
        record_count=record_count,
        config_count=config_count,
        dimension_count=dimension_count,
        html_filename=html_filename
    )
    
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
    
    # Define which dimensions to include in alert
    dfs_to_alert = {
        "Completeness-Mandatory": get_failed_audits_by_dimension(
            spark, 
            "completeness_mandatory_column", 
            "fact_dataaudit_completeness_mandatory_column"
        ),
        "Validity": get_failed_audits_by_dimension(
            spark,
            "validity",
            "fact_dataaudit_validity"
        ),
    }
    
    # Combine all DataFrames
    combined_df = combine_dataframes(dfs_to_alert)
    
    # Send email if there are any failures
    if combined_df and combined_df.count() > 0:
        send_email_with_attachment(combined_df)
    else:
        print("✅ No failed audits across all dimensions - no email sent")
    
    spark.stop()


if __name__ == "__main__":
    main()