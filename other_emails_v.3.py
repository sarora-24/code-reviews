"""
Module for initializing BigQuery tables and inserting GUIDs.

This module creates customer_graph, customer_graph_legacy, and used_guids tables
in BigQuery, and populates them with GUIDs from a specified query.
"""

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

def initialize_tables():
    """
    Initializes BigQuery tables and inserts GUIDs.

    This function creates three tables: customer_graph, customer_graph_legacy, and used_guids.
    It then queries for new user emails and inserts the resulting GUIDs into the customer_graph,
    customer_graph_legacy, and used_guids tables.

    Raises:
        GoogleAPIError: If there is an error in creating the tables or inserting rows.
    """
    # Define the project ID, dataset, and tables to create
    project_id = 'cdp-dev-342319'
    dataset_id = 'Ashish_Tables'
    tables = ['customer_graph', 'customer_graph_legacy', 'used_guids']
    client = bigquery.Client(project=project_id)

    # Define the schema for the customer_graph and customer_graph_legacy tables
    common_schema = [
        bigquery.SchemaField("GUID", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("UserEmail", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("BillingID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ShippingID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Date", "DATE", mode="REQUIRED"),
    ]

    # Define the schema for the used_guids table
    used_guids_schema = [
        bigquery.SchemaField("GUID", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Date", "DATE", mode="REQUIRED")
    ]

    # Create the tables
    for table_id in tables:
        table_ref = client.dataset(dataset_id).table(table_id)
        schema = common_schema if table_id != 'used_guids' else used_guids_schema
        table = bigquery.Table(table_ref, schema=schema)
        try:
            client.create_table(table)
            print(f"Table {table.table_id} created successfully.")
        except GoogleAPIError as e:
            print(f"Table creation failed: {e}")

    # Define the query to fetch new emails
    query = """
        WITH user_emails AS (
            SELECT DISTINCT
                du.Email AS UserEmail,
                CAST(NULL AS STRING) AS ShippingID,
                CAST(NULL AS STRING) AS BillingID
            FROM `marketing-data-lakehouse-01.EDWOlap.ClarkitbizDimUser` du
            -- This contains user info
            LEFT JOIN `marketing-data-production.Sales_Data.Invoices_And_Orders_Sales_Data` iosd
                ON du.Email = iosd.UserEmail
            WHERE iosd.UserEmail IS NULL

            UNION ALL

            SELECT DISTINCT
                us.email AS UserEmail,
                CAST(NULL AS STRING) AS ShippingID,
                CAST(NULL AS STRING) AS BillingID
            FROM `marketing-data-lakehouse-01.EDWNonOlap.GetReportUserSubscriptions` us
            LEFT JOIN `marketing-data-lakehouse-01.EDWOlap.ClarkitbizDimUser` du
                ON us.email = du.Email
            LEFT JOIN `marketing-data-production.Sales_Data.Invoices_And_Orders_Sales_Data` iosd
                ON us.Email = iosd.UserEmail
            WHERE iosd.UserEmail IS NULL AND du.Email IS NULL
        )
        SELECT
            ug.GUID,
            ue.UserEmail,
            ue.BillingID,
            ue.ShippingID,
            CURRENT_DATE() as Date
        FROM user_emails ue
        JOIN `cdp-dev-342319.Ashish_Tables.all_users_with_guid` ug
        ON ue.UserEmail = ug.UserEmail
    """

    # Fetch new emails
    query_job = client.query(query)
    new_emails = list(query_job.result())

    # Prepare rows for insertion, converting the date to a string
    rows_to_insert = [
        {
            "GUID": email.GUID,
            "UserEmail": email.UserEmail,
            "BillingID": email.BillingID,
            "ShippingID": email.ShippingID,
            "Date": email.Date.isoformat()  # Convert date to string
        }
        for email in new_emails
    ]

    # Insert rows into customer_graph and customer_graph_legacy tables
    for table_id in ['customer_graph', 'customer_graph_legacy']:
        table_ref = client.dataset(dataset_id).table(table_id)
        errors = client.insert_rows_json(table_ref, rows_to_insert)
        if errors:
            print(f"Errors occurred while inserting into {table_id}: {errors}")
        else:
            print(f"GUIDs added successfully to {table_id}.")

    # Insert GUIDs into the used_guids table, converting the date to a string
    used_guids_rows = [{"GUID": email.GUID, "Date": email.Date.isoformat()} for email in new_emails]
    table_ref = client.dataset(dataset_id).table('used_guids')
    errors = client.insert_rows_json(table_ref, used_guids_rows)
    if errors:
        print(f"Errors occurred while inserting into used_guids: {errors}")
    else:
        print("GUIDs added successfully to used_guids.")

if __name__ == '__main__':
    initialize_tables()
