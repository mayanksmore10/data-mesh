import pendulum

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID = "datamesh_postgres"


@dag(
    dag_id="datamesh_quality_audit",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),
    catchup=False,
    tags=[
        "data-mesh",
        "quality",
        "governance",
    ],
    description=(
        "Quality and governance checks for "
        "Sales, Finance and Operations data products"
    ),
)
def datamesh_quality_audit():

    @task
    def validate_data_products():

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        sales_count = hook.get_first(
            """
            SELECT COUNT(*)
            FROM sales_data_product;
            """
        )[0]

        finance_count = hook.get_first(
            """
            SELECT COUNT(*)
            FROM finance_data_product;
            """
        )[0]

        operations_count = hook.get_first(
            """
            SELECT COUNT(*)
            FROM operations_data_product;
            """
        )[0]

        print(
            f"[VALIDATE] Sales records: {sales_count}"
        )
        print(
            f"[VALIDATE] Finance records: {finance_count}"
        )
        print(
            f"[VALIDATE] Operations records: {operations_count}"
        )

        if sales_count == 0:
            raise ValueError(
                "Sales data product is empty."
            )

        if finance_count == 0:
            raise ValueError(
                "Finance data product is empty."
            )

        if operations_count == 0:
            raise ValueError(
                "Operations data product is empty."
            )

        print(
            "[VALIDATE] All data products contain data."
        )


    @task
    def quality_audit():

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        checks = {

            "sales_null_event_id": """
                SELECT COUNT(*)
                FROM sales_data_product
                WHERE event_id IS NULL;
            """,

            "sales_negative_amount": """
                SELECT COUNT(*)
                FROM sales_data_product
                WHERE amount < 0;
            """,

            "finance_null_event_id": """
                SELECT COUNT(*)
                FROM finance_data_product
                WHERE event_id IS NULL;
            """,

            "finance_negative_amount": """
                SELECT COUNT(*)
                FROM finance_data_product
                WHERE amount < 0;
            """,

            "operations_null_event_id": """
                SELECT COUNT(*)
                FROM operations_data_product
                WHERE event_id IS NULL;
            """,

            "operations_negative_processing_time": """
                SELECT COUNT(*)
                FROM operations_data_product
                WHERE processing_time_minutes < 0;
            """,
        }

        total_failures = 0

        for check_name, query in checks.items():

            result = hook.get_first(query)[0]

            print(
                f"[QUALITY] {check_name}: {result}"
            )

            total_failures += result

        if total_failures > 0:
            raise ValueError(
                f"Quality audit failed with "
                f"{total_failures} invalid records."
            )

        print(
            "[QUALITY] All data-quality checks passed."
        )


    @task
    def generate_report():

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        results = hook.get_records(
            """
            SELECT
                'Sales' AS domain,
                COUNT(*) AS records
            FROM sales_data_product

            UNION ALL

            SELECT
                'Finance' AS domain,
                COUNT(*) AS records
            FROM finance_data_product

            UNION ALL

            SELECT
                'Operations' AS domain,
                COUNT(*) AS records
            FROM operations_data_product;
            """
        )

        print("=" * 65)
        print("DATA MESH QUALITY REPORT")
        print("=" * 65)

        for domain, records in results:

            print(
                f"{domain:<15} : {records} records"
            )

        print("=" * 65)
        print(
            "[REPORT] Data Mesh quality report generated."
        )


    validated = validate_data_products()

    audited = quality_audit()

    reported = generate_report()

    validated >> audited >> reported


datamesh_quality_audit()