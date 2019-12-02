do-everything:
	dbt compile --target snowflake
	dbt seed --target snowflake --full-refresh
	dbt run --target snowflake --full-refresh
	dbt test --target snowflake
