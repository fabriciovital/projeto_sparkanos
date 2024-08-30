# Sparkanos Project


!!! info

    All data used in the project is public.


## About Sparkanos

The goal of the Sparkanos project is to extract data from PostgreSQL and write it to the data lake in the landing layer. From there, the data will be processed and written into Delta format across the bronze, silver, and gold layers.

Trino will be used as an API to connect to these Delta tables, enabling efficient querying and data access.

For data visualization, we will use Superset as the tool, with Trino serving as the API to connect to the MinIO lakehouse.

Airflow will be the orchestration tool for managing and automating the data workflows.

Finally, all data and processes will be cataloged and documented in OpenMetadata.
