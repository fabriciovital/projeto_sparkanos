# Ingestions Type

## Full Ingestions
Performs overwrite and places the data on top of all source tables.

## Incremental Ingestions
Updates only new data based on the modified date.

## About
In the first processing, we perform a full load of the data, and from the second load onwards, we perform an incremental update.

## How to Test Incremental Update
To perform an incremental update, go to the ````applications/trino/``` folder and in the **trino.sql** file, use the scripts from the 'Simulation incremental update' section. This script will update the data in the production PostgreSQL database, then execute the incremental process where only the new rows will be added.
