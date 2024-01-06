This is a dimensional data modeling project using dbt.

The main files/folders are: 1 - models, 2 - snapshots, 3 - dbt_job, 4 - dbt_project

1 - models:
    There are 3 schemas: staging, intermediate, analytics(aka marts)

    Final models are: fact table: weekly_sales_inventory
                     dimension table: customer_dim
2 - snapshot:
    Uses check strategy to create snapshot "customer_snapshot" (base of final customer_dim model)

3 - dbt_job.sh:
    Shell script of running the dbt job and adding log files

4 - dbt_project.yml:
    yaml file with generic model configs if not specified in the model's individual folder/file