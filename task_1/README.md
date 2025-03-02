# Task 1

## How to run it
1. Create python virtual evnironment:
`python3 -m venv .venv`
2. Activate virtual environment:
`source .venv/bin/activate`
3. Install dbt:
`pip install -r requirements.txt`
4. Run PostgreSQL in Docker container:
`docker run --name postgres_task_1 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=solidgate -p 5432:5432 -d postgres`
5. Create "seed" tables:
`dbt seed`
6. Run dbt to build models:
`dbt run`

Now you can query result (and source) tables.

Options:
1. `docker exec -it postgres_task_1 bash` -> `psql -U postgres -d solidgate`
2. `psql -h localhost -p 5432 -U postgres -d solidgate`
3. Or in your favourite IDE (`postgresql://postgres:postgres@localhost:5432/solidgate`)
