FROM python:3.10

WORKDIR /usr/app/dbt

RUN pip install dbt-core dbt-clickhouse

COPY . .

CMD ["dbt", "run"]