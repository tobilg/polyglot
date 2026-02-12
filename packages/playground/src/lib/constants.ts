export const DIALECT_DISPLAY_NAMES: Record<string, string> = {
  generic: "Generic SQL",
  athena: "Amazon Athena",
  bigquery: "Google BigQuery",
  clickhouse: "ClickHouse",
  cockroachdb: "CockroachDB",
  databricks: "Databricks",
  doris: "Apache Doris",
  dremio: "Dremio",
  drill: "Apache Drill",
  druid: "Apache Druid",
  duckdb: "DuckDB",
  dune: "Dune SQL",
  exasol: "Exasol",
  fabric: "Microsoft Fabric",
  hive: "Apache Hive",
  materialize: "Materialize",
  mysql: "MySQL",
  oracle: "Oracle",
  postgres: "PostgreSQL",
  presto: "Presto",
  prql: "PRQL",
  redshift: "Amazon Redshift",
  risingwave: "RisingWave",
  singlestore: "SingleStore",
  snowflake: "Snowflake",
  solr: "Apache Solr",
  spark: "Apache Spark",
  sqlite: "SQLite",
  starrocks: "StarRocks",
  tableau: "Tableau",
  teradata: "Teradata",
  tidb: "TiDB",
  trino: "Trino",
  tsql: "SQL Server (T-SQL)",
};

export const DEFAULT_TRANSPILE_SQL = `SELECT
  IFNULL(u.name, 'Anonymous') AS user_name,
  DATE_FORMAT(o.created_at, '%Y-%m') AS order_month,
  COUNT(*) AS order_count,
  SUM(o.total) AS total_spent
FROM orders o
LEFT JOIN users u ON o.user_id = u.id
WHERE o.created_at >= DATE_SUB(NOW(), INTERVAL 6 MONTH)
GROUP BY u.name, DATE_FORMAT(o.created_at, '%Y-%m')
HAVING total_spent > 100
ORDER BY total_spent DESC
LIMIT 20;`;

export const DEFAULT_AST_SQL = `SELECT
  c.name AS customer_name,
  COUNT(o.id) AS order_count,
  SUM(o.amount) AS total_amount
FROM customers c
INNER JOIN orders o ON c.id = o.customer_id
WHERE o.status = 'completed'
  AND o.created_at >= '2024-01-01'
GROUP BY c.name
HAVING COUNT(o.id) > 5
ORDER BY total_amount DESC;`;

export const DEFAULT_FORMAT_SQL = `select u.id,u.name,u.email,count(o.id) as order_count,sum(o.total) as lifetime_value from users u left join orders o on u.id=o.user_id left join addresses a on u.id=a.user_id where u.active=true and u.created_at>='2024-01-01' group by u.id,u.name,u.email having count(o.id)>0 order by lifetime_value desc limit 50;`;

export const DEFAULT_VALIDATE_SQL = `SELECT
  name,
  age,
  department
FROM employees
WHERE salary > 50000
  AND hire_date >= '2024-01-01'
ORDER BY name;`;
