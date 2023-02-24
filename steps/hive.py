import logging

logger = logging.getLogger("hive")


class HiveService:
    def __init__(
        self, correlation_id: str, spark_session,
    ):
        self._correlation_id = correlation_id
        self._spark_session = spark_session

    def execute_queries(self, queries):
        for query in queries:
            if query and not query.isspace():
                logger.info(
                    f"Executing query : {query} for correlation id : {self._correlation_id}"
                )
                self._spark_session.sql(query)
            else:
                logger.info(
                    f"Empty query received. Not executing. Correlation id : {self._correlation_id}"
                )

    def create_database_if_not_exist(self, db_name):
        create_db_query = f"CREATE DATABASE IF NOT EXISTS {db_name}"
        logger.info(
            f"Creating audit database named : {db_name} using sql : '{create_db_query}' for correlation id : {self._correlation_id}"
        )
        self._spark_session.sql(create_db_query)
        logger.info(f"Database created if not present: {db_name}")

    def execute_sql_statement_with_interpolation(
        self, file=None, sql_statement=None, interpolation_dict=None
    ):
        if file:
            with open(file, "r") as fd:
                content = fd.read()
        else:
            content = sql_statement

        if interpolation_dict:
            for k, v in interpolation_dict.items():
                content = content.replace(k, v)

        if content.count(";") > 1:
            self.execute_queries([statement for statement in content.split(";") if statement])
        else:
            self._spark_session.sql(content)
