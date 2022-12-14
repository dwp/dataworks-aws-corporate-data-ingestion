from logging import getLogger

logger = getLogger("hive")


class HiveService:
    def __init__(
            self,
            transition_db_name: str,
            db_name: str,
            correlation_id: str,
            spark_session,
    ):
        self._transition_db_name = transition_db_name
        self._db_name = db_name
        self._correlation_id = correlation_id
        self._spark_session = spark_session

    def execute_queries(self, queries):
        for query in queries:
            if query and not query.isspace():
                logger.info(
                    f"Executing query : {query} for correlation id : {self._correlation_id}",
                    query,
                    self._correlation_id,
                )
                self._spark_session.sql(query)
            else:
                logger.info(f"Empty query received. Not executing. Correlation id : {self._correlation_id}")

    def create_database_if_not_exist(self, db_name):
        create_db_query = f"CREATE DATABASE IF NOT EXISTS {db_name}"
        self._spark_session.sql(create_db_query)
        logger.info(
            "Creating audit database named : %s using sql : '%s' for correlation id : %s",
            db_name,
            create_db_query,
            self._correlation_id,
        )

    def execute_sql_statement_with_interpolation(self, file=None, sql_statement=None, interpolation_dict=None):
        content = None
        if file:
            with open(file, 'r') as fd:
                content = fd.read()
        else:
            content = sql_statement

        logger.info(interpolation_dict)
        if interpolation_dict:
            for k, v in interpolation_dict.items():
                content = content.replace(k, v)

        logger.info(f"Run SQL statement: {content}")

        if content.count(';') > 1:
            self.execute_queries(content.split(';'))
        else:
            self._spark_session.sql(content)
