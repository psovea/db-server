import mysql.connector
from environs import Env

# Helper to build SELECT queries. Search_values is a dict that creates the
# WHERE-clause. If split-search is True, the value of the dict needs to be a
# list. This list gets put into an OR-clause within the WHERE-clause.
def build_query(table, keys, search_values, inner_join=None, order_by=None, split_search=True):
    if split_search:
        search_params = " AND  ".join([
            " OR ".join(["{} LIKE '{}'".format(key, val) for val in values])
            for key, values in search_values.items()
        ])
    else:
        search_params = " AND ".join(["{} LIKE '{}'".format(
            key, value) for key, value in search_values.items()])

    if inner_join:
        join_table, (key1, key2) = next(iter(inner_join.items()))
        for join_table, (key1, key2) in inner_join.items():
            table = "({} INNER JOIN {} ON {} = {})".format(
                table, join_table, key1, key2)

    query = "SELECT {} FROM {} WHERE {}".format(
        ",".join(keys), table, search_params)

    if order_by:
        query += "ORDER BY {}".format(",".join(order_by))
    return query

# Class to manage MYSQL-connection and contains helpers for some often-used
# queries. The constructor establishes the connection using the mysql-module
class MysqlConnector:
    def __init__(self):
        env = Env()
        env.read_env()
        host = env.str("DB_HOST")
        database = env.str("DB_NAME")
        user = env.str("DB_USER")
        password = env.str("DB_PASS")
        self.connection = mysql.connector.connect(
            host=host, database=database, user=user, password=password)

        if self.connection.is_connected():
            self.cursor = self.connection.cursor()
        else:
            print("Can't connect to DB...")

    # Executes a query. no-result needs to be true if the query is not
    # a SELECT-query. only_one depicts if we only need one result.
    def execQuery(self, query, only_one=False, no_result=False):
        self.cursor.execute(query)

        if no_result:
            self.connection.commit()
            return

        # Fetch and return only the first occurrence.
        if only_one:
            return self.cursor.fetchone()

        # Fetch and return all occurrences.
        res = self.cursor.fetchall()
        return res

    # Gets the id of the record in table that complies to the search_values,
    # which needs to be a dict.
    def getId(self, table, search_values):
        first_search_key, first_search_value = next(
            iter(search_values.items()))
        query = "SELECT id FROM {} WHERE {} = '{}'".format(
            table, first_search_key, first_search_value)
        for search_key, search_value in search_values.items():
            query += " AND {} = '{}'".format(search_key, search_value)
        query += " LIMIT 1"
        self.cursor.execute(query)
        id = self.cursor.fetchone()
        if id:
            return id[0]
        return None

    # Insert insert_values (dict) into table.
    def insert(self, table, insert_values):
        keys = ','.join(list(insert_values.keys()))
        values = ','.join(
            ["'{}'".format(value) for value in list(insert_values.values())])
        query = "INSERT INTO {} ({}) VALUES ({})".format(table, keys, values)
        self.cursor.execute(query)
        self.connection.commit()
        return self.cursor.lastrowid

    # Gets the id of a record in table if a record matches search_values.
    # Otherwise inserts insert_values
    def getOrInsert(self, table, search_values, insert_values):
        id = self.getId(table, search_values)
        if id:
            return id
        return self.insert(table, insert_values)

    # Custom query that retrieves the transport_type of all lines.
    def getLineToTypeMapping(self):
        query = "SELECT transport_lines.public_id, transport_types.name FROM transport_lines LEFT JOIN transport_types ON transport_type_id = transport_types.id"
        self.cursor.execute(query)
        return self.cursor.fetchall()
