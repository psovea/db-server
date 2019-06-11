import mysql.connector
from environs import Env

def build_query(table, keys, search_values, inner_join=None):
    keywords = " AND  ".join([
        " OR ".join(["{} LIKE '{}'".format(key, val) for val in values])
        for key, values in search_values.items()
    ])

    if inner_join:
        join_table, (key1, key2) = next(iter(inner_join.items()))
        for join_table, (key1, key2) in inner_join.items():
            table = "({} INNER JOIN {} ON {} = {})".format(table, join_table, key1, key2)

    print(search_values.keys())
    query = "SELECT {} FROM {} WHERE {}".format(",".join(keys), table, keywords)
    print(query)
    return query

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


    def execQuery(self, query, only_one=False):
        self.cursor.execute(query)

        # Fetch and return only the first occurrence.
        if only_one:
            return self.cursor.fetchone()

        # Fetch and return all occurrences.
        res = self.cursor.fetchall()
        print(res)
        return res


    def getId(self, table, search_values):
        first_search_key, first_search_value = next(iter(search_values.items()))
        query = "SELECT id FROM {} WHERE {} = '{}'".format(table, first_search_key, first_search_value)
        for search_key, search_value in search_values.items():
            query += " AND {} = '{}'".format(search_key, search_value)
        query += " LIMIT 1"
        print(query)
        self.cursor.execute(query)
        id = self.cursor.fetchone()
        if id:
            return id[0]
        return None

    def insert(self, table, insert_values):
        keys = ','.join(list(insert_values.keys()))
        values = ','.join(
            ["'{}'".format(value) for value in list(insert_values.values())])
        query = "INSERT INTO {} ({}) VALUES ({})".format(table, keys, values)
        self.cursor.execute(query)
        self.connection.commit()
        return self.cursor.lastrowid

    def getOrInsert(self, table, search_values, insert_values):
        id = self.getId(table, search_values)
        if id:
            return id
        return self.insert(table, insert_values)

    def getLineToTypeMapping(self):
        query = "SELECT transport_lines.external_code, transport_types.name FROM transport_lines LEFT JOIN transport_types ON transport_type_id = transport_types.id"
        self.cursor.execute(query)
        return self.cursor.fetchall()


