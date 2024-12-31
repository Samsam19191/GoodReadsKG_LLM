from neo4j import GraphDatabase

class Neo4jConnector:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def run_query(self, query, parameters=None, single=False):
        """
        Executes a query on the Neo4j database.
        
        :param query: Cypher query to execute.
        :param parameters: Parameters for the query.
        :param single: If True, return a single record (converted to a dictionary).
        :return: Query result(s) as a dictionary or list of dictionaries.
        """
        with self.driver.session() as session:
            result = session.run(query, parameters)
            if single:
                record = result.single()
                return record.data() if record else None
            return [record.data() for record in result]