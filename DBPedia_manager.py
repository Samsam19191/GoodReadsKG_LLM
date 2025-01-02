from SPARQLWrapper import SPARQLWrapper, JSON


class DBpediaConnector:
    PREFIXES = """
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    """

    def __init__(self, endpoint="http://dbpedia.org/sparql"):
        self.sparql = SPARQLWrapper(endpoint)

    def query(self, sparql_query):
        """
        Execute a SPARQL query and return the results as JSON.
        """
        sparql_query = self.PREFIXES + sparql_query
        self.sparql.setQuery(sparql_query)
        self.sparql.setReturnFormat(JSON)
        try:
            results = self.sparql.query().convert()
            return results["results"]["bindings"]
        except Exception as e:
            print(f"Error executing SPARQL query: {e}")
            return []
