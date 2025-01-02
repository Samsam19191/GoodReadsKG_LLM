import json
from neo4j_manager import Neo4jConnector
from DBPedia_manager import DBpediaConnector
import re


class DBpediaEnrichment:
    def __init__(
        self,
        neo4j_uri,
        neo4j_user,
        neo4j_password,
        dbpedia_endpoint="http://dbpedia.org/sparql",
    ):
        """
        Initialize the class with Neo4j connection details and a DBpediaConnector instance.
        """
        self.db = Neo4jConnector(neo4j_uri, neo4j_user, neo4j_password)
        self.dbpedia = DBpediaConnector(dbpedia_endpoint)

    def escape_sparql_string(self, value):
        """
        Escape special characters in a string for use in a SPARQL query.
        """
        return re.sub(r'(["\\])', r"\\\1", value)

    def fetch_author_biography(self, author_name):
        """
        Query DBpedia to fetch the biography of an author.
        """
        query = f"""
        SELECT ?abstract WHERE {{
            ?author a dbo:Person ;
                   foaf:name "{author_name}"@en ;
                   dbo:abstract ?abstract .
            FILTER (lang(?abstract) = "en")
        }}
        """
        results = self.dbpedia.query(query)

        if not results:
            # General query without language filter
            query_no_lang = f"""
            SELECT ?abstract WHERE {{
            ?author a dbo:Person ;
                    rdfs:label "{author_name}" ;
                    dbo:abstract ?abstract .
            }}
            """
            results = self.dbpedia.query(query_no_lang)

        return results[0]["abstract"]["value"] if results else None

    def add_author_biography(self, author_name):
        """
        Fetch and add an author's biography to the graph.
        """
        biography = self.fetch_author_biography(author_name)
        if biography:
            self.db.run_query(
                "MATCH (a:Author {name: $name}) SET a.biography = $bio",
                {"name": author_name, "bio": biography},
            )
            print(f"Added biography for author '{author_name}'.")
        else:
            print(f"No biography found for author '{author_name}'.")

    def fetch_author_birthplace(self, author_name):
        """
        Query DBpedia to fetch the birthplace of an author.
        """
        query = f"""
        SELECT ?birthPlace WHERE {{
            ?author a dbo:Person ;
                foaf:name "{author_name}"@en ;
                dbo:birthPlace ?birthPlace .
        }}
        """
        results = self.dbpedia.query(query)
        return results[0]["birthPlace"]["value"] if results else None

    def add_author_birthplace(self, author_name):
        """
        Fetch and add an author's birthplace to the graph.
        """
        birthplace = self.fetch_author_birthplace(author_name)
        if birthplace:
            self.db.run_query(
                "MATCH (a:Author {name: $name}) SET a.birthplace = $birthplace",
                {"name": author_name, "birthplace": birthplace},
            )
            print(f"Added birthplace for author '{author_name}'.")
        else:
            print(f"No birthplace found for author '{author_name}'.")

    def fetch_book_genres(self, book_title):
        """
        Query DBpedia to fetch genres of a book.
        """
        query = f"""
        SELECT ?genre WHERE {{
            ?book a dbo:Book ;
                foaf:name "{book_title}"@en ;
                dbo:literaryGenre ?genre .
        }}
        """
        results = self.dbpedia.query(query)
        return [result["genre"]["value"] for result in results] if results else []

    def add_book_genres(self, book_title):
        """
        Fetch and add genres for a book to the graph.
        """
        genres = self.fetch_book_genres(book_title)
        if len(genres) == 0:
            print(f"No genres found for book '{book_title}'.")
        else:
            for genre in genres:
                self.db.run_query(
                    """
                    MATCH (b:Book {name: $title})
                    MERGE (g:Genre {name: $genre})
                    MERGE (b)-[:HAS_GENRE]->(g)
                    """,
                    {"title": book_title, "genre": genre},
                )
            print(f"Added genres for book '{book_title}': {genres}")

    def fetch_book_editions(self, book_title):
        """
        Query DBpedia to fetch relationships between book editions.
        """
        query = f"""
        SELECT ?subsequentWork ?precedingWork WHERE {{
            ?book a dbo:Book ;
                foaf:name "{book_title}"@en ;
                dbo:subsequentWork ?subsequentWork ;
                dbo:precedingWork ?precedingWork .
        }}
        """
        results = self.dbpedia.query(query)
        return results[0] if results else {}

    def add_book_editions(self, book_title):
        """
        Fetch and add relationships between book editions to the graph.
        """
        editions = self.fetch_book_editions(book_title)
        if not editions:
            print(f"No edition relationships found for book '{book_title}'.")
        else:
            if editions.get("subsequentWork"):
                self.db.run_query(
                    """
                    MATCH (b1:Book {name: $title}), (b2:Book {name: $subsequent})
                    MERGE (b1)-[:SUBSEQUENT_EDITION]->(b2)
                    """,
                    {
                        "title": book_title,
                        "subsequent": editions["subsequentWork"]["value"],
                    },
                )
            if editions.get("precedingWork"):
                self.db.run_query(
                    """
                    MATCH (b1:Book {name: $title}), (b2:Book {name: $preceding})
                    MERGE (b1)-[:PRECEDING_EDITION]->(b2)
                    """,
                    {
                        "title": book_title,
                        "preceding": editions["precedingWork"]["value"],
                    },
                )
            print(f"Added edition relationships for book '{book_title}'.")

    def fetch_book_subjects(self, book_title):
        """
        Query DBpedia to fetch subjects of a book.
        """
        query = f"""
        SELECT ?subject WHERE {{
            ?book a dbo:Book ;
                foaf:name "{book_title}"@en ;
                dct:subject ?subject .
        }}
        """
        results = self.dbpedia.query(query)
        return [result["subject"]["value"] for result in results] if results else []

    def add_book_subjects(self, book_title):
        """
        Fetch and add subjects for a book to the graph.
        """
        subjects = self.fetch_book_subjects(book_title)
        if len(subjects) == 0:
            print(f"No subjects found for book '{book_title}'.")
        else:
            for subject in subjects:
                self.db.run_query(
                    """
                    MATCH (b:Book {name: $title})
                    MERGE (s:Subject {uri: $subject})
                    MERGE (b)-[:HAS_SUBJECT]->(s)
                    """,
                    {"title": book_title, "subject": subject},
                )
            print(f"Added subjects for book '{book_title}': {subjects}")

    def fetch_book_adaptations(self, book_title):
        """
        Query DBpedia to fetch film or TV adaptations of a book.
        """
        query = f"""
        SELECT ?adaptation WHERE {{
            ?book a dbo:Book ;
                foaf:name "{book_title}"@en ;
                dbo:film ?adaptation .
        }}
        """
        results = self.dbpedia.query(query)
        return [result["adaptation"]["value"] for result in results] if results else []

    def add_book_adaptations(self, book_title):
        """
        Fetch and add adaptations for a book to the graph.
        """
        adaptations = self.fetch_book_adaptations(book_title)
        if len(adaptations) == 0:
            print(f"No adaptations found for book '{book_title}'.")
        else:
            for adaptation in adaptations:
                self.db.run_query(
                    """
                    MATCH (b:Book {name: $title})
                    MERGE (a:Adaptation {uri: $adaptation})
                    MERGE (b)-[:HAS_ADAPTATION]->(a)
                    """,
                    {"title": book_title, "adaptation": adaptation},
                )
            print(f"Added adaptations for book '{book_title}': {adaptations}")

    def fetch_book_description(self, book_title):
        """
        Query DBpedia to fetch the description of a book.
        """
        query = f"""
        SELECT ?abstract WHERE {{
            ?book a dbo:Book ;
                foaf:name "{book_title}"@en ;
                dbo:abstract ?abstract .
            FILTER (lang(?abstract) = "en")
        }}
        """
        results = self.dbpedia.query(query)
        return results[0]["abstract"]["value"] if results else None

    def add_book_description(self, book_title):
        """
        Fetch and add the description for a book to the graph.
        """
        # Check if the book already has a description
        existing_description = self.db.run_query(
            "MATCH (b:Book {name: $title}) RETURN b.description AS description",
            {"title": book_title},
        )
        if existing_description and existing_description[0]["description"]:
            print(f"Book '{book_title}' already has a description.")
            return

        description = self.fetch_book_description(book_title)
        if description:
            self.db.run_query(
                "MATCH (b:Book {name: $title}) SET b.description = $description",
                {"title": book_title, "description": description},
            )
            print(f"Added description for book '{book_title}'.")
        else:
            print(f"No description found for book '{book_title}'.")

    def enrich_graph_with_dbpedia(self):
        """
        Enrich the graph by adding metadata from DBpedia.
        """
        # # Enrich authors
        # authors = self.db.run_query("MATCH (a:Author) RETURN a.name AS name")
        # for author in authors:
        #     author_name = author["name"]
        #     print(f"Processing author: '{author_name}'...")
        #     self.add_author_biography(author_name)
        #     self.add_author_birthplace(author_name)  # Enrich with birthplace

        # Enrich books
        books = self.db.run_query("MATCH (b:Book) RETURN b.id AS id, b.name AS name")
        for book in books:
            book_title = book["name"]
            escaped_title = self.escape_sparql_string(book_title)
            print(f"Processing book: '{escaped_title}'...")
            self.add_book_description(escaped_title)
            self.add_book_genres(escaped_title)
            self.add_book_subjects(escaped_title)
            self.add_book_adaptations(escaped_title)
            self.add_book_editions(escaped_title)

    def close(self):
        """
        Close the connection to the database.
        """
        self.db.close()
