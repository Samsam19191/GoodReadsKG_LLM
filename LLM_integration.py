from openai import OpenAI
from neo4j_manager import Neo4jConnector


class GraphEnrichment:
    def __init__(self, neo4j_uri, neo4j_user, neo4j_password, openai_api_key):
        """
        Initialize the class with Neo4j connection details and OpenAI API key.
        """
        self.db = Neo4jConnector(neo4j_uri, neo4j_user, neo4j_password)
        self.client = OpenAI(api_key=openai_api_key)

    def get_similar_books_from_llm(self, title, author):
        """
        Use an LLM to find similar books based on the title and author.
        """
        prompt = f"""
            Given the book title "{title}" and author "{author}", suggest up to 3 similar books.
            If you do not recognize the book or cannot find similar ones, respond with "none".
            Provide only the titles of similar books as a comma-separated list or "none".
            """

        try:
            response = self.client.completions.create(model="gpt-4", prompt=prompt)
            result = response.choices[0].message.content.strip()
            if result.lower() == "none":
                return []
            return [book.strip() for book in result.split(",") if book.strip()]
        except Exception as e:
            print(f"Error querying LLM: {e}")
            return []

    def add_similarity_relationships(self, book_id, title, author):
        """
        Find similar books using the LLM and add SIMILAR_TO relationships to the graph.
        """
        try:
            similar_books = self.get_similar_books_from_llm(title, author)
            if not similar_books:
                print(f"No similar books found for '{title}' by '{author}'.")
                return

            for similar_title in similar_books:
                # Check if the similar book exists in the graph
                result = self.db.run_query(
                    "MATCH (b:Book {name: $title}) RETURN b.id AS id",
                    {"title": similar_title},
                    single=True,
                )

                if result:
                    similar_book_id = result["id"]

                    # Check if the relationship already exists
                    relationship_check = self.db.run_query(
                        """
                        MATCH (b1:Book {id: $book_id})-[r:SIMILAR_TO]->(b2:Book {id: $similar_book_id})
                        RETURN r
                        UNION
                        MATCH (b2:Book {id: $similar_book_id})-[r:SIMILAR_TO]->(b1:Book {id: $book_id})
                        RETURN r
                        """,
                        {"book_id": book_id, "similar_book_id": similar_book_id},
                        single=True,
                    )

                    if not relationship_check:
                        # Add the similarity relationship
                        self.db.run_query(
                            """
                            MATCH (b1:Book {id: $book_id}), (b2:Book {id: $similar_book_id})
                            MERGE (b1)-[:SIMILAR_TO]->(b2)
                            """,
                            {"book_id": book_id, "similar_book_id": similar_book_id},
                        )
                        print(
                            f"Added SIMILAR_TO relationship between '{title}' and '{similar_title}'."
                        )
                    else:
                        print(
                            f"SIMILAR_TO relationship already exists between '{title}' and '{similar_title}'."
                        )
                else:
                    print(f"'{similar_title}' not found in the graph. Skipping.")
        except Exception as e:
            print(f"Error adding similarity relationships: {e}")

    def enrich_with_relationships(self):
        """
        Iterate through all books in the graph and add similarity relationships.
        """
        # Fetch all books and their authors from the graph
        books = self.db.run_query(
            """
            MATCH (b:Book)-[:`WRITTEN_BY`]->(a:Author)
            RETURN b.id AS id, b.name AS name, a.name AS author
            """
        )

        for book in books:
            book_id = book["id"]
            title = book["name"]
            author = book["author"]

            print(f"Processing book: '{title}' by '{author}'...")
            self.add_similarity_relationships(book_id, title, author)

    def close(self):
        """
        Close the connection to the database.
        """
        self.db.close()
