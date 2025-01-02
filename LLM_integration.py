from openai import OpenAI
from neo4j_manager import Neo4jConnector


class LLMGraphEnrichment:
    def __init__(self, neo4j_uri, neo4j_user, neo4j_password, openai_api_key):
        """
        Initialize the class with Neo4j connection details and OpenAI API key.
        """
        self.db = Neo4jConnector(neo4j_uri, neo4j_user, neo4j_password)
        self.client = OpenAI(api_key=openai_api_key)

    def get_description_from_llm(self, title, author):
        """
        Use an LLM to get the description of a book based on the title and author.
        """
        prompt = f"""
            Provide a concise description for the book titled "{title}" by "{author}".
            If you do not recognize the book or cannot provide a description, respond with "none".
            """
        try:
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo", messages=[{"role": "user", "content": prompt}]
            )
            result = response.choices[0].message["content"].strip()
            if result.lower() == "none":
                return None
            return result
        except Exception as e:
            print(f"Error querying LLM for description: {e}")
            return None

    def add_description_to_book(self, book_id, title, author):
        """
        Add the description attribute to a book node in the graph if not already available.
        """
        try:
            # Check if the book already has a description
            result = self.db.run_query(
                "MATCH (b:Book {id: $book_id}) RETURN b.description AS description",
                {"book_id": book_id},
                single=True,
            )
            if result and result.get("description"):
                print(f"Book '{title}' already has a description.")
                return False  # Description already exists

            # Fetch description from the LLM
            description = self.get_description_from_llm(title, author)
            if not description:
                print(f"No description found for '{title}' by '{author}'.")
                return False

            # Update the book node with the description
            self.db.run_query(
                "MATCH (b:Book {id: $book_id}) SET b.description = $description",
                {"book_id": book_id, "description": description},
            )
            print(f"Added description to '{title}'.")
            return True  # Description added
        except Exception as e:
            print(f"Error adding description to book: {e}")
            return False

    def add_attributes_from_llm(self, book_id, title, author, description):
        """
        Use the LLM to extract attributes (e.g., genre) and add them to the graph.
        """
        if not description or description.strip() == "":
            print(f"Skipping attributes for '{title}' as description is not available.")
            return

        prompt = f"""
            Analyze the following description of a book titled "{title}" by "{author}":
            "{description}"
            Extract the genre, themes, and target audience of the book. Provide them in the format:
            Genre: [genre], Themes: [themes], Audience: [audience].
            If you cannot determine any of these attributes, use "unknown".
            """

        try:
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo", messages=[{"role": "user", "content": prompt}]
            )
            result = response.choices[0].message["content"].strip()
            attributes = {
                "genre": "unknown",
                "themes": "unknown",
                "audience": "unknown",
            }

            # Parse the response for attributes
            for line in result.split("\n"):
                if "Genre:" in line:
                    attributes["genre"] = line.replace("Genre:", "").strip()
                elif "Themes:" in line:
                    attributes["themes"] = line.replace("Themes:", "").strip()
                elif "Audience:" in line:
                    attributes["audience"] = line.replace("Audience:", "").strip()

            # Add attributes to the book node
            self.db.run_query(
                """
                MATCH (b:Book {id: $book_id})
                SET b.genre = $genre, b.themes = $themes, b.audience = $audience
                """,
                {
                    "book_id": book_id,
                    "genre": attributes["genre"],
                    "themes": attributes["themes"],
                    "audience": attributes["audience"],
                },
            )
            print(f"Added attributes to '{title}': {attributes}.")
        except Exception as e:
            print(f"Error adding attributes to book: {e}")

    def add_similarity_relationships(self, book_id, title, description):
        """
        Use the description to find similar books and add SIMILAR_TO relationships.
        """
        if not description or description.strip() == "":
            prompt = f"""
            Given the book title "{title}", suggest up to 3 similar books.
            If you do not recognize the book or cannot find similar ones, respond with "none".
            Provide only the titles of similar books as a comma-separated list or "none".
            """
        else:
            prompt = f"""
                Based on the following description of the book titled "{title}":
                "{description}"
                Suggest up to 3 similar books. Provide only the titles of similar books as a comma-separated list.
                If you cannot find similar books, respond with "none".
                """
        try:
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo", messages=[{"role": "user", "content": prompt}]
            )
            result = response.choices[0].message["content"].strip()
            if result.lower() == "none":
                print(f"No similar books found for '{title}'.")
                return

            similar_books = [book.strip() for book in result.split(",") if book.strip()]
            for similar_title in similar_books:
                # Check if the similar book exists in the graph
                result = self.db.run_query(
                    "MATCH (b:Book {name: $title}) RETURN b.id AS id",
                    {"title": similar_title},
                    single=True,
                )

                if result:
                    similar_book_id = result["id"]

                    # Add the similarity relationship if it doesn't already exist
                    relationship_check = self.db.run_query(
                        """
                        MATCH (b1:Book {id: $book_id})-[r:SIMILAR_TO]->(b2:Book {id: $similar_book_id})
                        RETURN r
                        """,
                        {"book_id": book_id, "similar_book_id": similar_book_id},
                        single=True,
                    )

                    if not relationship_check:
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

    def enrich_with_LLM(self):
        """
        Iterate through all books in the graph and enrich them with descriptions, attributes, and relationships.
        """
        books = self.db.run_query(
            """
            MATCH (b:Book)-[:`WRITTEN_BY`]->(a:Author)
            RETURN b.id AS id, b.name AS name, a.name AS author, b.description AS description
            """
        )

        for book in books:
            book_id = book["id"]
            title = book["name"]
            author = book["author"]
            description = book.get("description")

            print(f"Processing book: '{title}' by '{author}'...")

            result = None  # Initialize result
            # Add description if missing
            if not description:
                if self.add_description_to_book(book_id, title, author):
                    # Fetch updated description
                    result = self.db.run_query(
                        "MATCH (b:Book {id: $book_id}) RETURN b.description AS description",
                        {"book_id": book_id},
                        single=True,
                    )
                description = result["description"] if result else None

            # Add attributes and relationships
            self.add_attributes_from_llm(book_id, title, author, description)
            self.add_similarity_relationships(book_id, title, description)

    def close(self):
        """
        Close the connection to the database.
        """
        self.db.close()
