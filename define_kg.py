import csv
from neo4j_manager import Neo4jConnector


class GraphCreator:
    def __init__(self):
        self.db = None

    def connect_to_neo4j(self, uri, user, password):
        self.db = Neo4jConnector(uri, user, password)

    def generate_book_graph(self, filename):
        if self.db is not None:
            self.db.run_query("MATCH (n) DETACH DELETE n")
            with open(f"processed_data/{filename}.csv", "r", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    if not row["Name"]:
                        continue
                    # Create Book Node
                    self.db.run_query(
                        """
                        MERGE (b:Book {id: $id})
                        SET b.name = $name, 
                            b.rating = $rating, 
                            b.pagesNumber = $pages_number,
                            b.publishYear = $publish_year, 
                            b.publisher = $publisher,
                            b.language = $language,
                            b.description = CASE WHEN $description = "None" THEN null ELSE $description END
                        """,
                        {
                            "id": row["Id"],
                            "name": row["Name"],
                            "rating": float(row["Rating"]) if row["Rating"] else None,
                            "pages_number": (
                                int(row["pagesNumber"]) if row["pagesNumber"] else None
                            ),
                            "publish_year": (
                                int(row["PublishYear"]) if row["PublishYear"] else None
                            ),
                            "publisher": row["Publisher"],
                            "language": row["Language"],
                            "description": row.get("Description"),
                        },
                    )

                    # Create Author Node and Relationship
                    authors = row["Authors"].split(";")
                    for author in authors:
                        self.db.run_query(
                            """
                            MERGE (a:Author {name: $author_name})
                            MERGE (b:Book {id: $book_id})
                            MERGE (b)-[:WRITTEN_BY]->(a)
                            """,
                            {
                                "author_name": author.strip(),
                                "book_id": row["Id"],
                            },
                        )
                    print(f"Added book '{row['Name']}' to the graph.")
        else:
            print("Database is not connected")

    def add_ratings_to_graph(self, filename):
        if self.db is not None:
            # delete existing relationships and users
            self.db.run_query("MATCH (u:User)<-[r:REVIEWED_BY]-() DELETE r, u")
            self.db.run_query("MATCH (u:User) DELETE u")
            with open(f"processed_data/{filename}.csv", "r", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    # Check if the book exists
                    book_exists = self.db.run_query(
                        "MATCH (b:Book {name: $book_name}) RETURN b",
                        {"book_name": row["Name"]},
                        single=True,
                    )
                    # Skip if book does not exist in db
                    if not book_exists:
                        print(f"Book '{row['Name']}' not found in the graph. Skipping.")
                        continue
                    # Create User Node
                    self.db.run_query(
                        """
                        MERGE (u:User {id: $user_id})
                        """,
                        {"user_id": row["ID"]},
                    )

                    # Create REVIEWED_BY Relationship
                    self.db.run_query(
                        """
                        MATCH (b:Book {name: $book_name})
                        MATCH (u:User {id: $user_id})
                        MERGE (b)-[:REVIEWED_BY {rating: $rating, num_rating: $num_rating}]->(u)
                        """,
                        {
                            "book_name": row["Name"],
                            "user_id": row["ID"],
                            "rating": row["Rating"],
                            "num_rating": row["NumericalRating"],
                        },
                    )
                    print(f"Added rating for book '{row['Name']}' to the graph.")

        else:
            print("Database is not connected")

    def disconnect_from_neo4j(self):
        self.db.close()
        self.db = None
