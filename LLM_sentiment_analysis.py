from neo4j_manager import Neo4jConnector
import openai


class SentimentAnalysisUpdater:
    def __init__(self, neo4j_uri, neo4j_user, neo4j_password, openai_api_key):
        # Initialize Neo4j and OpenAI API
        self.db = Neo4jConnector(neo4j_uri, neo4j_user, neo4j_password)
        openai.api_key = openai_api_key

    def close(self):
        # Close the Neo4j connection
        self.db.close()

    def fetch_reviews(self):
        # Fetch user reviews from the graph
        query = """
        MATCH (b:Book)<-[r:REVIEWED_BY]-(u:User)
        WHERE NOT EXISTS(r.sentiment)  // Only process reviews without sentiment
        RETURN b.id AS book_id, u.id AS user_id, r.rating AS rating, r.review AS review
        """
        with self.db.driver.session() as session:
            result = session.run(query)
            return result

    def analyze_sentiment(self, text):
        # Perform sentiment analysis using OpenAI
        response = openai.Completion.create(
            engine="text-davinci-003",
            prompt=f"Analyze the sentiment of this review: {text}",
            max_tokens=60,
        )
        sentiment = response.choices[0].text.strip()
        return sentiment

    def update_sentiment(self, book_id, user_id, sentiment):
        # Update the sentiment in the graph
        query = """
        MATCH (b:Book {id: $book_id})<-[r:REVIEWED_BY]-(u:User {id: $user_id})
        SET r.sentiment = $sentiment
        """
        self.db.run_query(
            query, {"book_id": book_id, "user_id": user_id, "sentiment": sentiment}
        )

    def process_reviews(self):
        # Process reviews to analyze and update sentiment
        reviews = self.fetch_reviews()
        for record in reviews:
            sentiment = self.analyze_sentiment(record["review"])
            self.update_sentiment(record["book_id"], record["user_id"], sentiment)