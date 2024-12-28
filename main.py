from preprocess_data import DataProcessor
import os
from dotenv import load_dotenv
from define_kg import GraphCreator
from LLM_sentiment_analysis import SentimentAnalysisUpdater

# Load environment variables from .env file
load_dotenv()


def main():
    # Process data
    # book_processor = DataProcessor(fileoutput="cleaned_books-small")
    # book_processor.reset_data()
    # book_processor.process_books(filename="book-small")

    # rating_processor = DataProcessor(fileoutput="cleaned_ratings")
    # rating_processor.reset_data()
    # for i in range(0, 6000, 1000):
    #     rating_processor.process_ratings(filename=f"user_rating_{i}_to_{i+1000}")
    # rating_processor.process_ratings(filename="user_rating_6000_to_11000")

    # Define knowledge graph
    graph_creator = GraphCreator()
    
    neo4j_uri = os.getenv("NEO4J_URI")
    neo4j_username = os.getenv("NEO4J_USERNAME")
    neo4j_password = os.getenv("NEO4J_PASSWORD")

    graph_creator.connect_to_neo4j(neo4j_uri, neo4j_username, neo4j_password)
    # graph_creator.generate_book_graph("cleaned_books-small")
    graph_creator.add_ratings_to_graph("cleaned_ratings")
    graph_creator.disconnect_from_neo4j()

    # # Perform sentiment analysis
    # openai_api_key = os.getenv("OPENAI_API_KEY")

    # sentiment_updater = SentimentAnalysisUpdater(
    # neo4j_uri, neo4j_username, neo4j_password, openai_api_key
    # )
    # sentiment_updater.process_reviews()
    # sentiment_updater.close()


if __name__ == "__main__":
    main()
