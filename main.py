from DBPedia_integration import DBpediaEnrichment
from preprocess_data import DataProcessor
import os
from dotenv import load_dotenv
from define_kg import GraphCreator
from LLM_integration import LLMGraphEnrichment

# Load environment variables from .env file
load_dotenv()
neo4j_uri = os.getenv("NEO4J_URI")
neo4j_username = os.getenv("NEO4J_USERNAME")
neo4j_password = os.getenv("NEO4J_PASSWORD")
openai_api_key = os.getenv("OPENAI_API_KEY")


def main():
    
    # PROCESS DATA
    
    book_processor = DataProcessor(fileoutput="cleaned_books-small")
    book_processor.reset_data()
    book_processor.process_books(filename="book-small")
    book_processor.process_books(filename="book700k-800k")
    book_processor.process_books(filename="book1-100k")
    for i in range(1, 20):
        book_processor.process_books(filename=f"book{i}00k-{i+1}00k")

    rating_processor = DataProcessor(fileoutput="cleaned_ratings")
    rating_processor.reset_data()
    for i in range(0, 6000, 1000):
        rating_processor.process_ratings(filename=f"user_rating_{i}_to_{i+1000}")
    rating_processor.process_ratings(filename="user_rating_6000_to_11000")

    # DEFINE KNOWLEDGE GRAPH
    
    graph_creator = GraphCreator()
    graph_creator.connect_to_neo4j(neo4j_uri, neo4j_username, neo4j_password)
    graph_creator.generate_book_graph("cleaned_books-small")
    graph_creator.add_ratings_to_graph("cleaned_ratings")
    graph_creator.disconnect_from_neo4j()

    # ENRICH KNOWLEDGE GRAPH WITH LLM
    
    LLM_graph_enrichment = LLMGraphEnrichment(
        neo4j_uri, neo4j_username, neo4j_password, openai_api_key
    )
    try:
        LLM_graph_enrichment.enrich_with_LLM()
    finally:
        LLM_graph_enrichment.close()

    # ENRICH KNOWLEDGE GRAPH WITH DBPEDIA
    
    dbpedia_enrichment = DBpediaEnrichment(
        neo4j_uri=neo4j_uri,
        neo4j_user=neo4j_username,
        neo4j_password=neo4j_password,
    )
    try:
        dbpedia_enrichment.enrich_graph_with_dbpedia()
    finally:
        dbpedia_enrichment.close()
    

if __name__ == "__main__":
    main()
