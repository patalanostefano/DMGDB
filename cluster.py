from neo4j import GraphDatabase
import logging
from models.citation_extractor import LegalCitationPipeline
from neo4j_package.article_match import ArticleMatcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def cluster(neo4j_config):
    driver = GraphDatabase.driver(
        neo4j_config['url'],
        database=neo4j_config['database'],
        auth=(neo4j_config['user'], neo4j_config['password'])
    )

    pipeline = LegalCitationPipeline()
    matcher = ArticleMatcher(neo4j_config)

    with driver.session() as session:
        logger.info("Starting clustering process...")

        # Fetch all contenuto nodes
        result = session.run("""
            MATCH (n:contenuto)
            RETURN n.elementId AS id, n.contenuto AS content
        """)

        relations_created = 0

        for record in result:
            node_id = record["id"]
            content = record["content"]

            if content:
                citations = pipeline.process_text(content)
                print('\ncitations: ',citations)

                for doc_name, article_number in citations:
                    matched_doc, matched_article_id = matcher.find_best_match(
                        doc_name, article_number)
                    print('\ndoc: ',matched_doc)
                    print('\nart: ',matched_article_id)
                    if matched_article_id:
                        matcher.create_related_relationship(
                            node_id, matched_article_id)
                        relations_created += 1

        logger.info(
            f"Clustering completed. {relations_created} RELATED relationships created based on article citations.")

        result = session.run("""
            MATCH ()-[r:RELATED]-()
            RETURN count(r) as total_relationships
        """)
        total_relationships = result.single()["total_relationships"]
        logger.info(
            f"Total RELATED relationships in the database: {total_relationships}")

    driver.close()
    matcher.close()


if __name__ == "__main__":
    neo4j_config = {
        'url': 'neo4j+ssc://bc1e22a5.databases.neo4j.io',
        'database': 'neo4j',
        'user': 'neo4j',
        'password': 'FgT82ozrijBRXCmi1oQNK5Le8DAHqz90-YYtHXLjLjk'
    }

    cluster(neo4j_config)
