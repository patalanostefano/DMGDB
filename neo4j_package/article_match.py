from neo4j import GraphDatabase
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ArticleMatcher:
    def __init__(self, neo4j_config):
        self.driver = GraphDatabase.driver(
            neo4j_config['url'],
            database=neo4j_config['database'],
            auth=(neo4j_config['user'], neo4j_config['password'])
        )

    def close(self):
        self.driver.close()

    def find_best_match(self, doc_name, article_number):
        try:
            with self.driver.session() as session:
                escaped_name = doc_name.replace('"', '\\"').strip()
                query_string = f'"{escaped_name}"'

                query = """
                    CALL db.index.fulltext.queryNodes('documentName', $query_str) 
                    YIELD node, score
                    WHERE node:Document 
                    AND node.nome_legge IS NOT NULL
                    AND score > 0.8  // Strict score threshold
                    RETURN node.nome_legge AS nome_legge, 
                        node.elementId AS document_id,
                        score
                    ORDER BY score DESC
                    LIMIT 1
                """
                
                result = session.run(query, query_str=query_string)
                record = result.single()

                if not record:
                    logger.info(f"No strong match found for document: {doc_name} (threshold not met)")
                    return None, None

                document_name = record["nome_legge"]
                document_id = record["document_id"]
                match_score = record["score"]
                
                logger.info(f"""
                    Match found:
                    Original: {doc_name}
                    Matched: {document_name}
                    Score: {match_score}
                """)

                # Only proceed if we have a very strong match
                if match_score > 0.8:
                    # Find the exact matching article
                    article_query = """
                        MATCH (d:Document {elementId: $doc_id})-[:HAS*]->(a:contenuto)
                        WHERE a.titolo = 'Art. ' + $art_num
                        OR a.titolo = 'Articolo ' + $art_num
                        RETURN a.elementId AS article_id
                        LIMIT 1
                    """
                    article_result = session.run(article_query, 
                                            doc_id=document_id,
                                            art_num=str(article_number))
                    
                    article = article_result.single()

                    if article:
                        logger.info(f"Found matching article: {article['article_id']}")
                        return document_id, article["article_id"]
                    
                    logger.info(f"No matching article found for number: {article_number}")
                    return document_id, None
                
                logger.info(f"Match score too low: {match_score}")
                return None, None

        except Exception as e:
            logger.error(f"Error in find_best_match: {str(e)}")
            return None, None



    def create_related_relationship(self, source_id, target_id):
        with self.driver.session() as session:
            session.run("""
                MATCH (source:contenuto {id: $source_id})
                MATCH (target:contenuto {id: $target_id})
                MERGE (source)-[:RELATED]->(target)
            """, source_id=source_id, target_id=target_id)
