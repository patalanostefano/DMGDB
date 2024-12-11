from neo4j import GraphDatabase
from models.chunker import Chunker
import os
import glob
import logging
import uuid
import json
from models.chunker import Embedder
from models.citation_extractor import LegalCitationPipeline
from neo4j_package.article_match import ArticleMatcher


legal_embedder = Embedder(legal=True) 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

chunker = Chunker.get_instance()


def ingest_document_d(doc_location, neo4j_config):
    return _ingest_document(doc_location, neo4j_config, tag='d')


def _ingest_document(doc_location, neo4j_config, tag):
    driver = GraphDatabase.driver(
        neo4j_config['url'],
        database=neo4j_config['database'],
        auth=(neo4j_config['user'], neo4j_config['password'])
    )

    try:
        with driver.session() as session:
            name = os.path.basename(doc_location)
            chunks = chunker.chunk_text(doc_location)

            logger.info(f"Processing document: {name}")
            logger.info(f"Number of chunks: {len(chunks)}")
            logger.info(
                f"First chunk structure: {chunks[0] if chunks else 'No chunks'}")

            # Create Document node with tag
            session.run(
                "MERGE (d:Document {name: $name}) SET d.tag = $tag",
                name=name, tag=tag
            )

            previous_chunk_id = None
            for i, chunk in enumerate(chunks):
                try:
                    chunk_text = chunk['text']
                    chunk_embedding = chunk['embedding']
                    chunk_entities = chunk['entities']

                    logger.info(
                        f"Processing chunk {i+1}/{len(chunks)} for document {name}")
                    logger.info(f"Chunk text: {chunk_text[:100]}...")
                    logger.info(
                        f"Number of entities in chunk: {len(chunk_entities)}")

                    if not chunk_text or not chunk_embedding:
                        logger.warning(
                            f"Invalid chunk found in document {name}. Skipping.")
                        continue

                    # Create Chunk node with unique chunk_id and link to Document or previous Chunk
                    unique_chunk_id = str(uuid.uuid4())
                    if i == 0:
                        result = session.run(
                            """
                            CREATE (c:Chunk {text: $text, chunk_id: $chunk_id})
                            SET c.embedding = $embedding
                            WITH c
                            MATCH (d:Document {name: $doc_name})
                            CREATE (d)-[:NEXT]->(c)
                            RETURN elementId(c) as neo4j_id
                            """,
                            doc_name=name, text=chunk_text, embedding=chunk_embedding, chunk_id=unique_chunk_id
                        )
                    else:
                        result = session.run(
                            """
                            CREATE (c:Chunk {text: $text, chunk_id: $chunk_id})
                            SET c.embedding = $embedding
                            WITH c
                            MATCH (prev:Chunk)
                            WHERE elementId(prev) = $prev_id
                            CREATE (prev)-[:NEXT]->(c)
                            RETURN elementId(c) as neo4j_id
                            """,
                            text=chunk_text, embedding=chunk_embedding, prev_id=previous_chunk_id, chunk_id=unique_chunk_id
                        )

                    neo4j_id = result.single()["neo4j_id"]
                    logger.info(
                        f"Created Chunk node with ID: {neo4j_id} and unique chunk_id: {unique_chunk_id}")

                    # Create Entity nodes and link them to the Chunk
                    for entity in chunk_entities:
                        entity_text = entity['text']
                        entity_label = entity['label']

                        session.run(
                            """
                            MERGE (e:Entity {text: $text, label: $label})
                            WITH e
                            MATCH (c:Chunk {chunk_id: $chunk_id})
                            MERGE (c)-[:HAS_ENTITY]->(e)
                            """,
                            text=entity_text, label=entity_label, chunk_id=unique_chunk_id
                        )

                    logger.info(
                        f"Processed {len(chunk_entities)} entities for chunk {i+1}")
                    previous_chunk_id = neo4j_id

                except Exception as e:
                    logger.error(
                        f"Error processing chunk {i} of document {name}: {str(e)}")

            logger.info(f"Document '{name}' ingestion completed.")

    except Exception as e:
        logger.error(f"Error ingesting document {doc_location}: {str(e)}")

    finally:
        driver.close()

    logger.info(f"Document '{name}' ingested successfully with tag '{tag}'.")

def related_intentional(neo4j_config):
    driver = GraphDatabase.driver(
        neo4j_config['url'],
        database=neo4j_config['database'],
        auth=(neo4j_config['user'], neo4j_config['password'])
    )

    pipeline = LegalCitationPipeline()
    matcher = ArticleMatcher(neo4j_config)
    
    related_dir = "data/related"
    jsonl_files = glob.glob(os.path.join(related_dir, '*.jsonl'))
    
    logger.info(f"Found {len(jsonl_files)} .jsonl files to process")
    
    with driver.session() as session:
        for jsonl_file in jsonl_files:
            logger.info(f"Processing file: {jsonl_file}")
            
            with open(jsonl_file, 'r', encoding='utf-8') as file:
                for line in file:
                    try:
                        data = json.loads(line)
                        article_number = data.get('article_number')
                        text_content = data.get('text')
                        
                        if not article_number or not text_content:
                            continue
                        
                        article_title = f"Art. {article_number}"
                        
                        # Find the corresponding contenuto node
                        result = session.run("""
                            MATCH (d:Document)-[:HAS*]->(c:contenuto)
                            WHERE d.nome_legge = 'Codice Civile' 
                            AND c.titolo = $article_title
                            AND NOT (c)-[:HAS]->()
                            RETURN elementId(c) as source_id
                        """, article_title=article_title)
                        
                        record = result.single()
                        if not record:
                            logger.warning(f"No matching node found for article {article_title}")
                            continue
                            
                        source_id = record["source_id"]
                        
                        # Process text content to find citations
                        citations = pipeline.process_text(text_content)
                        
                        for doc_name, cited_article_number in citations:
                            matched_doc, matched_article_id = matcher.find_best_match(
                                doc_name, cited_article_number)
                            
                            if matched_article_id:
                                session.run("""
                                    MATCH (source), (target)
                                    WHERE elementId(source) = $source_id 
                                    AND elementId(target) = $target_id
                                    MERGE (source)-[r:RELATED]->(target)
                                    SET r.text = $text_content
                                """, 
                                    source_id=source_id, 
                                    target_id=matched_article_id,
                                    text_content=text_content
                                )
                                
                                logger.info(f"Created RELATED relationship with text content: {article_title} -> {cited_article_number}")
                    
                    except json.JSONDecodeError as e:
                        logger.error(f"Error parsing JSON line: {e}")
                    except Exception as e:
                        logger.error(f"Error processing line: {e}")
    
    driver.close()
    logger.info("Completed processing intentional relations from .jsonl files")



def process_contenuto_nodes(neo4j_config):
    driver = GraphDatabase.driver(
        neo4j_config['url'],
        database=neo4j_config['database'],
        auth=(neo4j_config['user'], neo4j_config['password'])
    )

    with driver.session() as session:
        # Process all nodes with 'rubrica' field
        result = session.run("""
        MATCH (n:contenuto)
        RETURN elementId(n) as id, 
               n.rubrica as rubrica,
               n.contenuto as contenuto
        """)

        for record in result:
            node_id = record['id']
            rubrica = record['rubrica']
            contenuto = record['contenuto']
            
            # If contenuto exists, use it for embedding, otherwise use rubrica
            text_to_embed = contenuto if contenuto is not None else rubrica
            embedding = legal_embedder.get_embedding(text_to_embed)
            
            session.run("""
            MATCH (n)
            WHERE elementId(n) = $node_id
            SET n.embedding = $embedding
            """, node_id=node_id, embedding=embedding)

        # Process parti nodes
        result = session.run("""
        MATCH (n:parti)
        WHERE n.rubrica IS NOT NULL
        RETURN elementId(n) as id, n.rubrica as content
        """)

        for record in result:
            node_id = record['id']
            content = record['content']
            
            embedding = legal_embedder.get_embedding(content)
            
            session.run("""
            MATCH (c:contenuto)
            WHERE elementId(c) = $node_id
            SET c.embedding = $embedding
            """, node_id=node_id, embedding=embedding)

    driver.close()
    logger.info("Processed nodes with embeddings.")


def load_docs(neo4j_config):
    file_location = "data/docs"
    pdf_files = glob.glob(os.path.join(file_location, '*.pdf'))

    print(f'#PDF files found: {len(pdf_files)}!')

    for pdf_file in pdf_files:
        ingest_document_d(pdf_file, neo4j_config)


def create_kb_nodes(session, data, parent_node=None, parent_label=None):
    for key, value in data.items():
        if isinstance(value, dict):
            if key == "Document":
                node_query = """
                MERGE (n:Document {nome_legge: $nome_legge})
                RETURN n
                """
                result = session.run(
                    node_query, nome_legge=value.get("nome_legge", "Unknown"))
            else:
                node_query = """
                CREATE (n:{})
                RETURN n
                """.format(key)
                result = session.run(node_query)

            node = result.single()["n"]

            for prop_key, prop_value in value.items():
                if not isinstance(prop_value, (dict, list)):
                    session.run(f"MATCH (n:{key}) WHERE elementId(n) = $id SET n.{prop_key} = $value",
                                id=node.element_id, value=prop_value)

            # Link to parent node if it exists
            if parent_node and parent_label:
                link_query = f"""
                MATCH (parent:{parent_label}), (child:{key})
                WHERE elementId(parent) = $parent_id AND elementId(child) = $child_id
                CREATE (parent)-[:HAS]->(child)
                """
                session.run(link_query, parent_id=parent_node.element_id,
                            child_id=node.element_id)

            # Recursively create nodes for nested dictionaries
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, (dict, list)):
                    create_kb_nodes(session, {sub_key: sub_value}, node, key)

        elif isinstance(value, list):
            # Create nodes for list items
            for item in value:
                if isinstance(item, dict):
                    create_kb_nodes(
                        session, {key: item}, parent_node, parent_label)


def ingest_kb_structure(file_path, neo4j_config):
    with open(file_path, 'r', encoding='utf-8') as file:
        try:
            data = json.load(file)
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON file: {file_path}. Skipping.")
            return

    driver = GraphDatabase.driver(
        neo4j_config['url'],
        database=neo4j_config['database'],
        auth=(neo4j_config['user'], neo4j_config['password'])
    )

    with driver.session() as session:
        create_kb_nodes(session, data)

    driver.close()
    logger.info(f"KB structure from '{file_path}' ingested successfully.")

    process_contenuto_nodes(neo4j_config)


def load_kb(neo4j_config):
    file_location = "data/kb"
    json_files = glob.glob(os.path.join(file_location, '*.json'))

    print(f'#JSON files found: {len(json_files)}!')

    for json_file in json_files:
        ingest_kb_structure(json_file, neo4j_config)

    logger.info("KB ingestion and processing completed.")


if __name__ == "__main__":
    neo4j_config = {
        'url': 'neo4j+ssc://bc1e22a5.databases.neo4j.io',
        'database': 'neo4j',
        'user': 'neo4j',
        'password': 'FgT82ozrijBRXCmi1oQNK5Le8DAHqz90-YYtHXLjLjk'
    }
    #load_docs(neo4j_config)
    #load_kb(neo4j_config)
    #process_contenuto_nodes(neo4j_config)
    #related_intentional(neo4j_config)
