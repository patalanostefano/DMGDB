from neo4j import GraphDatabase


def initialise_schema(neo4j_config):
    cypher_schema = [
        # Drop existing indexes if they exist
        "DROP INDEX chunkVectorIndex",
        "DROP INDEX document_nome_legge",
        "DROP INDEX chunk_text"

        # Create constraints
        "CREATE CONSTRAINT chunk_text_unique IF NOT EXISTS FOR (c:Chunk) REQUIRE c.text IS UNIQUE",
        "CREATE CONSTRAINT document_name_unique IF NOT EXISTS FOR (d:Document) REQUIRE d.name IS UNIQUE",

        # Create vector indexes
        "CALL db.index.vector.createNodeIndex('chunkVectorIndex', 'Chunk', 'embedding', 768, 'cosine')",
        "CALL db.index.vector.createNodeIndex('contenuto_embedding_index', 'contenuto', 'embedding', 768, 'cosine')",
        "CREATE TEXT INDEX document_nome_legge_index FOR (d:Document) ON EACH [d.nome_legge]",
        "CREATE TEXT INDEX contenuto_titolo_index FOR (c:contenuto) ON EACH [c.titolo]",
        "CREATE INDEX chunk_text_idx IF NOT EXISTS FOR (n:Chunk) ON (n.text)",
        "CREATE INDEX contenuto_contenuto_index IF NOT EXISTS FOR (n:contenuto) ON (n.contenuto)"
    ]

    driver = GraphDatabase.driver(
        neo4j_config['url'],
        auth=(neo4j_config['user'], neo4j_config['password'])
    )
    driver.verify_connectivity()
    with driver.session() as session:
        for cypher in cypher_schema:
            try:
                session.run(cypher)
                print(f"Executed: {cypher}")
            except Exception as e:
                print(f"Error executing: {cypher}")
                print(f"Error message: {str(e)}")

    driver.close()
    print("Neo4j schema initialization completed.")

