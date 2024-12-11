from typing import Optional, List, Dict
from neo4j import GraphDatabase
from chunker import Embedder

class GraphSearcher:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.embeddings = Embedder()
        self.legal_embeddings = Embedder(legal=True)

    def search_by_embedding(self, doc_name: Optional[str], query: str, limit: int) -> List[Dict]:
        embedding = self.embeddings.get_embedding(query)

        if doc_name:
            cypher_query = """
            MATCH (d:Document {name: $doc_name})-[:NEXT*]->(c:Chunk)
            WITH c, d
            CALL db.index.vector.queryNodes('chunkVectorIndex', 400, $embedding) YIELD node, score
            WHERE node = c
            WITH DISTINCT c, score, d
            OPTIONAL MATCH (prev)-[:NEXT]->(c)
            OPTIONAL MATCH (c)-[:NEXT]->(next)
            OPTIONAL MATCH (c)-[:HAS_ENTITY]->(e)
            OPTIONAL MATCH (c)-[:RELATED]->(related)
            RETURN c.text AS text,
                   c.chunk_id AS chunk_id,
                   score,
                   d.name AS source,
                   collect(DISTINCT prev.text) AS prev_chunks,
                   collect(DISTINCT next.text) AS next_chunks,
                   collect(DISTINCT {text: e.text, label: e.label}) AS entities,
                   collect(DISTINCT related) AS related_nodes
            ORDER BY score DESC
            LIMIT $limit
            """
            params = {
                "doc_name": doc_name,
                "embedding": embedding,
                "limit": limit
            }
            return self._execute_single_doc_query(cypher_query, params)
        else:
            cypher_query = """
            MATCH (d:Document)-[:NEXT*]->(c:Chunk)
            WITH c, d
            CALL db.index.vector.queryNodes('chunkVectorIndex', 400, $embedding) YIELD node, score
            WHERE node = c
            WITH DISTINCT c, score, d
            OPTIONAL MATCH (prev)-[:NEXT]->(c)
            OPTIONAL MATCH (c)-[:NEXT]->(next)
            OPTIONAL MATCH (c)-[:HAS_ENTITY]->(e)
            OPTIONAL MATCH (c)-[:RELATED]->(related)
            RETURN c.text AS text,
                   c.chunk_id AS chunk_id,
                   score,
                   d.name AS source,
                   collect(DISTINCT prev.text) AS prev_chunks,
                   collect(DISTINCT next.text) AS next_chunks,
                   collect(DISTINCT {text: e.text, label: e.label}) AS entities,
                   collect(DISTINCT related) AS related_nodes
            ORDER BY score DESC
            LIMIT $limit
            """
            params = {
                "embedding": embedding,
                "limit": limit
            }
            return self._execute_query(cypher_query, params)

    
    def search_by_text(self, doc_name: Optional[str], text: str, limit: int) -> List[Dict]:
        search_text = text.lower()  
        if doc_name:
            cypher_query = """
            CALL db.index.fulltext.queryNodes("chunk_text_idx", $search_text) YIELD node as c, score
            MATCH (d:Document {name: $doc_name})-[:NEXT*]->(c)
            WITH c, d, score
            ORDER BY score DESC
            WITH DISTINCT c, d, score
            OPTIONAL MATCH (prev)-[:NEXT]->(c)
            OPTIONAL MATCH (c)-[:NEXT]->(next)
            OPTIONAL MATCH (c)-[:HAS_ENTITY]->(e)
            OPTIONAL MATCH (c)-[:RELATED]->(related)
            RETURN c.text AS text,
                c.chunk_id AS chunk_id,
                score,
                d.name AS source,
                collect(DISTINCT prev.text) AS prev_chunks,
                collect(DISTINCT next.text) AS next_chunks,
                collect(DISTINCT {text: e.text, label: e.label}) AS entities,
                collect(DISTINCT related) AS related_nodes
            ORDER BY score DESC
            LIMIT $limit
            """
            params = {
                "doc_name": doc_name,
                "search_text": search_text,
                "limit": limit
            }
            return self._execute_single_doc_query(cypher_query, params)
        else:
            cypher_query = """
            CALL db.index.fulltext.queryNodes("chunk_text_idx", $search_text) YIELD node as c, score
            MATCH (d:Document)-[:NEXT*]->(c)
            WITH c, d, score
            ORDER BY score DESC
            WITH DISTINCT c, d, score
            OPTIONAL MATCH (prev)-[:NEXT]->(c)
            OPTIONAL MATCH (c)-[:NEXT]->(next)
            OPTIONAL MATCH (c)-[:HAS_ENTITY]->(e)
            OPTIONAL MATCH (c)-[:RELATED]->(related)
            RETURN c.text AS text,
                c.chunk_id AS chunk_id,
                score,
                d.name AS source,
                collect(DISTINCT prev.text) AS prev_chunks,
                collect(DISTINCT next.text) AS next_chunks,
                collect(DISTINCT {text: e.text, label: e.label}) AS entities,
                collect(DISTINCT related) AS related_nodes
            ORDER BY score DESC
            LIMIT $limit
            """
            params = {
                "search_text": search_text,
                "limit": limit
            }
            return self._execute_query(cypher_query, params)


    def search_by_category(self, doc_name: Optional[str], category: str, limit: int) -> List[Dict]:
        cypher_query = """
        MATCH (c)-[:HAS_ENTITY]->(e:Entity)
        WHERE toLower(e.label) = toLower($category)
        """
        if doc_name:
            cypher_query += """
            MATCH (d:Document {name: $doc_name})-[:NEXT*]->(c:Chunk)
            """
        cypher_query += """
        WITH c, e, d.name AS doc_name
        WITH DISTINCT c, e, doc_name
        OPTIONAL MATCH (prev)-[:NEXT]->(c)
        OPTIONAL MATCH (c)-[:NEXT]->(next)
        OPTIONAL MATCH (c)-[:RELATED]->(related)
        RETURN c.text AS text,
               c.chunk_id AS chunk_id,
               1.0 AS score,
               doc_name AS source,
               collect(DISTINCT prev.text) AS prev_chunks,
               collect(DISTINCT next.text) AS next_chunks,
               collect(DISTINCT {text: e.text, label: e.label}) AS entities,
               collect(DISTINCT related) AS related_nodes
        LIMIT $limit
        """
        
        params = {
            "doc_name": doc_name,
            "category": category,
            "limit": limit
        }

        if doc_name:
            return self._execute_single_doc_query(cypher_query, params)
        else:
            return self._execute_query(cypher_query, params)

    def _execute_single_doc_query(self, cypher_query: str, params: Dict) -> List[Dict]:
        results = []
        with self.driver.session() as session:
            raw_results = list(session.run(cypher_query, params))

            for record in raw_results:
                related_nodes = record.get('related_nodes', [])
                processed_related = []
                for related in related_nodes:
                    if related.labels and 'contenuto' in related.labels:
                        processed_related.append({
                            'text': related.get('contenuto', '')
                        })
                    else:
                        
                        pass

                chunk = {
                    'text': record['text'],
                    'chunk_id': record['chunk_id'],
                    'similarity': record['score'],
                    'source': record['source'],
                    'prev_chunks': record['prev_chunks'],
                    'next_chunks': record['next_chunks'],
                    'entities': record['entities'],
                    'related': processed_related
                }
                results.append(chunk)

        return results

    def _execute_query(self, cypher_query: str, params: Dict) -> List[Dict]:
        results = []
        with self.driver.session() as session:
            raw_results = list(session.run(cypher_query, params))

            for record in raw_results:
                related_nodes = record.get('related_nodes', [])
                processed_related = []
                for related in related_nodes:
                    if related.labels and 'contenuto' in related.labels:
                        processed_related.append({
                            'text': related.get('contenuto', '')
                        })
                    else:
                        
                        pass

                chunk = {
                    'text': record['text'],
                    'chunk_id': record['chunk_id'],
                    'similarity': record['score'],
                    'source': record['source'],
                    'prev_chunks': record['prev_chunks'],
                    'next_chunks': record['next_chunks'],
                    'entities': record['entities'],
                    'related': processed_related
                }
                results.append(chunk)

        return results



    
    def search_by_article(self, article_number: int, law_name: Optional[str] = None) -> List[Dict]:
        """
        Search for a specific article in a law document.
        """
        article_title = f"Art. {article_number}"
        cypher_query = """
        MATCH (d:Document)-[:HAS*]->(c:contenuto)
        WHERE c.titolo = $article_title
        AND NOT (c)-[:HAS]->()  
        """
        if law_name:
            cypher_query += """
            AND d.nome_legge = $law_name
            """
        cypher_query += """
        RETURN c.titolo AS titolo,
               c.rubrica AS rubrica,
               c.contenuto AS contenuto,
               d.nome_legge AS law_name
        LIMIT 1
        """
        params = {
            "article_title": article_title,
            "law_name": law_name
        }
        with self.driver.session() as session:
            result = session.run(cypher_query, params).single()
            if result:
                return [{
                    'titolo': result['titolo'],
                    'rubrica': result['rubrica'],
                    'contenuto': result['contenuto'],
                    'law_name': result['law_name']
                }]
            else:
                return []

    def wide_search(self, query: str, law_name: Optional[str] = None) -> List[Dict]:
        """
        Perform a wide BFS-style search with three types of searches:
        1. BFS using embedding similarity among child nodes
        2. Direct embedding search among leaf nodes
        3. Direct text search among leaf nodes
        """
        query_embedding = self.legal_embeddings.get_embedding(query)
        
        results = []
        visited = set()
        queue = []

        with self.driver.session() as session:
            start_query = """
            MATCH (d:Document)
            WHERE CASE 
                WHEN $law_name IS NOT NULL THEN d.nome_legge = $law_name 
                ELSE true 
            END
            CALL {
                WITH d
                MATCH (d)-[:HAS*]->(n:parti)
                RETURN n as startNode
                UNION
                WITH d
                MATCH (d)-[:HAS*]->(n:contenuto)
                WHERE n.titolo STARTS WITH 'TITOLO'
                RETURN n as startNode
            }
            RETURN DISTINCT startNode
            """
            
            start_nodes = session.run(start_query, {"law_name": law_name})
            
            # Initialize queue with start nodes
            for record in start_nodes:
                node = record['startNode']
                if node.id not in visited:
                    visited.add(node.id)
                    queue.append(node)


            # 1. BFS with embedding similarity
            bfs_result = None
            while queue and not bfs_result:
                current_node = queue.pop(0)
                
                # Get children and compare using vector index
                child_query = """
                MATCH (parent)-[:HAS]->(child:contenuto)
                WHERE id(parent) = $parent_id
                WITH child
                WHERE exists(child.embedding)
                CALL db.index.vector.queryNodes('contenuto_embedding_index', $query_embedding, 1)
                YIELD node, score
                WHERE node = child
                RETURN node, score
                ORDER BY score DESC
                LIMIT 1
                """
                children = session.run(child_query, {
                    "parent_id": current_node.id,
                    "query_embedding": query_embedding
                })

                for child_record in children:
                    child = child_record['node']
                    if child.id in visited:
                        continue
                    visited.add(child.id)

                    # Check if it's a leaf node (has contenuto field)
                    if 'contenuto' in child:
                        bfs_result = {
                            'source': child.get('source', ''),
                            'titolo': child.get('titolo', ''),
                            'contenuto': child['contenuto'],
                            'similarity': child_record['score']
                        }
                        break
                    else:
                        queue.append(child)

            # 2. Direct embedding search among leaves
            embedding_query = """
            MATCH (c:contenuto)
            WHERE exists(c.contenuto)
            CALL db.index.vector.queryNodes('contenuto_embedding_index', $query_embedding, 1)
            YIELD node, score
            RETURN node, score
            ORDER BY score DESC
            LIMIT 1
            """
            embedding_result = session.run(embedding_query, {"query_embedding": query_embedding}).single()
            
            # 3. Direct text search among leaves
            text_query = """
            CALL db.index.fulltext.queryNodes('contenuto_contenuto_index', $query)
            YIELD node, score
            WHERE exists(node.contenuto)
            RETURN node, score
            ORDER BY score DESC
            LIMIT 1
            """
            text_result = session.run(text_query, {"query": query}).single()

            # Combine results
            final_results = []
            
            if bfs_result:
                final_results.append(bfs_result)
                
            if embedding_result:
                node = embedding_result['node']
                final_results.append({
                    'source': node.get('source', ''),
                    'titolo': node.get('titolo', ''),
                    'contenuto': node['contenuto'],
                    'similarity': embedding_result['score']
                })
                
            if text_result:
                node = text_result['node']
                final_results.append({
                    'source': node.get('source', ''),
                    'titolo': node.get('titolo', ''),
                    'contenuto': node['contenuto'],
                    'similarity': text_result['score']
                })

            return final_results


    def close(self):
        self.driver.close()


