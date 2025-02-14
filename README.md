# DMGDB Scalable Graph RAG on a large corpus of legal documents

## Overview
This project implements an advanced Graph-based Retrieval-Augmented Generation (RAG) system specifically designed for the Italian legal domain. The system efficiently handles legal documents of various formats, implementing accurate graph RAG techniques and creating a dynamic, scalable knowledge base for legal information retrieval and question answering.

## Key Features
- **Document Processing Pipeline**
  - Conversion of various legal document formats to structured JSON
  - Entity extraction using SpaCy NLP with custom abbreviations
  - Custom embedding generation using DeepMount00

- **Dual Graph Structure**
  - **Use Case Documents Graph**
    - Hierarchical representation of documents, chunks, and entities
    - Custom embeddings using gliner
    - Full-text search capabilities
    
  - **Legal Documents Graph**
    - Hierarchical structure for laws 
    - Two types of relationships:
      - Extensional: Citation-based relationships
      - Intensional: Semantic relationships between articles

## System Components

### Document Processing
- JSON restructuring for standardization
- Text chunking and embedding generation
- Entity extraction using custom NLP pipeline

### Relationship Detection
1. **Intensional Relationships**
   - Extracted from structured knowledge files (compendi, dottrine)
   - Enhanced with link prediction algorithms

2. **Extensional Relationships**
   - Citation chain detection using LegalBERT-based classification
   - Article reference extraction using custom NER model

### RAG Implementation

#### Use Case Documents RAG
Agent-based implementation with the following capabilities:
- Embedding-based search (wide or document-specific)
- Text-based search with full-text indexing
- Category-based entity retrieval
- Contextual navigation (Previous, Next, Related)

#### Law Corpus RAG
Specialized agent with:
- Direct article search
- Wide search using BFS on hierarchical structure
- Related articles retrieval
- Rubrica field embedding for improved context

## Performance

The system has been evaluated on multiple Q&A datasets:
1. Data extraction from use case documents
2. Q&A over use case documents
3. Open legal domain Q&A

### Key Results
- Improved accuracy compared to traditional RAG approaches
- Enhanced query coverage
- Reduced hallucination in responses
- Better handling of complex legal queries
- Efficient processing of large document corpora


## Citations
- Guo, Z., et al. (2024). LightRAG: Simple and Fast Retrieval-Augmented Generation
- Edge, D., et al. (2024). From Local to Global: A Graph RAG Approach to Query-Focused Summarization
- Peng, B., et al. (2024). Graph Retrieval-Augmented Generation: A Survey

## Author
Stefano Patalano (2024)
