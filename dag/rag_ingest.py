"""
## RAG ingestion pipeline
Builds the vector store for RAG by:
1. **Fetch** – Scrapes content from configured web URLs
2. **Chunk** – Splits documents into overlapping text chunks
3. **Embed** – Generates vector embeddings via OpenAI
4. **Store** – Upserts embeddings into Pinecone
Publishes the `rag_index` asset on completion so downstream DAGs (like `rag_query`) know fresh data is available.
### Required Airflow variables
`OPENAI_API_KEY`
`PINECONE_API_KEY`
`PINECONE_INDEX_NAME`
`RAG_SOURCE_URLS` - JSON list of URLs to ingest