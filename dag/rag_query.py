"""
## RAG query pipeline
Queries an existing Pinecone vector store and generates an answer using OpenAI for example.
1. **Embed** the query via OpenAI
2. **Retrieve** the top-k most relevant chunks from Pinecone
3. **Generate** an answer grounded in the retrieved context
Trigger this DAG manually and supply your question via the `query` parameter in the trigger form.
### Required Airflow variables
`OPENAI_API_KEY`
`PINECONE_API_KEY`
`PINECONE_INDEX_NAME`
