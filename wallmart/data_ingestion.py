from langchain_astradb import AstraDBVectorStore
from langchain_huggingface import HuggingFaceEmbeddings
from wallmart.data_converter import DataConverter
from wallmart.config import Config

class DataIngestor:
    def __init__(self):
        self.embeddings = HuggingFaceEmbeddings(
            model_name=Config.EMBEDDING_MODEL
        )
        self.vector_store = AstraDBVectorStore(
            embedding=self.embeddings,
            collection_name="wallmart_db_v2",
            api_endpoint=Config.ASTRA_DB_API_ENDPOINT,
            token=Config.ASTRA_DB_APPLICATION_TOKEN,
            namespace=Config.ASTRA_DB_KEYSPACE
        )

    def ingest_data(self,load_existing=True):
        if load_existing:
            return self.vector_store
        
        docs = DataConverter("data/flipkart_product_review.csv").convert()
        self.vector_store.add_documents(docs)

        return self.vector_store

