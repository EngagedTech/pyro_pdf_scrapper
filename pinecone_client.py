# pinecone_client.py

from pinecone import Pinecone
from config import PINECONE_API_KEY

def get_pinecone_client():
    return Pinecone(api_key=PINECONE_API_KEY)
