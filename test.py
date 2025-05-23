# from pinecone.grpc import PineconeGRPC as Pinecone

# pc = Pinecone(api_key="pcsk_7WDufM_NxEbMqyabfd5bZyqG2zGDmRh9B924QFNQ5NBE6ei1X3WDhaQqY6yMNv2Putuf24")
# index = pc.Index(host="pyro-1-iuirg9d.svc.aped-4627-b74a.pinecone.io")

# Delete records from index.
# index.delete(ids=["003d75eb0cd06d0362ca6d3845ffbbc5", "0312937cdd88c4fd5b63ba2704422c69"], namespace='Accounts_Bulk_Data-2025-05-17')

#Delete all records from index.
# index.delete(delete_all=True, namespace='Accounts_Bulk_Data-2025-05-17')


# TERMINAL
# PINECONE_API_KEY="pcsk_7WDufM_NxEbMqyabfd5bZyqG2zGDmRh9B924QFNQ5NBE6ei1X3WDhaQqY6yMNv2Putuf24"
# INDEX_HOST="pyro-1-iuirg9d.svc.aped-4627-b74a.pinecone.io"
# NAMESPACE="Accounts_Bulk_Data-2025-05-17"

# curl -X DELETE "https://$INDEX_HOST/namespaces/$NAMESPACE" \
#     -H "Api-Key: $PINECONE_API_KEY" \
#     -H "X-Pinecone-API-Version: 2025-04"