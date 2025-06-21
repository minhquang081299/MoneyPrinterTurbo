from .base_storage import BaseStorage
from elasticsearch import (
    AsyncElasticsearch,
    Elasticsearch,
)
from typing import List, Dict, Any, Optional, Union
import asyncio
from datetime import datetime
from loguru import logger as app_logger

aclients: Optional[Dict[str, AsyncElasticsearch]] = None     
clients: Optional[Dict[str, Elasticsearch]] = None

channels = settings.channels
if clients is None:
    clients = {}
if aclients is None:
    aclients = {}

for channel in channels:
    app_logger.info(f"Debug - Initializing connection for channel: {channel.name}")
    es_url = f"{channel.elastic_search.host}:{channel.elastic_search.port}"
    app_logger.info(f"Debug - Elasticsearch URL: {es_url}")
    basic_auth = None
    if channel.elastic_search.password:
        basic_auth = (
            channel.elastic_search.user,
            channel.elastic_search.password,
        )
        app_logger.info("Debug - Using basic auth")
    try:
        aclients[channel.name] = AsyncElasticsearch(
            es_url, basic_auth=basic_auth, verify_certs=False
        )
        clients[channel.name] = Elasticsearch(
            es_url, basic_auth=basic_auth, verify_certs=False
        )
        app_logger.info(f"Debug - Successfully initialized connections for {channel.name}")
    except Exception as e:
        app_logger.error(f"Debug - Error initializing connection for {channel.name}: {str(e)}")
        raise

app_logger.info(f"Debug - Async clients: {aclients}")
# app_logger.info(f"Debug - Sync clients: {clients}")

channels_embed: Dict[str,ChannelConfig] = None
if channels_embed is None:
    channels_embed = {}

for channel in channels:

    channels_embed[channel.name] = ChannelConfig(embedding=channel.embedding)

# Configuration

class ElasticsearchStorage(BaseStorage):
    BATCH_SIZE = 2000  # Increased for better bulk performance
    MAX_RETRIES = 5
    MIN_SCORE = 0.7
    TIMEOUT = 60  # seconds
    CACHE_TTL = 3600  # 1 hour cache
    


    def __init__(
        self, channel_id:str
    ):
        """Enhanced initialization"""
        # Use provided connection params directly
        if channel_id is not None:
            self.client = clients[channel_id]
            self.aclient = aclients[channel_id]

        self._initialized_indices = set()
        self._bulk_queue = []
        self._queue_lock = asyncio.Lock()

    
    async def asearch_vectors_hybrid_knn(
        self,
        index_name: str,
        embedding: Optional[List[float]] = None,
        keywords: Union[Optional[str], Optional[List[str]]] = None,
        title: Optional[str] = None,
        score_threshold=0.7,
        vector_boost: float = 1.0,
        text_boost: float = 0.2,
        pageIndex: int = 1,  # Changed from 0 to 1
        pageSize: int = 10,
        limit: Optional[int] = None,
        valid_data: Optional[bool] = None,
        media_type: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Search for similar vectors in the specified index with pagination support"""
        # Set size to use pageSize instead of top_k
        size = pageSize
        # Calculate from parameter based on pageIndex and pageSize, adjusting for 1-indexed pagination
        from_param = (
            pageIndex - 1
        ) * pageSize  # Adjust calculation to handle 1-indexed pageIndex

        if limit is not None:
            size = limit
        # Xây dựng knn động
        results = []
        should_clauses = []
        if keywords:
            # Add text search on multiple fields
            should_clauses.extend([
                {
                    "match": {
                        "description": {
                            "query": title,
                            "boost": 0.5*text_boost
                        }
                    }
                },
                {
                    "terms": {
                        "keywords": keywords, 
                        "boost": text_boost
                    }
                }
            ])

        if embedding:
            should_clauses.append(
                {
                    "knn": {
                        "field": "embedding",
                        "query_vector": embedding,
                        "k": size,
                        "num_candidates": max(size * 5, 100),
                        "boost": vector_boost
                    }
                }
            )
        # Build filter clauses - only add valid_data filter if valid_data parameter is True
        filter_clauses = []
        if media_type is not None:  # Kiểm tra nếu có giá trị lọc media_type được cung cấp
            filter_clauses.append(
                {
                    "term": {
                        "media_type": media_type
                    }
                }
            )
        if valid_data is True:
            filter_clauses.append(
                {
                    "term": {
                        "valid_data": True
                    }
                }
            )

        # Xây dựng script hoàn chỉnh
        search_query = {
            "_source": {"excludes": ["*.embedding", "embedding"]},
            "query": {"bool": {"should": should_clauses, "minimum_should_match": 1 if should_clauses else 0}},
            "size": size,
            "from": from_param,
            "sort": [{"_score": {"order": "desc"}}],
        }
        # Add filter clause to the query if it's not empty
        if filter_clauses:
             search_query["query"]["bool"]["filter"] = filter_clauses

        if score_threshold is not None:
            search_query["min_score"] = score_threshold

        try:
            response = await self.aclient.search(
                index=index_name, body=search_query, request_timeout=60
            )
            # Add total hits information to help with pagination on the client side
            total_hits = response["hits"]["total"]["value"]

            results = [
                {
                    "id": hit["_id"],
                    "source": {
                        "media_id": hit["_source"]["media_id"],
                        "media_path": hit["_source"]["media_path"],
                        "media_type": hit["_source"]["media_type"],
                        "valid_data": hit["_source"]["valid_data"],
                        "avatar": hit["_source"].get("avatar"),
                        "description": hit["_source"]["description"],
                        "tags": hit["_source"]["tags"],
                        "updated_at": hit["_source"].get("updated_at")
                        or hit["_source"].get("timestamp"),
                    },
                    "score": hit["_score"],
                }
                for hit in response["hits"]["hits"]
            ]

            # Return both the results and pagination metadata
            return {
                "items": results,
                "pagination": {
                    "pageIndex": pageIndex,
                    "pageSize": size,
                    "totalItems": total_hits,
                    "totalPages": max(
                        1, (total_hits + pageSize - 1) // pageSize
                    ),  # Ensure minimum of 1 page
                },
            }
        except Exception as e:
            app_logger.error(f"Failed to search vectors in index {index_name}: {e}")
            return {"items": results}
