"""
FastAPI Analytics API for Medical Telegram Warehouse
Exposes data warehouse insights through REST endpoints
"""

import os

from fastapi import FastAPI, HTTPException, Query, Path, Depends, Request, Security, Response
from fastapi_cache.decorator import cache
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyHeader
from typing import List, Optional
import uvicorn

# caching and rate limiting
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address


# simple API key auth
API_KEY = os.getenv('API_KEY', 'changeme')
api_key_header = APIKeyHeader(name='X-API-Key', auto_error=False)

def verify_api_key(api_key: str = Security(api_key_header)):
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail='Invalid or missing API key')
    return True

from api.database import get_db_session
from api.schemas import (
    TopProduct,
    ChannelActivity,
    MessageSearchResult,
    VisualContentStats,
    ChannelStats,
    ImageCategoryStats
)
from api.queries import (
    get_top_products,
    get_channel_activity,
    search_messages,
    get_visual_content_stats,
    get_all_channels,
    get_image_category_performance
)

# Initialize FastAPI app
app = FastAPI(
    title="Medical Telegram Analytics API",
    description="""
    REST API for analyzing Ethiopian medical business data from Telegram channels.
    
    ## Features
    - 📊 Top products and trends analysis
    - 📢 Channel activity monitoring
    - 🔍 Message search with keyword filtering
    - 🖼️ Visual content analytics
    - 📈 Engagement metrics
    
    ## Data Sources
    - Telegram messages from medical/pharmaceutical channels
    - YOLO object detection results
    - Dimensional data warehouse (star schema)
    """,
    version="1.0.0",
    contact={
        "name": "Kara Solutions",
        "url": "https://karasolutions.et",
    }
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# initialize rate limiter
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(429, _rate_limit_exceeded_handler)


@app.on_event("startup")
def startup():
    # set up simple in-memory cache (for prod, swap to redis)
    FastAPICache.init(InMemoryBackend(), prefix="fastapi-cache")


# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.get("/", tags=["Health"])
async def root():
    """Root endpoint - API health check"""
    return {
        "status": "healthy",
        "api": "Medical Telegram Analytics API",
        "version": "1.0.0",
        "docs": "/docs"
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """Detailed health check with database connection test"""
    try:
        db = next(get_db_session())
        # Test query
        result = db.execute("SELECT COUNT(*) FROM raw.telegram_messages").fetchone()
        message_count = result[0]
        db.close()
        
        return {
            "status": "healthy",
            "database": "connected",
            "total_messages": message_count
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database connection failed: {str(e)}")


# ============================================================================
# ENDPOINT 1: TOP PRODUCTS
# ============================================================================

@app.get(
    "/api/reports/top-products",
    response_model=List[TopProduct],
    tags=["Reports"],
    summary="Get most frequently mentioned products/terms",
    description="""
    Returns the most frequently mentioned terms or products across all channels.
    
    Uses text analysis to extract common keywords from message content.
    Useful for identifying trending products and popular medications.
    """
)
@limiter.limit("10/minute")
@cache(expire=60)
async def top_products(
    limit: int = Query(
        default=10,
        ge=1,
        le=100,
        description="Number of top products to return"
    ),
    authorized: bool = Depends(verify_api_key)
):
        ge=1,
        le=100,
        description="Number of top products to return"
    )
):
    """Get top mentioned products/terms"""
    try:
        db = next(get_db_session())
        results = get_top_products(db, limit)
        db.close()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINT 2: CHANNEL ACTIVITY
# ============================================================================

@app.get(
    "/api/channels/{channel_name}/activity",
    response_model=ChannelActivity,
    tags=["Channels"],
    summary="Get channel activity and trends",
    description="""
    Returns detailed posting activity and engagement trends for a specific channel.
    
    Includes:
    - Total posts and timeframe
    - Average engagement metrics
    - Visual content statistics
    - Posting patterns
    """
)
@limiter.limit("20/minute")
@cache(expire=60)
async def channel_activity(
    channel_name: str = Path(
        ...,
        description="Name of the Telegram channel (e.g., 'CheMed123')"
    ),
    authorized: bool = Depends(verify_api_key)
):
        description="Name of the Telegram channel (e.g., 'CheMed123')"
    )
):
    """Get activity metrics for a specific channel"""
    try:
        db = next(get_db_session())
        result = get_channel_activity(db, channel_name)
        db.close()
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Channel '{channel_name}' not found"
            )
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/api/channels",
    response_model=List[ChannelStats],
    tags=["Channels"],
    summary="List all channels",
    description="Returns a list of all channels with basic statistics"
)
@limiter.limit("20/minute")
@cache(expire=120)
async def list_channels(
    authorized: bool = Depends(verify_api_key)
):
    """Get list of all channels with basic stats"""
    try:
        db = next(get_db_session())
        results = get_all_channels(db)
        db.close()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINT 3: MESSAGE SEARCH
# ============================================================================

@app.get(
    "/api/search/messages",
    response_model=List[MessageSearchResult],
    tags=["Search"],
    summary="Search messages by keyword",
    description="""
    Search for messages containing specific keywords.
    
    Supports:
    - Case-insensitive search
    - Partial word matching
    - Filtering by channel
    - Limit results
    """
)
@limiter.limit("20/minute")
@cache(expire=30)
async def search_messages_endpoint(
    query: str = Query(
        ...,
        min_length=2,
        description="Search keyword or phrase"
    ),
    channel: Optional[str] = Query(
        None,
        description="Filter by channel name (optional)"
    ),
    limit: int = Query(
        default=20,
        ge=1,
        le=100,
        description="Maximum number of results to return"
    ),
    authorized: bool = Depends(verify_api_key)
):
        min_length=2,
        description="Search keyword or phrase"
    ),
    channel: Optional[str] = Query(
        None,
        description="Filter by channel name (optional)"
    ),
    limit: int = Query(
        default=20,
        ge=1,
        le=100,
        description="Maximum number of results to return"
    )
):
    """Search messages by keyword"""
    try:
        db = next(get_db_session())
        results = search_messages(db, query, channel, limit)
        db.close()
        
        if not results:
            return []
        
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINT 4: VISUAL CONTENT STATS
# ============================================================================

@app.get(
    "/api/reports/visual-content",
    response_model=VisualContentStats,
    tags=["Reports"],
    summary="Get visual content statistics",
    description="""
    Returns comprehensive statistics about image usage across all channels.
    
    Includes:
    - Total images and distribution
    - Image categories (promotional, product_display, etc.)
    - Engagement by image type
    - Channel comparisons
    - YOLO detection insights
    """
)
@limiter.limit("10/minute")
@cache(expire=120)
async def visual_content_stats(
    authorized: bool = Depends(verify_api_key)
):
    """Get statistics about image usage and visual content"""
    try:
        db = next(get_db_session())
        result = get_visual_content_stats(db)
        db.close()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# BONUS ENDPOINT: IMAGE CATEGORY PERFORMANCE
# ============================================================================

@app.get(
    "/api/reports/image-performance",
    response_model=List[ImageCategoryStats],
    tags=["Reports"],
    summary="Compare image category performance",
    description="""
    Analyzes engagement metrics by image category.
    
    Answers: Do promotional images (with people) perform better than product-only images?
    """
)
@limiter.limit("10/minute")
@cache(expire=120)
async def image_category_performance(
    authorized: bool = Depends(verify_api_key)
):
    """Compare performance metrics across image categories"""
    try:
        db = next(get_db_session())
        results = get_image_category_performance(db)
        db.close()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# DASHBOARD
# ============================================================================

@app.get("/dashboard", tags=["Dashboard"], summary="Simple HTML dashboard", dependencies=[Depends(verify_api_key)])
async def dashboard():
    # placeholder dashboard; could be replaced with a more sophisticated app
    html = """
    <html>
      <head><title>Medical Telegram Analytics Dashboard</title></head>
      <body>
        <h1>Dashboard</h1>
        <p>Use the API endpoints to fetch data programmatically.</p>
        <p>This page could embed charts using Plotly or redirect to a Streamlit app.</p>
      </body>
    </html>
    """
    return Response(content=html, media_type="text/html")

# ============================================================================
# RUN SERVER
# ============================================================================

if __name__ == "__main__":
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )