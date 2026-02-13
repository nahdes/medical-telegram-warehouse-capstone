"""
Pydantic schemas for API request/response validation
"""

from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, date


# ============================================================================
# TOP PRODUCTS
# ============================================================================

class TopProduct(BaseModel):
    """Schema for top mentioned products/terms"""
    term: str = Field(..., description="Product name or keyword")
    frequency: int = Field(..., description="Number of times mentioned", ge=0)
    avg_views: Optional[float] = Field(None, description="Average views for messages containing this term")
    channels: Optional[int] = Field(None, description="Number of channels mentioning this term")
    
    class Config:
        json_schema_extra = {
            "example": {
                "term": "paracetamol",
                "frequency": 45,
                "avg_views": 127.5,
                "channels": 3
            }
        }


# ============================================================================
# CHANNEL ACTIVITY
# ============================================================================

class ChannelActivity(BaseModel):
    """Schema for channel activity metrics"""
    channel_name: str = Field(..., description="Channel name")
    channel_type: str = Field(..., description="Channel category")
    total_posts: int = Field(..., description="Total number of posts", ge=0)
    first_post_date: Optional[date] = Field(None, description="Date of first post")
    last_post_date: Optional[date] = Field(None, description="Date of most recent post")
    days_active: Optional[int] = Field(None, description="Number of days between first and last post")
    avg_posts_per_day: Optional[float] = Field(None, description="Average posts per day")
    avg_views: Optional[float] = Field(None, description="Average views per post")
    avg_forwards: Optional[float] = Field(None, description="Average forwards per post")
    total_views: Optional[int] = Field(None, description="Total cumulative views")
    image_content_pct: Optional[float] = Field(None, description="Percentage of posts with images")
    
    class Config:
        json_schema_extra = {
            "example": {
                "channel_name": "CheMed123",
                "channel_type": "Medical",
                "total_posts": 276,
                "first_post_date": "2024-01-01",
                "last_post_date": "2024-01-15",
                "days_active": 14,
                "avg_posts_per_day": 19.7,
                "avg_views": 85.3,
                "avg_forwards": 2.1,
                "total_views": 23543,
                "image_content_pct": 52.5
            }
        }


class ChannelStats(BaseModel):
    """Basic channel statistics for listing"""
    channel_name: str
    channel_type: str
    total_posts: int
    avg_views: Optional[float]
    image_content_pct: Optional[float]


# ============================================================================
# MESSAGE SEARCH
# ============================================================================

class MessageSearchResult(BaseModel):
    """Schema for message search results"""
    message_id: int = Field(..., description="Message ID")
    channel_name: str = Field(..., description="Channel name")
    message_date: datetime = Field(..., description="Message timestamp")
    message_text: str = Field(..., description="Message content (truncated if long)")
    view_count: int = Field(..., description="Number of views", ge=0)
    forward_count: int = Field(..., description="Number of forwards", ge=0)
    has_image: bool = Field(..., description="Whether message contains an image")
    
    class Config:
        json_schema_extra = {
            "example": {
                "message_id": 12345,
                "channel_name": "CheMed123",
                "message_date": "2024-01-15T10:30:00",
                "message_text": "New arrival: Paracetamol 500mg tablets...",
                "view_count": 145,
                "forward_count": 8,
                "has_image": True
            }
        }


# ============================================================================
# VISUAL CONTENT STATS
# ============================================================================

class ImageCategoryBreakdown(BaseModel):
    """Breakdown by image category"""
    category: str = Field(..., description="Image category")
    count: int = Field(..., description="Number of images", ge=0)
    percentage: float = Field(..., description="Percentage of total", ge=0, le=100)
    avg_views: Optional[float] = Field(None, description="Average views for this category")
    avg_confidence: Optional[float] = Field(None, description="Average YOLO detection confidence")


class ChannelImageStats(BaseModel):
    """Image statistics per channel"""
    channel_name: str = Field(..., description="Channel name")
    total_images: int = Field(..., description="Total images from this channel", ge=0)
    avg_detections: Optional[float] = Field(None, description="Average objects detected per image")
    promotional_count: int = Field(0, description="Number of promotional images")
    product_display_count: int = Field(0, description="Number of product display images")


class VisualContentStats(BaseModel):
    """Comprehensive visual content statistics"""
    total_images: int = Field(..., description="Total images analyzed", ge=0)
    images_with_detections: int = Field(..., description="Images with YOLO detections", ge=0)
    avg_objects_per_image: Optional[float] = Field(None, description="Average objects detected")
    avg_detection_confidence: Optional[float] = Field(None, description="Average detection confidence score")
    
    # Category breakdown
    category_breakdown: List[ImageCategoryBreakdown] = Field(
        default_factory=list,
        description="Statistics by image category"
    )
    
    # Channel breakdown
    channel_breakdown: List[ChannelImageStats] = Field(
        default_factory=list,
        description="Statistics by channel"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "total_images": 145,
                "images_with_detections": 142,
                "avg_objects_per_image": 2.3,
                "avg_detection_confidence": 0.78,
                "category_breakdown": [
                    {
                        "category": "product_display",
                        "count": 58,
                        "percentage": 40.0,
                        "avg_views": 92.5,
                        "avg_confidence": 0.82
                    }
                ],
                "channel_breakdown": [
                    {
                        "channel_name": "CheMed123",
                        "total_images": 45,
                        "avg_detections": 2.1,
                        "promotional_count": 15,
                        "product_display_count": 20
                    }
                ]
            }
        }


# ============================================================================
# IMAGE CATEGORY PERFORMANCE
# ============================================================================

class ImageCategoryStats(BaseModel):
    """Performance statistics by image category"""
    category: str = Field(..., description="Image category")
    count: int = Field(..., description="Number of images", ge=0)
    avg_views: float = Field(..., description="Average views")
    avg_forwards: float = Field(..., description="Average forwards")
    max_views: int = Field(..., description="Maximum views", ge=0)
    total_views: int = Field(..., description="Total cumulative views", ge=0)
    
    class Config:
        json_schema_extra = {
            "example": {
                "category": "promotional",
                "count": 42,
                "avg_views": 112.5,
                "avg_forwards": 3.2,
                "max_views": 450,
                "total_views": 4725
            }
        }


# ============================================================================
# ERROR RESPONSE
# ============================================================================

class ErrorResponse(BaseModel):
    """Standard error response"""
    detail: str = Field(..., description="Error message")
    
    class Config:
        json_schema_extra = {
            "example": {
                "detail": "Channel 'InvalidChannel' not found"
            }
        }