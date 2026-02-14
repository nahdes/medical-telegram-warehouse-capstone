

def get_price_extractions(db: Session, limit: int = 100) -> List[dict]:
    """Return recent price / product extraction records"""
    query = text("""
        SELECT message_id, channel_name, products, prices, source_text
        FROM raw.price_extractions
        ORDER BY loaded_at DESC
        LIMIT :limit
    """)
    rows = db.execute(query, {"limit": limit})
    return [
        {
            "message_id": row[0],
            "channel_name": row[1],
            "products": row[2].split(',') if row[2] else [],
            "prices": row[3].split(',') if row[3] else [],
            "source_text": row[4]
        }
        for row in rows
    ]
