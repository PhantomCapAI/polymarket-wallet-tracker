from sqlalchemy.ext.asyncio import AsyncSession
from app.models.database import AsyncSessionLocal

async def get_db() -> AsyncSession:
    """Dependency to get database session"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

def get_async_session_factory():
    """Get async session factory"""
    return AsyncSessionLocal
