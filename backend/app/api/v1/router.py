from fastapi import APIRouter

from app.api.v1.endpoints import ai, bets, chatgpt, collections, data_quality, health, jobs, notifications, predictions, races

api_router = APIRouter()
api_router.include_router(health.router, tags=["health"])
api_router.include_router(races.router, prefix="/api/v1", tags=["races"])
api_router.include_router(predictions.router, prefix="/api/v1", tags=["predictions"])
api_router.include_router(ai.router, prefix="/api/v1", tags=["ai"])
api_router.include_router(chatgpt.router, prefix="/api/v1", tags=["chatgpt-manual"])
api_router.include_router(bets.router, prefix="/api/v1", tags=["bets"])
api_router.include_router(jobs.router, prefix="/api/v1", tags=["jobs"])
api_router.include_router(collections.router, prefix="/api/v1", tags=["collections"])
api_router.include_router(data_quality.router, prefix="/api/v1", tags=["data-quality"])
api_router.include_router(notifications.router, prefix="/api/v1", tags=["notifications"])
