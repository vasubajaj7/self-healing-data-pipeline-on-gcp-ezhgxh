from src.backend.utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py
from .admin_routes import router as admin_router  # src/backend/api/routes/admin_routes.py
from .quality_routes import router as quality_router  # src/backend/api/routes/quality_routes.py
from .healing_routes import router as healing_router  # src/backend/api/routes/healing_routes.py
from .monitoring_routes import router as monitoring_router  # src/backend/api/routes/monitoring_routes.py
from .ingestion_routes import router as ingestion_router  # src/backend/api/routes/ingestion_routes.py
from .optimization_routes import router as optimization_router  # src/backend/api/routes/optimization_routes.py

# Initialize logger
logger = get_logger(__name__)

# List of router instances for the API
routers = [
    admin_router,
    quality_router,
    healing_router,
    monitoring_router,
    ingestion_router,
    optimization_router
]

logger.info(f"Initialized API routes: {', '.join(router.prefix for router in routers)}")