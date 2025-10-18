from fastapi import APIRouter, status, Depends
from fastapi.responses import JSONResponse
from src.technical_analytics.api.routes.tickers import get_all_tickers, add_ticker
from src.technical_analytics.schema_registry.response_models import TickerCategory
from src.technical_analytics.services.database import PostgresDB
from src.technical_analytics.services.broker import AlpacaBroker
from src.technical_analytics.api.dependencies import get_db, get_broker
from src.technical_analytics.utils.logger import logger

router = APIRouter(tags=["Jobs"])


@router.post("/run_all")
def run_all_tickers(db: PostgresDB = Depends(get_db), broker: AlpacaBroker = Depends(get_broker)):
    completed = []

    for category, tickers in get_all_tickers(db=db).items():
        for ticker in tickers:
            if ticker not in completed:
                try:
                    _ = add_ticker(ticker, TickerCategory(category.lower()), db=db, broker=broker)
                    completed.append(ticker)
                except Exception as e:
                    logger.error(f"{ticker}: {e}")
    return JSONResponse({"completed": completed}, status_code=status.HTTP_201_CREATED)
