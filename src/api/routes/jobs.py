from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from src.api.routes.tickers import get_all_tickers, add_ticker
from src.schema_registry.response_models import TickerCategory
from src.utils.logger import logger

router = APIRouter(tags=["Jobs"])


@router.post("/run_all")
def run_all_tickers():
    completed = []
    for category, tickers in get_all_tickers().items():
        for ticker in tickers:
            if ticker not in completed:
                try:
                    _ = add_ticker(ticker, TickerCategory(category.lower()))
                    completed.append(ticker)
                except Exception as e:
                    logger.error(f"{ticker}: {e}")
    return JSONResponse({"completed": completed}, status_code=status.HTTP_201_CREATED)
