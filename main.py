from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from contextlib import asynccontextmanager
from src.utils.logger import logger
from src.services import services
from src.api.routes import tickers, trades, tables, jobs


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    app.state.db.engine.dispose(close=True)
    app.state.broker.close_connection()
    logger.info("Closed all connections")


def create_app() -> FastAPI:
    app = FastAPI(
        title="BANKVAULT",
        version="0.0.0",
        root_path="/api/v1",
        lifespan=lifespan,
    )

    app.state.db = services.db
    app.state.broker = services.broker
    app.state.broker.connect()

    app.include_router(tickers.router, prefix="/tickers", tags=["Tickers"])
    app.include_router(trades.router, prefix="/trades", tags=["Trades"])
    app.include_router(tables.router, prefix="/tables", tags=["Tables"])
    app.include_router(jobs.router, prefix="/jobs", tags=["Jobs"])

    @app.get("/", include_in_schema=False)
    def root():
        return RedirectResponse("/api/v1/docs")

    return app


app = create_app()
