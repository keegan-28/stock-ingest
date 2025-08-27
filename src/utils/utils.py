import yaml
from src.common.logger import logger


def load_config(path: str) -> dict[str, str]:
    try:
        with open(path, "r") as file:
            config = yaml.safe_load(file)
        assert "tickers" in config and isinstance(config["tickers"], list)
        return config
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        raise
