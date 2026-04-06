from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    database_url: str = "sqlite+aiosqlite:///bdc_metrics.db"
    database_url_sync: str = "sqlite:///bdc_metrics.db"
    edgar_user_agent: str = "CompanyName admin@example.com"
    schedule_hour_1: int = 6
    schedule_hour_2: int = 18
    schedule_timezone: str = "US/Eastern"
    log_level: str = "INFO"
    edgar_rate_limit: float = 8.0  # requests per second (SEC limit is 10)

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()

# BDC Fund Registry - extend this list as needed.
# User will provide full CIK codes; placeholders marked TBD.
FUNDS = [
    {"ticker": "BCRED", "name": "Blackstone Private Credit Fund", "cik": "1803498"},
    {"ticker": "OCIC", "name": "Blue Owl Credit Income Corp", "cik": "1812554"},
    {"ticker": "ADS", "name": "Apollo Debt Solutions BDC", "cik": "1837532"},
    {"ticker": "HLEND", "name": "HPS Corporate Lending Fund", "cik": "1838126"},
    {"ticker": "ASIF", "name": "Ares Strategic Income Fund", "cik": "1918712"},
]

# Date range for data collection
DATA_START_DATE = "2023-01-01"
