import os
from pathlib import Path
from dotenv import load_dotenv

# Определяем базовую директорию проекта
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Настройки логирования
LOG_ENABLED = True
LOG_LEVEL = os.getenv("SCRAPY_LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
LOG_DATEFORMAT = "%Y-%m-%d %H:%M:%S"

# Создание директории для логов
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Файл для логов Scrapy
LOG_FILE = LOG_DIR / "scrapy.log"

# Загружаем .env
env_path = BASE_DIR / ".env"
load_dotenv(dotenv_path=env_path)

# Создаем папку data если её нет
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

JSON_FILE = DATA_DIR / "books_full.json"

MIN_QUALITY_VALUE = 85

FEED_SETTINGS = {
    # Настройки вывода
    "FEED_FORMAT": "json",
    "FEED_URI": JSON_FILE,
    # Настройки многопоточности (20 потоков)
    "DOWNLOAD_DELAY": 0,  # Базовая задержка 0, будем использовать случайные
    "CONCURRENT_REQUESTS": 20,  # 20 одновременных запросов
    "CONCURRENT_REQUESTS_PER_DOMAIN": 20,  # 20 запросов к одному домену
    # Случайные задержки
    "RANDOMIZE_DOWNLOAD_DELAY": True,
    "DOWNLOAD_DELAY_RANGE": (0.5, 2.0),  # От 0.5 до 2.0 секунд
    # Автотроттлинг
    "AUTOTHROTTLE_ENABLED": True,
    "AUTOTHROTTLE_START_DELAY": 0.5,
    "AUTOTHROTTLE_MAX_DELAY": 3.0,
    "AUTOTHROTTLE_TARGET_CONCURRENCY": 20.0,
    # Retry настройки
    "RETRY_ENABLED": True,
    "RETRY_TIMES": 3,
    "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 429, 403],
    # Логирование
    "LOG_LEVEL": "INFO",
    # Настройки кэширования
    "HTTPCACHE_ENABLED": False,  # Можно включить для отладки
    "HTTPCACHE_EXPIRATION_SECS": 3600,
    "HTTPCACHE_DIR": "httpcache",
    # User-Agent
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36",
    # Дополнительные настройки
    "COOKIES_ENABLED": False,
    "TELNETCONSOLE_ENABLED": False,
    "ROBOTSTXT_OBEY": True,
}

BOT_NAME = "books_scraper"

SPIDER_MODULES = ["books_scraper.spiders"]
NEWSPIDER_MODULE = "books_scraper.spiders"

# PostgreSQL настройки из .env
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "books_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

# Формируем полный URI для подключения
POSTGRES_DB_URI = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Настройки пайплайнов
ITEM_PIPELINES = {
    "books_scraper.pipelines.DataValidationPipeline": 100,
    "books_scraper.pipelines.BooksPostgresPipeline": 300,
}

# Настройки для обхода блокировок
ROBOTSTXT_OBEY = True

# Retry настройки
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429, 403]


# User-Agent middleware
DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    "scrapy_user_agents.middlewares.RandomUserAgentMiddleware": 400,
    "books_scraper.middlewares.RandomDelayMiddleware": 543,
    "books_scraper.middlewares.SeleniumLoggingMiddleware": 544,
    "scrapy_selenium.SeleniumMiddleware": 800,
}

# Настройки кэширования
HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 3600
HTTPCACHE_DIR = "httpcache"

# Selenium настройки
SELENIUM_DRIVER_NAME = "chrome"
SELENIUM_DRIVER_EXECUTABLE_PATH = None

# Определяем аргументы Selenium
selenium_arguments = [
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--window-size=1920,1080",
    "--disable-blink-features=AutomationControlled",
]

# Добавляем headless режим если указано в .env
if os.getenv("SELENIUM_HEADLESS", "true").lower() == "true":
    selenium_arguments.append("--headless")

SELENIUM_DRIVER_ARGUMENTS = selenium_arguments
