"""
Модуль для продвинутого логирования парсинга в файл с поддержкой пайплайнов
"""

import logging
import os
from pathlib import Path
from datetime import datetime
import sys
from typing import Dict, Any


class PipelineLogger:
    """Логгер для пайплайнов"""

    @staticmethod
    def log_pipeline_start(pipeline_name: str, spider_name: str):
        """Логирование начала работы пайплайна"""
        logger = logging.getLogger(f"{spider_name}.pipelines")
        logger.info(f"🚀 Запуск пайплайна: {pipeline_name}")

    @staticmethod
    def log_database_init(spider_name: str, table_info: str):
        """Логирование инициализации базы данных"""
        logger = logging.getLogger(f"{spider_name}.pipelines")
        logger.info(f"📊 Инициализация БД: {table_info}")

    @staticmethod
    def log_table_clearing(spider_name: str, table_name: str, record_count: int):
        """Логирование очистки таблицы"""
        logger = logging.getLogger(f"{spider_name}.pipelines")
        if record_count > 0:
            logger.info(
                f"🧹 Очистка таблицы '{table_name}': удалено {record_count} записей"
            )
        else:
            logger.info(f"✅ Таблица '{table_name}' уже пуста")

    @staticmethod
    def log_item_save(spider_name: str, url: str, is_update: bool, item_counter: int):
        """Логирование сохранения/обновления item"""
        logger = logging.getLogger(f"{spider_name}.pipelines")
        action = "Обновлена" if is_update else "Сохранена"

        if item_counter % 20 == 0:  # Логируем каждые 20 записей
            logger.info(f"💾 {action} книга #{item_counter}: {url[:50]}...")
        else:
            logger.debug(f"💾 {action} книга: {url}")

    @staticmethod
    def log_validation_stats(spider_name: str, stats: Dict[str, Any]):
        """Логирование статистики валидации"""
        logger = logging.getLogger(f"{spider_name}.pipelines")
        logger.info(f"📋 Статистика валидации: {stats}")

    @staticmethod
    def log_validation_error(spider_name: str, field: str, value: Any, error: str):
        """Логирование ошибок валидации"""
        logger = logging.getLogger(f"{spider_name}.pipelines")
        logger.warning(
            f"⚠️  Ошибка валидации поля '{field}': {error} (значение: {value})"
        )

    @staticmethod
    def log_database_error(spider_name: str, error: str, item_info: str = ""):
        """Логирование ошибок базы данных"""
        logger = logging.getLogger(f"{spider_name}.pipelines")
        logger.error(f"❌ Ошибка БД: {error}")
        if item_info:
            logger.error(f"   Проблемный item: {item_info}")

    @staticmethod
    def log_pipeline_completion(
        spider_name: str, pipeline_name: str, items_processed: int
    ):
        """Логирование завершения работы пайплайна"""
        logger = logging.getLogger(f"{spider_name}.pipelines")
        logger.info(
            f"✅ Пайплайн '{pipeline_name}' завершен. "
            f"Обработано items: {items_processed}"
        )


class SpiderLogger:
    """Кастомный логгер для парсинга с записью в файл"""

    def __init__(self, spider_name, log_dir="logs"):
        self.spider_name = spider_name
        self.log_dir = Path(log_dir)
        self.setup_logging()

    def setup_logging(self):
        """Настройка логирования с записью в файл и выводом в консоль"""
        # Создаем директорию для логов если не существует
        self.log_dir.mkdir(exist_ok=True)

        # Формируем имя файла с датой
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.log_dir / f"{self.spider_name}_{timestamp}.log"

        # Создаем логгер для паука
        self.logger = logging.getLogger(self.spider_name)
        self.logger.setLevel(logging.DEBUG)

        # Создаем логгер для пайплайнов
        self.pipeline_logger = logging.getLogger(f"{self.spider_name}.pipelines")
        self.pipeline_logger.setLevel(logging.DEBUG)

        # Очищаем существующие обработчики
        for logger in [self.logger, self.pipeline_logger]:
            logger.handlers.clear()

        # Форматтер для логов
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # 1. Обработчик для записи в файл (все сообщения)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        # 2. Обработчик для вывода в консоль
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)

        # Фильтр для разделения логов в консоли
        class ConsoleFilter(logging.Filter):
            def filter(self, record):
                # В консоль выводим только INFO+ для паука и WARNING
                if ".pipelines" in record.name:
                    return record.levelno >= logging.WARNING
                return record.levelno >= logging.INFO

        console_handler.addFilter(ConsoleFilter())
        console_handler.setFormatter(formatter)

        # Добавляем обработчики
        for logger in [self.logger, self.pipeline_logger]:
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)

        self.log_file_path = log_file
        self.logger.info(
            f"Логирование инициализировано. Логи сохраняются в: {log_file}"
        )

    def log_start(self, settings_info):
        """Логирование начала работы паука"""
        self.logger.info("=" * 70)
        self.logger.info(f"НАЧАЛО ПАРСИНГА: {self.spider_name}")
        self.logger.info("=" * 70)
        self.logger.info(f"Настройки: {settings_info}")
        self.logger.info("-" * 70)

    def log_progress(self, request_count, item_count, elapsed_time):
        """Логирование прогресса"""
        if request_count % 5 == 0:
            speed = item_count / elapsed_time if elapsed_time > 0 else 0
            self.logger.info(
                f"[Прогресс] Страниц: {request_count}, "
                f"Книг: {item_count}, "
                f"Время: {elapsed_time:.1f} сек, "
                f"Скорость: {speed:.2f} книг/сек"
            )

    def log_book_detail(self, book_number, url, driver_available):
        """Логирование обработки детальной страницы книги"""
        if book_number % 10 == 0:
            driver_info = " (Selenium)" if driver_available else ""
            self.logger.info(f"[Книга #{book_number}{driver_info}] Обработка: {url}")

    def log_selenium_demo(self, book_number, success, details):
        """Логирование демонстрации Selenium"""
        status = "✓" if success else "✗"
        self.logger.info(f"[Selenium Demo #{book_number}] {status} {details}")

    def log_book_data(self, book_number, item):
        """Логирование собранных данных книги"""
        if book_number % 20 == 0:
            self.logger.info("-" * 50)
            self.logger.info(f"📖 КНИГА #{book_number}")
            self.logger.info(f"  Заголовок: {item.get('title', 'N/A')}")
            self.logger.info(f"  Цена: {item.get('price', 'N/A')}")
            self.logger.info(f"  В наличии: {item.get('in_stock', False)}")
            self.logger.info(f"  Количество: {item.get('available_quantity', 0)}")
            self.logger.info(f"  Рейтинг: {item.get('rating', 'N/A')}")
            self.logger.info(f"  Категория: {item.get('category', 'N/A')}")
            self.logger.info(
                f"  Качество данных: {item.get('data_quality_score', 'N/A')}%"
            )
            self.logger.info(f"  URL: {item.get('url', 'N/A')}")
            self.logger.info("-" * 50)

    def log_error(self, error_type, message, url=None):
        """Логирование ошибок"""
        if url:
            self.logger.error(f"[{error_type}] {message} | URL: {url}")
        else:
            self.logger.error(f"[{error_type}] {message}")

    def log_warning(self, warning_type, message):
        """Логирование предупреждений"""
        self.logger.warning(f"[{warning_type}] {message}")

    def log_completion(self, reason, request_count, item_count, elapsed_time):
        """Логирование завершения работы"""
        self.logger.info("=" * 70)
        self.logger.info(f"ЗАВЕРШЕНИЕ ПАРСИНГА: {self.spider_name}")
        self.logger.info("=" * 70)
        self.logger.info(f"Причина: {reason}")
        self.logger.info(f"Обработано страниц: {request_count}")
        self.logger.info(f"Собрано книг: {item_count}")
        self.logger.info(f"Общее время: {elapsed_time:.1f} сек")

        if elapsed_time > 0:
            speed = item_count / elapsed_time
            self.logger.info(f"Средняя скорость: {speed:.2f} книг/сек")

        # Статистика по файлу логов
        if hasattr(self, "log_file_path"):
            try:
                file_size = os.path.getsize(self.log_file_path) / 1024  # в КБ
                self.logger.info(f"Размер файла логов: {file_size:.2f} КБ")
            except Exception:
                pass

        self.logger.info("=" * 70)

    def get_logger(self):
        """Возвращает объект логгера паука"""
        return self.logger

    def get_pipeline_logger(self):
        """Возвращает объект логгера для пайплайнов"""
        return self.pipeline_logger


class MiddlewareLogger:
    """Логгер для middleware"""

    @staticmethod
    def log_middleware_init(spider_name: str, middleware_name: str, config: dict):
        """Логирование инициализации middleware"""
        logger = logging.getLogger(f"{spider_name}.middlewares")
        logger.info(f"⚙️  Инициализация {middleware_name}: {config}")

    @staticmethod
    def log_delay_statistics(spider_name: str, stats: dict):
        """Логирование статистики задержек"""
        logger = logging.getLogger(f"{spider_name}.middlewares")
        logger.info(f"⏱️  Статистика задержек: {stats}")

    @staticmethod
    def log_selenium_usage(spider_name: str, selenium_count: int, regular_count: int):
        """Логирование использования Selenium"""
        logger = logging.getLogger(f"{spider_name}.middlewares.selenium")
        total = selenium_count + regular_count
        if total > 0:
            percent = (selenium_count / total) * 100
            logger.info(
                f"🔧 Использование Selenium: {selenium_count}/{total} ({percent:.1f}%)"
            )


def setup_spider_logging(spider_name):
    """Фабрика для создания логгера"""
    return SpiderLogger(spider_name)
