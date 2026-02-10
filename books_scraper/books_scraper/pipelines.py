import logging
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    DateTime,
    Text,
    Table,
    MetaData,
    Boolean,
    delete,
    select,
    func,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import re
from scrapy.exceptions import DropItem
from books_scraper.spiders.spider_logger import PipelineLogger


class BooksPostgresPipeline:
    """Пайплайн для сохранения данных книг в PostgreSQL."""

    def __init__(self, db_uri):
        """
        Инициализация пайплайна для работы с PostgreSQL.

        Args:
            db_uri (str): URI строки подключения к базе данных PostgreSQL
        """
        self.db_uri = db_uri
        self.engine = None
        self.Session = None
        self.metadata = None
        self.logger = logging.getLogger(__name__)
        self.should_clear_table = True
        self.items_processed = 0
        self.items_inserted = 0
        self.items_updated = 0

    @classmethod
    def from_crawler(cls, crawler):
        """
        Фабричный метод для создания экземпляра пайплайна из настроек Crawler.

        Получает конфигурационные параметры из settings.py Scrapy и передает
        их в конструктор пайплайна.

        Args:
            crawler: Объект Scrapy Crawler

        Returns:
            BooksPostgresPipeline: Экземпляр пайплайна
        """
        return cls(db_uri=crawler.settings.get("POSTGRES_DB_URI"))

    def open_spider(self, spider):
        """
        Вызывается при открытии паука (начало парсинга).

        Выполняет подготовительные операции:
        1. Создает подключение к PostgreSQL
        2. Определяет схему таблицы raw_books
        3. Создает таблицу если она не существует
        4. Настраивает очистку таблицы перед началом

        Args:
            spider: Объект Scrapy Spider
        """
        PipelineLogger.log_pipeline_start("BooksPostgresPipeline", spider.name)

        try:
            self.engine = create_engine(self.db_uri, echo=False)
            self.Session = sessionmaker(bind=self.engine)
            self.metadata = MetaData()

            # Определение таблицы books
            self.books_table = Table(
                "raw_books",
                self.metadata,
                Column("id", Integer, primary_key=True, autoincrement=True),
                Column("url", String(500), unique=True, nullable=False),
                Column("scraped_at", DateTime, default=datetime.utcnow),
                Column("title", String(500)),
                Column("price", Float),
                Column("price_raw", String(50)),
                Column("availability", String(100)),
                Column("availability_raw", Text),
                Column("available_quantity", Integer, default=0),
                Column("in_stock", Boolean, default=False),
                Column("availability_status", String(50)),
                Column("description", Text),
                Column("upc", String(50)),
                Column("product_type", String(100)),
                Column("price_excl_tax", Float),
                Column("price_excl_tax_raw", String(50)),
                Column("price_incl_tax", Float),
                Column("price_incl_tax_raw", String(50)),
                Column("tax", Float),
                Column("tax_raw", String(50)),
                Column("reviews_count", Integer),
                Column("rating", Integer),
                Column("rating_text", String(20)),
                Column("category", String(200)),
                Column("image_url", String(500)),
                Column("image_filename", String(500)),
                Column("data_quality_score", Float),
            )

            # Создание таблиц
            self.metadata.create_all(self.engine)

            table_info = (
                f"Таблица 'raw_books' "
                f"создана/проверена "
                f"({len(self.books_table.columns)} колонок)"
            )
            PipelineLogger.log_database_init(spider.name, table_info)

            # Проверяем настройку очистки таблицы
            self.should_clear_table = spider.settings.getbool(
                "CLEAR_TABLE_BEFORE_RUN", True
            )

            if self.should_clear_table:
                self.clear_table(spider)
            else:
                PipelineLogger.log_table_clearing(spider.name, "raw_books", 0)

        except SQLAlchemyError as e:
            PipelineLogger.log_database_error(
                spider.name, f"Ошибка подключения к БД: {e}"
            )
            raise

    def clear_table(self, spider):
        """
        Очистка таблицы raw_books перед началом нового парсинга.

        Удаляет все существующие записи из таблицы для предотвращения
        дублирования данных. Опционально, управляется настройкой
        CLEAR_TABLE_BEFORE_RUN.

        Args:
            spider: Объект Scrapy Spider
        """
        try:
            session = self.Session()

            # Проверяем, есть ли данные в таблице
            count_stmt = select(func.count()).select_from(self.books_table)
            record_count = session.execute(count_stmt).scalar()

            if record_count > 0:
                # Логируем начало очистки
                PipelineLogger.log_table_clearing(
                    spider.name, "raw_books", record_count
                )

                # Удаляем все записи
                delete_stmt = delete(self.books_table)
                result = session.execute(delete_stmt)
                session.commit()

                # Логируем результат
                PipelineLogger.log_table_clearing(
                    spider.name, "raw_books", result.rowcount
                )
            else:
                PipelineLogger.log_table_clearing(spider.name, "raw_books", 0)

            session.close()

        except SQLAlchemyError as e:
            session.rollback()
            PipelineLogger.log_database_error(
                spider.name, f"Ошибка очистки таблицы: {e}"
            )
            raise

    def process_item(self, item, spider):
        """
        Обработка и сохранение одного элемента данных в БД.

        Основной метод пайплайна, который:
        1. Проверяет наличие записи с таким же URL
        2. Обновляет существующую запись или создает новую
        3. Обрабатывает возможные ошибки БД
        4. Логирует результат операции

        Args:
            item (dict): Словарь с данными о книге
            spider: Объект Scrapy Spider

        Returns:
            dict: Исходный или обработанный элемент

        Raises:
            DropItem: При ошибках сохранения в БД
        """
        self.items_processed += 1

        try:
            session = self.Session()
            item_copy = item.copy()

            # Проверка на дубликаты по URL
            existing = session.execute(
                self.books_table.select().where(
                    self.books_table.c.url == item_copy["url"]
                )
            ).fetchone()

            if existing:
                # Обновление существующей записи
                update_stmt = (
                    self.books_table.update()
                    .where(self.books_table.c.url == item_copy["url"])
                    .values(**{k: v for k, v in item_copy.items() if v is not None})
                )
                session.execute(update_stmt)
                self.items_updated += 1

                # Логируем обновление
                PipelineLogger.log_item_save(
                    spider.name, item_copy["url"], True, spider.item_counter
                )
            else:
                # Вставка новой записи
                insert_stmt = self.books_table.insert().values(
                    **{k: v for k, v in item_copy.items() if v is not None}
                )
                session.execute(insert_stmt)
                self.items_inserted += 1

                # Логируем вставку
                PipelineLogger.log_item_save(
                    spider.name, item_copy["url"], False, spider.item_counter
                )

            session.commit()
            return item

        except SQLAlchemyError as e:
            session.rollback()

            # Логируем ошибку с деталями
            item_info = {
                "url": item.get("url", "N/A"),
                "title": item.get("title", "N/A")[:50],
                "error_fields": [k for k, v in item.items() if v is None],
            }

            PipelineLogger.log_database_error(
                spider.name, f"Ошибка сохранения в БД: {str(e)[:200]}", str(item_info)
            )

            raise DropItem(f"Database error: {e}")

        finally:
            session.close()

    def close_spider(self, spider):
        """
        Вызывается при завершении работы паука.

        Выполняет финальные операции:
        1. Закрывает соединение с БД
        2. Логирует статистику работы пайплайна
        3. Освобождает ресурсы

        Args:
            spider: Объект Scrapy Spider
        """
        if self.engine:
            self.engine.dispose()

            # Логируем статистику пайплайна
            stats = {
                "total_processed": self.items_processed,
                "inserted": self.items_inserted,
                "updated": self.items_updated,
                "success_rate": f"{
                    (self.items_inserted + self.items_updated)
                    / self.items_processed
                    * 100:.1f}%"
                if self.items_processed > 0
                else "0%",
            }

            PipelineLogger.log_pipeline_completion(
                spider.name, "BooksPostgresPipeline", self.items_processed
            )
            PipelineLogger.log_validation_stats(spider.name, stats)


class DataValidationPipeline:
    """Пайплайн для валидации и очистки данных."""

    def __init__(self):
        """
        Инициализация пайплайна валидации данных.

        Определяет наборы полей для валидации и инициализирует статистику.
        """
        self.logger = logging.getLogger(__name__)
        self.validation_stats = {
            "total_processed": 0,
            "cleaned_prices": 0,
            "cleaned_ints": 0,
            "fixed_dates": 0,
            "validation_errors": 0,
        }

        # Для простого паука
        self.required_fields = ["title", "price", "url"]
        self.optional_fields = ["description", "rating", "category"]

        # Поля, которые нужно преобразовать в float
        self.float_fields = ["price", "price_excl_tax", "price_incl_tax", "tax"]

        # Поля, которые нужно преобразовать в int
        self.int_fields = ["reviews_count", "rating", "available_quantity"]

    @classmethod
    def from_crawler(cls, crawler):
        """
        Фабричный метод для создания экземпляра пайплайна.

        Args:
            crawler: Объект Scrapy Crawler

        Returns:
            DataValidationPipeline: Экземпляр пайплайна валидации
        """
        pipeline = cls()
        PipelineLogger.log_pipeline_start("DataValidationPipeline", crawler.spider.name)
        return pipeline

    def process_item(self, item, spider):
        """
        Обработка и валидация одного элемента данных.

        Применяет правила очистки и валидации:
        1. Очистка числовых полей (цены, количество)
        2. Проверка обязательных полей
        3. Преобразование типов данных
        4. Логирование ошибок валидации

        Args:
            item (dict): Словарь с данными о книге
            spider: Объект Scrapy Spider

        Returns:
            dict: Очищенный и валидированный элемент
        """
        self.validation_stats["total_processed"] += 1

        try:
            cleaned_item = self.clean_item(item)

            # Проверяем обязательные поля
            missing_fields = [
                field for field in self.required_fields if not cleaned_item.get(field)
            ]
            if missing_fields:
                self.validation_stats["validation_errors"] += 1
                PipelineLogger.log_validation_error(
                    spider.name,
                    "required_fields",
                    missing_fields,
                    f"Отсутствуют обязательные поля: {missing_fields}",
                )

            return cleaned_item

        except Exception as e:
            self.validation_stats["validation_errors"] += 1
            PipelineLogger.log_validation_error(
                spider.name,
                "cleaning",
                str(item)[:100],
                f"Ошибка очистки данных: {str(e)[:100]}",
            )
            return item

    def clean_item(self, item):
        """
        Подробная очистка данных элемента.

        Выполняет специфические преобразования:
        1. Преобразование строковых цен в float
        2. Преобразование строковых чисел в int
        3. Нормализация формата даты scraped_at
        4. Обработка ошибок преобразования

        Args:
            item (dict): Словарь с исходными данными

        Returns:
            dict: Очищенный словарь данных
        """
        cleaned = item.copy()

        # Очищаем поля с ценами
        for field in self.float_fields:
            if field in cleaned and cleaned[field] is not None:
                if isinstance(cleaned[field], str):
                    try:
                        # Удаляем всё, кроме цифр и точки
                        original = cleaned[field]
                        cleaned[field] = float(re.sub(r"[^\d.]", "", cleaned[field]))
                        self.validation_stats["cleaned_prices"] += 1
                    except (ValueError, TypeError):
                        cleaned[field] = None
                        PipelineLogger.log_validation_error(
                            "DataValidationPipeline",
                            field,
                            original,
                            "Невозможно преобразовать в float",
                        )
                elif isinstance(cleaned[field], (int, float)):
                    # Уже число, оставляем как есть
                    pass

        # Преобразуем целочисленные поля
        for field in self.int_fields:
            if field in cleaned and cleaned[field] is not None:
                if isinstance(cleaned[field], str):
                    try:
                        original = cleaned[field]
                        cleaned[field] = int(cleaned[field])
                        self.validation_stats["cleaned_ints"] += 1
                    except (ValueError, TypeError):
                        cleaned[field] = 0
                        PipelineLogger.log_validation_error(
                            "DataValidationPipeline",
                            field,
                            original,
                            "Невозможно преобразовать в int",
                        )
                elif isinstance(cleaned[field], (int, float)):
                    cleaned[field] = int(cleaned[field])

        # Убедимся что scraped_at - это datetime
        if "scraped_at" in cleaned and isinstance(cleaned["scraped_at"], str):
            try:
                cleaned["scraped_at"] = datetime.fromisoformat(
                    cleaned["scraped_at"].replace("Z", "+00:00")
                )
                self.validation_stats["fixed_dates"] += 1
            except Exception:
                cleaned["scraped_at"] = datetime.utcnow()
                self.validation_stats["fixed_dates"] += 1

        return cleaned

    def close_spider(self, spider):
        """
        Вызывается при завершении работы паука.

        Логирует итоговую статистику валидации данных:
        1. Количество обработанных элементов
        2. Статистика по типам очистки
        3. Процент успешной валидации

        Args:
            spider: Объект Scrapy Spider
        """
        success_rate = (
            (
                (
                    self.validation_stats["total_processed"]
                    - self.validation_stats["validation_errors"]
                )
                / self.validation_stats["total_processed"]
                * 100
            )
            if self.validation_stats["total_processed"] > 0
            else 0
        )

        stats_summary = {
            "Обработано items": self.validation_stats["total_processed"],
            "Очищено цен": self.validation_stats["cleaned_prices"],
            "Очищено чисел": self.validation_stats["cleaned_ints"],
            "Исправлено дат": self.validation_stats["fixed_dates"],
            "Ошибок валидации": self.validation_stats["validation_errors"],
            "Успешность валидации": f"{success_rate:.1f}%",
        }

        PipelineLogger.log_validation_stats(spider.name, stats_summary)
        PipelineLogger.log_pipeline_completion(
            spider.name,
            "DataValidationPipeline",
            self.validation_stats["total_processed"],
        )
