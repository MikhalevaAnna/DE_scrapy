import argparse
import time
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    when,
    lit,
    row_number,
    dense_rank,
    md5,
    current_timestamp,
    lower,
)
from pyspark.sql.window import Window
from books_scraper.books_scraper.settings import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    MIN_QUALITY_VALUE,
)
from books_scraper.books_scraper.settings import POSTGRES_DB_URI
from sqlalchemy import create_engine, text
from datetime import datetime as dt

from etl_logger import setup_etl_logging
from database.schema_manager import SchemaManager


class BooksETL:
    def __init__(self, drop_existing=False):
        """
        Инициализация ETL-процесса обработки данных книг.

        Создает уникальный идентификатор запуска, инициализирует логгер,
        настраивает SparkSession и подготавливает конфигурацию
        для подключения к базе данных.

        Args:
            drop_existing (bool): Флаг для удаления существующих
                                таблиц DWH перед запуском
        """
        self.start_time = dt.now()
        self.etl_run_id = (
            f"etl_run_{self.start_time.strftime('%Y%m%d_%H%M%S')}_"
            f"{uuid.uuid4().hex[:8]}"
        )
        self.drop_existing = drop_existing

        # Инициализация продвинутого логгера
        self.etl_logger = setup_etl_logging("BooksETL")
        self.logger = self.etl_logger.logger

        # Конфигурация для логирования
        config = {
            "drop_existing": drop_existing,
            "dwh_schema": "dwh",
            "raw_schema": "public",
            "etl_run_id": self.etl_run_id,
        }
        self.etl_logger.log_etl_start(config)

        # Инициализация статистики
        self.stats = {
            "extraction": {},
            "transformation": {},
            "quality": {},
            "load": {},
            "timing": {},
        }

        try:
            self.etl_logger.log_stage_start(
                "Инициализация Spark", "Создание SparkSession"
            )

            # Создаем SparkSession с мониторингом
            self.spark = (
                SparkSession.builder.appName(f"BooksETL_{self.etl_run_id}")
                .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.executor.memory", "2g")
                .config("spark.driver.memory", "2g")
                .getOrCreate()
            )

            app_id = self.spark.sparkContext.applicationId
            self.logger.info(f"✅ SparkSession создан. App ID: {app_id}")

            # Параметры подключения
            self.db_properties = {
                "driver": "org.postgresql.Driver",
                "url": f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}"
                f"/{POSTGRES_DB}",
                "user": f"{POSTGRES_USER}",
                "password": f"{POSTGRES_PASSWORD}",
            }

            # Схемы
            self.raw_schema = "public"
            self.dwh_schema = "dwh"
            self.schema_manager = SchemaManager(drop_existing=self.drop_existing)

            self.etl_logger.log_stage_complete(
                "Инициализация Spark",
                {
                    "Spark Application ID": self.spark.sparkContext.applicationId,
                    "Spark Version": self.spark.version,
                },
            )

        except Exception as e:
            self.etl_logger.log_error(
                "Инициализация", f"Ошибка инициализации ETL: {str(e)}"
            )
            raise

    def initialize_dwh(self):
        """
        Инициализация хранилища данных (DWH) через SQLAlchemy.

        Создает схему DWH и все необходимые таблицы (фактовые таблицы
        и справочники) через SchemaManager.
        В случае ошибки логирует проблему и прекращает выполнение.

        Returns:
            bool: True если инициализация успешна, иначе исключение
        """
        self.etl_logger.log_stage_start(
            "Инициализация DWH", "Создание схемы и таблиц DWH"
        )

        try:
            start_time = time.time()
            success = self.schema_manager.initialize_dwh()
            duration = time.time() - start_time

            if success:
                self.etl_logger.log_stage_complete(
                    "Инициализация DWH",
                    {
                        "Статус": "Успешно",
                        "Время выполнения": f"{duration:.2f} сек",
                        "Схема": self.dwh_schema,
                    },
                )
            else:
                self.etl_logger.log_error(
                    "Инициализация DWH", "Не удалось инициализировать DWH"
                )
                raise Exception("Failed to initialize DWH")

            return success

        except Exception as e:
            self.etl_logger.log_error(
                "Инициализация DWH", f"Ошибка инициализации DWH: {str(e)}"
            )
            raise

    def extract_raw_data(self):
        """
        Извлечение необработанных данных из исходного слоя (raw layer).

        Загружает данные из таблицы raw_books в PostgreSQL через
        JDBC соединение, подсчитывает количество записей и собирает
        статистику по времени выполнения.

        Returns:
            DataFrame: PySpark DataFrame с сырыми данными книг

        Raises:
            Exception: При ошибках подключения или чтения данных
        """
        try:
            start_time = time.time()

            raw_df = (
                self.spark.read.format("jdbc")
                .option("url", self.db_properties["url"])
                .option("dbtable", "raw_books")
                .option("user", self.db_properties["user"])
                .option("password", self.db_properties["password"])
                .option("driver", self.db_properties["driver"])
                .load()
            )

            count = raw_df.count()
            duration = time.time() - start_time

            # УПРОЩЕННАЯ СТАТИСТИКА (без сложных вычислений)
            self.stats["extraction"] = {
                "records_extracted": count,
                "duration_seconds": duration,
            }

            self.logger.info(f"Извлечено {count} записей за {duration:.2f} сек")

            if count == 0:
                self.logger.warning("Таблица raw_books пуста!")

            return raw_df

        except Exception as e:
            self.logger.error(f"Ошибка извлечения: {e}")
            raise

    def transform_data(self, raw_df):
        """
        Преобразование сырых данных в структурированный формат DWH.

        Выполняет полный цикл трансформации:
        1. Очистка данных (удаление пробелов, обработка NULL)
        2. Дедубликация по UPC коду
        3. Создание справочников (категории, типы продуктов, рейтинги)
        4. Формирование фактовой таблицы с внешними ключами
        5. Анализ качества данных

        Args:
            raw_df (DataFrame): DataFrame с сырыми данными книг

        Returns:
            dict: Словарь с преобразованными DataFrame:
                - "books": Фактовая таблица книг
                - "categories": Справочник категорий
                - "product_types": Справочник типов продуктов
                - "ratings": Справочник рейтингов
        """
        self.etl_logger.log_stage_start(
            "Трансформация данных", "Очистка, дедубликация, создание справочников"
        )

        try:
            start_time = time.time()
            transformation_stats = {}
            # Инициализация quality_details для избежания ошибок
            self.stats["quality_details"] = {"low_quality_count": 0}

            # 1. Очистка данных
            self.logger.info("Этап 1: Очистка данных")
            cleaned_df = (
                raw_df.withColumn("title", trim(col("title")))
                .withColumn("category", trim(col("category")))
                .withColumn("product_type", trim(col("product_type")))
                .withColumn(
                    "price", when(col("price").isNull(), 0.0).otherwise(col("price"))
                )
                .withColumn(
                    "reviews_count",
                    when(col("reviews_count").isNull(), 0).otherwise(
                        col("reviews_count")
                    ),
                )
                .withColumn(
                    "rating", when(col("rating").isNull(), 0).otherwise(col("rating"))
                )
            )

            # Статистика очистки
            null_counts = {}
            for field in ["title", "category", "product_type", "price", "rating"]:
                null_count = cleaned_df.filter(col(field).isNull()).count()
                null_counts[f"null_{field}"] = null_count

            transformation_stats["cleaning"] = null_counts

            # 2. Дедубликация по UPC
            self.logger.info("Этап 2: Дедубликация данных")
            window_spec = Window.partitionBy("upc").orderBy("scraped_at")
            deduplicated_df = (
                cleaned_df.withColumn("row_num", row_number().over(window_spec))
                .filter(col("row_num") == 1)
                .drop("row_num")
            )

            total_before = cleaned_df.count()
            total_after = deduplicated_df.count()
            duplicates_removed = total_before - total_after

            transformation_stats["deduplication"] = {
                "records_before": total_before,
                "records_after": total_after,
                "duplicates_removed": duplicates_removed,
            }

            self.logger.info(f"  Удалено дубликатов: {duplicates_removed}")

            # 3. СОЗДАНИЕ СПРАВОЧНИКОВ (DIMENSIONS)

            # 3.1 Категории
            self.logger.info("Этап 3.1: Создание справочника категорий")
            categories_df = (
                deduplicated_df.filter(
                    (col("category").isNotNull()) & (col("category") != lit(""))
                )
                .select("category")
                .distinct()
                .withColumn("category_hash", md5(col("category")))
                .withColumn(
                    "category_id", dense_rank().over(Window.orderBy("category_hash"))
                )
                .select("category_id", "category")
                .withColumn("created_at", current_timestamp())
            )

            categories_count = categories_df.count()
            transformation_stats["categories"] = {"unique_categories": categories_count}

            # 3.2 Типы продуктов
            self.logger.info("Этап 3.2: Создание справочника типов продуктов")
            product_types_df = (
                deduplicated_df.filter(
                    (col("product_type").isNotNull()) & (col("product_type") != lit(""))
                )
                .select("product_type")
                .distinct()
                .withColumn("product_type_hash", md5(col("product_type")))
                .withColumn(
                    "product_type_id",
                    dense_rank().over(Window.orderBy("product_type_hash")),
                )
                .select("product_type_id", "product_type")
                .withColumn("created_at", current_timestamp())
            )

            product_types_count = product_types_df.count()
            transformation_stats["product_types"] = {
                "unique_product_types": product_types_count
            }

            # 3.3 Рейтинги
            self.logger.info("Этап 3.3: Загрузка справочника рейтингов")
            try:
                ratings_df = (
                    self.spark.read.format("jdbc")
                    .option("url", self.db_properties["url"])
                    .option("dbtable", f"{self.dwh_schema}.dim_ratings")
                    .option("user", self.db_properties["user"])
                    .option("password", self.db_properties["password"])
                    .option("driver", self.db_properties["driver"])
                    .load()
                )

                ratings_count = ratings_df.count()
                self.logger.info(f"  Загружено рейтингов из DWH: {ratings_count}")

                if ratings_count == 0:
                    self.logger.warning(
                        "Таблица рейтингов пуста, создаем дефолтные значения"
                    )
                    ratings_df = self.create_default_ratings()

            except Exception as e:
                self.logger.warning(
                    f"Не удалось загрузить рейтинги "
                    f"из DWH: {e}, создаем дефолтные значения"
                )
                ratings_df = self.create_default_ratings()

            # 4. ФАКТОВАЯ ТАБЛИЦА (FACT TABLE)
            self.logger.info("Этап 4: Создание фактовой таблицы")

            books_prepared_df = (
                deduplicated_df.filter(col("data_quality_score") >= MIN_QUALITY_VALUE)
                .withColumn(
                    "in_stock",
                    when(
                        lower(col("availability")).contains("in stock"), True
                    ).otherwise(False),
                )
                .withColumn(
                    "available_quantity",
                    when(col("available_quantity").isNull(), 0).otherwise(
                        col("available_quantity")
                    ),
                )
                .withColumn(
                    "availability_status",
                    when(lower(col("availability")).contains("in stock"), "in_stock")
                    .when(
                        lower(col("availability")).contains("out of stock"),
                        "out_of_stock",
                    )
                    .otherwise("unknown"),
                )
            )

            # Создание фактовой таблицы с внешними ключами
            books_df = (
                books_prepared_df.join(categories_df, "category", "left")
                .join(product_types_df, "product_type", "left")
                .join(
                    ratings_df,
                    books_prepared_df["rating"] == ratings_df["rating_value"],
                    "left",
                )
                .select(
                    col("upc").alias("book_id"),
                    col("title"),
                    col("description"),
                    col("price"),
                    col("in_stock"),
                    col("available_quantity"),
                    col("reviews_count"),
                    col("rating_id"),
                    col("category_id"),
                    col("product_type_id"),
                    col("image_url"),
                    col("url"),
                    col("scraped_at"),
                    current_timestamp().alias("processed_at"),
                )
            )

            # 4. ТАБЛИЦА КАЧЕСТВА ДАННЫХ
            bad_quality_df = (
                deduplicated_df.filter(col("data_quality_score") < MIN_QUALITY_VALUE)
                .withColumn(
                    "in_stock",
                    when(
                        lower(col("availability")).contains("in stock"), True
                    ).otherwise(False),
                )
                .withColumn(
                    "available_quantity",
                    when(col("available_quantity").isNull(), 0).otherwise(
                        col("available_quantity")
                    ),
                )
                .withColumn(
                    "availability_status",
                    when(lower(col("availability")).contains("in stock"), "in_stock")
                    .when(
                        lower(col("availability")).contains("out of stock"),
                        "out_of_stock",
                    )
                    .otherwise("unknown"),
                )
            )

            books_count = books_df.count()
            bad_quality_count = bad_quality_df.count()
            transformation_stats["fact_table"] = {"total_books": books_count}
            transformation_stats["bad_quality_books"] = bad_quality_count
            # 5. РАСЧЕТ КАЧЕСТВА ДАННЫХ

            self.logger.info("Этап 5: Детальный анализ качества данных по книгам")
            self.analyze_data_quality_details(bad_quality_df)

            duration = time.time() - start_time
            transformation_stats["duration_seconds"] = round(duration, 2)

            self.stats["transformation"] = transformation_stats

            self.etl_logger.log_stage_complete(
                "Трансформация данных",
                {
                    "Время выполнения": f"{duration:.2f} сек",
                    "Всего книг после трансформации": books_count,
                    "Уникальных категорий": categories_count,
                    "Уникальных типов продуктов": product_types_count,
                },
            )

            return {
                "books": books_df,
                "categories": categories_df,
                "product_types": product_types_df,
                "ratings": ratings_df,
            }

        except Exception as e:
            self.etl_logger.log_error(
                "Трансформация данных", f"Ошибка трансформации данных: {str(e)}"
            )
            raise

    def analyze_data_quality_details(self, low_quality_books):
        """
        Детальный анализ качества данных для книг с низким score.

        Анализирует книги с data_quality_score ниже порогового значения,
        логирует информацию о проблемных записях и собирает статистику.
        Используется для мониторинга проблем сбора данных.

        Args:
            low_quality_books (DataFrame): DataFrame с книгами,
                                          имеющими низкий quality score

        Returns:
            DataFrame: Исходный DataFrame для цепочки вызовов
        """
        try:
            # Инициализируем статистику
            if "quality_details" not in self.stats:
                self.stats["quality_details"] = {"low_quality_count": 0}

            self.logger.info("🔍 Детальный анализ качества данных по книгам...")

            # Получаем книги с низким качеством данных (< 85%)
            low_quality_count = low_quality_books.count()

            # Логируем статистику
            self.logger.info("📊 СТАТИСТИКА КАЧЕСТВА ДАННЫХ:")

            # 1. Книги с низким качеством (<85%)
            if low_quality_count > 0:
                self.logger.warning(
                    f"❌❌❌ НАЙДЕНЫ КНИГИ "
                    f"С НИЗКИМ КАЧЕСТВОМ ДАННЫХ "
                    f"(<{MIN_QUALITY_VALUE}%):"
                )
                self.logger.info(f"  ⚠️  Низкое качество у {low_quality_count} книг")

                # Получаем все book_id книг с низким качеством
                low_quality_ids = low_quality_books.select("upc").collect()
                low_quality_ids_list = [row["upc"] for row in low_quality_ids]

                # Выводим все book_id
                self.logger.warning(
                    f"    📋 UPC всех книг с низким "
                    f"качеством сбора данных"
                    f": {', '.join(low_quality_ids_list)}"
                )

                # Выводим детали по первым 10 книгам
                low_quality_details = (
                    low_quality_books.select("upc", "title", "data_quality_score")
                    .orderBy("data_quality_score")
                    .limit(10)
                    .collect()
                )

                self.logger.warning("    Примеры книг с низким качеством:")
                for book in low_quality_details:
                    title = book["title"] if book["title"] else "Без названия"
                    self.logger.warning(
                        f"      - {book['upc']}: '{title[:50]}...' - "
                        f"{book['data_quality_score']:.1f}%"
                    )

                if low_quality_count > 10:
                    self.logger.warning(
                        f"      ... и еще {low_quality_count - 10} книг"
                    )

            else:
                self.logger.info(
                    f"✅ Нет книг с качеством данных ниже {MIN_QUALITY_VALUE}%"
                )

            # Сохраняем статистику
            self.stats["quality_details"]["low_quality_count"] = low_quality_count

            return low_quality_books

        except Exception as e:
            self.logger.error(f"Ошибка при детальном анализе качества данных: {e}")
            # Все равно сохраняем статистику
            self.stats["quality_details"] = {"low_quality_count": 0}
            return low_quality_books

    def create_default_ratings(self):
        """
        Создание справочника рейтингов по умолчанию.

        Генерирует базовый набор рейтингов от 0 до 5 звезд с описаниями.
        Используется когда таблица dim_ratings не существует или пуста.

        Returns:
            DataFrame: PySpark DataFrame со справочником рейтингов
        """
        self.logger.info("Создание справочника рейтингов по умолчанию")

        ratings_df = self.spark.createDataFrame(
            [
                (1, 0, "Zero", "Без рейтинга"),
                (2, 1, "One", "Очень плохо"),
                (3, 2, "Two", "Плохо"),
                (4, 3, "Three", "Средне"),
                (5, 4, "Four", "Хорошо"),
                (6, 5, "Five", "Отлично"),
            ],
            ["rating_id", "rating_value", "rating_name", "rating_description"],
        )

        self.logger.info(f"Создано {ratings_df.count()} записей рейтингов по умолчанию")
        return ratings_df

    def load_to_dwh(self, dataframes):
        """
        Загрузка преобразованных данных в слой хранилища данных (DWH).

        Последовательно загружает данные в таблицы DWH в правильном порядке:
        1. Очистка существующих таблиц
        2. Загрузка справочников (dimensions)
        3. Загрузка фактовой таблицы
        4. Проверка целостности загрузки

        Args:
            dataframes (dict): Словарь с преобразованными DataFrame

        Returns:
            dict: Статистика загрузки по таблицам
        """
        self.etl_logger.log_stage_start(
            "Загрузка в DWH", "Загрузка трансформированных данных в DWH"
        )

        try:
            load_start_time = time.time()
            load_stats = {}

            # 1. Очищаем таблицы через SQLAlchemy
            self.logger.info("Шаг 1: Очистка таблиц DWH")
            try:
                engine = create_engine(POSTGRES_DB_URI)
                with engine.connect() as conn:
                    truncate_sql = f"""
                        TRUNCATE TABLE
                            {self.dwh_schema}.fact_books,
                            {self.dwh_schema}.dim_product_types,
                            {self.dwh_schema}.dim_categories
                        CASCADE;
                    """

                    try:
                        conn.execute(text(truncate_sql))
                        self.logger.info("✓ Таблицы очищены с помощью TRUNCATE CASCADE")
                    except Exception as e:
                        self.logger.warning(f"TRUNCATE CASCADE не удался: {e}")
                        self.delete_tables_in_order(conn)

                    conn.commit()

            except Exception as e:
                self.logger.error(f"Ошибка очистки таблиц: {e}")

            # 2. Загружаем данные в append режиме
            load_stats["tables_loaded"] = {}

            # Загрузка dim_categories
            self.logger.info("Шаг 2: Загрузка dim_categories")
            categories_count = dataframes["categories"].count()
            categories_start = time.time()
            dataframes["categories"].write.mode("append").jdbc(
                url=self.db_properties["url"],
                table=f"{self.dwh_schema}.dim_categories",
                properties=self.db_properties,
            )
            categories_duration = time.time() - categories_start
            load_stats["tables_loaded"]["dim_categories"] = {
                "records": categories_count,
                "duration_seconds": round(categories_duration, 2),
            }
            self.etl_logger.log_data_load(
                "dim_categories", categories_count, categories_duration
            )

            # Загрузка dim_product_types
            self.logger.info("Шаг 3: Загрузка dim_product_types")
            product_types_count = dataframes["product_types"].count()
            product_types_start = time.time()
            dataframes["product_types"].write.mode("append").jdbc(
                url=self.db_properties["url"],
                table=f"{self.dwh_schema}.dim_product_types",
                properties=self.db_properties,
            )
            product_types_duration = time.time() - product_types_start
            load_stats["tables_loaded"]["dim_product_types"] = {
                "records": product_types_count,
                "duration_seconds": round(product_types_duration, 2),
            }
            self.etl_logger.log_data_load(
                "dim_product_types", product_types_count, product_types_duration
            )

            # Загрузка dim_ratings (только если пустая)
            self.logger.info("Шаг 4: Загрузка dim_ratings")
            try:
                existing_ratings = (
                    self.spark.read.format("jdbc")
                    .option("url", self.db_properties["url"])
                    .option("dbtable", f"{self.dwh_schema}.dim_ratings")
                    .option("user", self.db_properties["user"])
                    .option("password", self.db_properties["password"])
                    .option("driver", self.db_properties["driver"])
                    .load()
                )

                if existing_ratings.count() == 0:
                    ratings_count = dataframes["ratings"].count()
                    ratings_start = time.time()
                    dataframes["ratings"].write.mode("append").jdbc(
                        url=self.db_properties["url"],
                        table=f"{self.dwh_schema}.dim_ratings",
                        properties=self.db_properties,
                    )
                    ratings_duration = time.time() - ratings_start
                    load_stats["tables_loaded"]["dim_ratings"] = {
                        "records": ratings_count,
                        "duration_seconds": round(ratings_duration, 2),
                        "status": "loaded",
                    }
                    self.etl_logger.log_data_load(
                        "dim_ratings", ratings_count, ratings_duration
                    )
                else:
                    self.logger.info(
                        "✓ Таблица dim_ratings уже содержит данные, пропускаем загрузку"
                    )
                    load_stats["tables_loaded"]["dim_ratings"] = {
                        "records": existing_ratings.count(),
                        "status": "skipped (already has data)",
                    }

            except Exception as e:
                self.logger.warning(
                    f"Не удалось проверить dim_ratings: {e}, загружаем данные"
                )
                ratings_count = dataframes["ratings"].count()
                ratings_start = time.time()
                dataframes["ratings"].write.mode("append").jdbc(
                    url=self.db_properties["url"],
                    table=f"{self.dwh_schema}.dim_ratings",
                    properties=self.db_properties,
                )
                ratings_duration = time.time() - ratings_start
                load_stats["tables_loaded"]["dim_ratings"] = {
                    "records": ratings_count,
                    "duration_seconds": round(ratings_duration, 2),
                    "status": "loaded (fallback)",
                }
                self.etl_logger.log_data_load(
                    "dim_ratings", ratings_count, ratings_duration
                )

            # Загрузка fact_books
            self.logger.info("Шаг 5: Загрузка fact_books")
            books_count = dataframes["books"].count()
            books_start = time.time()
            dataframes["books"].write.mode("append").jdbc(
                url=self.db_properties["url"],
                table=f"{self.dwh_schema}.fact_books",
                properties=self.db_properties,
            )
            books_duration = time.time() - books_start
            load_stats["tables_loaded"]["fact_books"] = {
                "records": books_count,
                "duration_seconds": round(books_duration, 2),
            }
            self.etl_logger.log_data_load("fact_books", books_count, books_duration)

            # 3. Проверка загрузки
            self.logger.info("Шаг 6: Проверка загрузки данных")
            verification_stats = self.verify_data_load()
            load_stats["verification"] = verification_stats

            total_duration = time.time() - load_start_time
            load_stats["total_duration_seconds"] = round(total_duration, 2)

            self.stats["load"] = load_stats

            self.etl_logger.log_stage_complete(
                "Загрузка в DWH",
                {
                    "Время выполнения": f"{total_duration:.2f} сек",
                    "Всего загружено таблиц": len(load_stats["tables_loaded"]),
                    "Всего записей fact_books": books_count,
                },
            )

            return load_stats

        except Exception as e:
            self.etl_logger.log_error(
                "Загрузка в DWH", f"Ошибка загрузки данных: {str(e)}"
            )
            raise

    def delete_tables_in_order(self, conn):
        """
        Резервный метод удаления данных из таблиц DWH.

        Удаляет записи из таблиц в правильном порядке зависимостей:
        1. fact_books (фактовая таблица)
        2. dim_product_types (справочник типов продуктов)
        3. dim_categories (справочник категорий)

        Используется при неудачной попытке TRUNCATE CASCADE.

        Args:
            conn: SQLAlchemy connection object
        """
        delete_queries = [
            f"DELETE FROM {self.dwh_schema}.fact_books",
            f"DELETE FROM {self.dwh_schema}.dim_product_types",
            f"DELETE FROM {self.dwh_schema}.dim_categories",
        ]

        for query in delete_queries:
            try:
                conn.execute(text(query))
                self.logger.debug(f"Выполнен: {query}")
            except Exception as e:
                self.logger.warning(f"Ошибка при выполнении {query}: {e}")

    def verify_data_load(self):
        """
        Верификация успешной загрузки данных в DWH.

        Проверяет наличие данных во всех таблицах DWH после загрузки,
        подсчитывает количество записей и логирует результаты.
        Позволяет выявить проблемы с загрузкой на раннем этапе.

        Returns:
            dict: Статистика проверки по каждой таблице
        """
        self.logger.info("Проверка загрузки данных в DWH...")

        verification_stats = {}

        try:
            tables_to_check = [
                ("dim_ratings", "Справочник рейтингов"),
                ("dim_categories", "Справочник категорий"),
                ("dim_product_types", "Справочник типов продуктов"),
                ("fact_books", "Фактовая таблица книг"),
            ]

            for table_name, table_description in tables_to_check:
                try:
                    df = (
                        self.spark.read.format("jdbc")
                        .option("url", self.db_properties["url"])
                        .option("dbtable", f"{self.dwh_schema}.{table_name}")
                        .option("user", self.db_properties["user"])
                        .option("password", self.db_properties["password"])
                        .option("driver", self.db_properties["driver"])
                        .load()
                    )

                    count = df.count()
                    verification_stats[table_name] = {
                        "records": count,
                        "status": "loaded" if count > 0 else "empty",
                    }

                    self.logger.info(f"  ✓ {table_description}: {count} записей")

                    if count == 0:
                        self.logger.warning(f"  ⚠️  Таблица {table_name} пуста!")

                except Exception as e:
                    verification_stats[table_name] = {
                        "records": 0,
                        "status": "error",
                        "error": str(e)[:100],
                    }
                    self.logger.error(f"  ❌ Ошибка чтения таблицы {table_name}: {e}")

            return verification_stats

        except Exception as e:
            self.logger.error(f"Ошибка проверки загрузки данных: {e}")
            return {"error": str(e)}

    def run(self):
        """
        Основной метод выполнения полного ETL-пайплайна.

        Оркестрирует выполнение всех этапов ETL:
        1. Инициализация DWH
        2. Извлечение данных
        3. Преобразование данных
        4. Загрузка в DWH
        5. Сбор финальной статистики

        Returns:
            dict: Итоговая статистика выполнения ETL

        Raises:
            Exception: При критических ошибках в любом из этапов
        """
        try:
            self.etl_logger.log_stage_start(
                "Запуск ETL процесса", f"ETL Run ID: {self.etl_run_id}"
            )

            # 1. Инициализация DWH (создание схемы и таблиц)
            self.initialize_dwh()

            # 2. Извлечение данных из raw слоя
            raw_data = self.extract_raw_data()

            if raw_data.count() == 0:
                self.etl_logger.log_error("Запуск ETL", "Нет данных для обработки")
                self.logger.error("Нет данных для обработки! Завершение работы.")
                return

            # 3. Трансформация данных
            transformed_data = self.transform_data(raw_data)

            # 4. Загрузка в DWH слой
            self.load_to_dwh(transformed_data)

            # 5. Сбор итоговой статистики
            total_duration = (dt.now() - self.start_time).total_seconds()

            total_stats = {
                "etl_run_id": self.etl_run_id,
                "total_duration_seconds": round(total_duration, 2),
                "extraction_records": (
                    self.stats["extraction"].get("records_extracted", 0)
                ),
                "transformation_books": (
                    self.stats["transformation"]
                    .get("fact_table", {})
                    .get("total_books", 0)
                ),
                "quality": self.stats["quality_details"].get("low_quality_count", {}),
                "tables_loaded": len(self.stats["load"].get("tables_loaded", {})),
                "spark_application_id": self.spark.sparkContext.applicationId,
                "status": "success",
            }

            # Сохраняем статистику в логгер
            self.etl_logger.add_statistic("total_stats", total_stats)

            self.etl_logger.log_completion(True, total_stats)
            self.logger.info("🎉 ETL процесс успешно завершен!")

            return total_stats

        except Exception as e:
            total_duration = (dt.now() - self.start_time).total_seconds()

            error_stats = {
                "etl_run_id": self.etl_run_id,
                "total_duration_seconds": round(total_duration, 2),
                "status": "failed",
                "error": str(e)[:200],
            }

            self.etl_logger.log_completion(False, error_stats)
            self.etl_logger.log_error(
                "Запуск ETL", f"ETL процесс завершился с ошибкой: {str(e)}"
            )

            raise

        finally:
            try:
                self.spark.stop()
                self.logger.info("✅ Spark сессия остановлена")
            except Exception as e:
                self.logger.warning(f"⚠️  Ошибка остановки Spark сессии: {e}")


if __name__ == "__main__":
    """
    Точка входа для запуска ETL-пайплайна из командной строки.

    Обрабатывает аргументы командной строки, инициализирует
    и запускает ETL процесс, а также управляет кодом возврата
    программы в зависимости от результата выполнения.

    Аргументы командной строки:
        --drop: Удаляет существующие таблицы DWH перед запуском
        --verbose (-v): Включает подробное логирование

    Exit codes:
        0: Успешное выполнение
        1: Ошибка выполнения ETL
    """
    parser = argparse.ArgumentParser(description="Run Books ETL Pipeline")
    parser.add_argument(
        "--drop", action="store_true", help="Drop existing DWH tables before run"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Запуск ETL
    try:
        etl = BooksETL(drop_existing=args.drop)
        result = etl.run()

        if result and result.get("status") == "success":
            print("\n✅ ETL успешно завершен!")
            print(f"   Run ID: {result['etl_run_id']}")
            print(f"   Время выполнения: {result['total_duration_seconds']} сек")
            print(f"   Загружено книг: {result['transformation_books']}")
            exit(0)
        else:
            print("\n❌ ETL завершился с ошибкой")
            exit(1)

    except Exception as e:
        print(f"\n💥 Критическая ошибка ETL: {e}")
        exit(1)
