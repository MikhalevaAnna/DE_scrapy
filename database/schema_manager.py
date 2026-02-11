"""
Модуль для управления схемой DWH через SQLAlchemy
"""

import argparse
import logging
from sqlalchemy import create_engine, text, inspect, MetaData
from sqlalchemy.exc import SQLAlchemyError
from books_scraper.books_scraper.settings import POSTGRES_DB_URI


logger = logging.getLogger(__name__)


class SchemaManager:
    def __init__(self, drop_existing=False):
        """
        Инициализация менеджера схемы

        Args:
            drop_existing: Если True, удаляет существующие таблицы
            перед созданием
        """
        self.dwh_schema = "dwh"
        self.drop_existing = drop_existing

        # Создаем engine
        self.engine = create_engine(POSTGRES_DB_URI, echo=False)

        # Метаданные для схемы DWH
        self.metadata = MetaData(schema=self.dwh_schema)

    def create_schema(self):
        """Создание схемы DWH если она не существует"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.dwh_schema}"))
                conn.commit()
            logger.info(f"Schema '{self.dwh_schema}' created/verified")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Error creating schema: {e}")
            return False

    def drop_tables(self):
        """Удаление всех таблиц в схеме DWH"""
        try:
            with self.engine.connect() as conn:
                # 1. Сначала удаляем foreign key constraints
                conn.execute(
                    text(f"""
                    DO $$
                    DECLARE
                        r RECORD;
                    BEGIN
                        FOR r IN (SELECT conname, tablename
                                 FROM pg_constraint
                                 JOIN pg_class ON conrelid = pg_class.oid
                                 JOIN pg_namespace
                                 ON pg_class.relnamespace = pg_namespace.oid
                                 WHERE nspname = '{self.dwh_schema}'
                                 AND contype = 'f')
                        LOOP
                            EXECUTE
                            'ALTER TABLE {self.dwh_schema}.' || r.tablename ||
                                    ' DROP CONSTRAINT ' || r.conname;
                        END LOOP;
                    END $$;
                """)
                )

                # 2. Удаляем таблицы с CASCADE
                tables_to_drop = [
                    f"{self.dwh_schema}.fact_books",
                    f"{self.dwh_schema}.dim_product_types",
                    f"{self.dwh_schema}.dim_categories",
                    f"{self.dwh_schema}.dim_ratings",
                ]

                for table in tables_to_drop:
                    try:
                        conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
                        logger.debug(f"Dropped table: {table}")
                    except Exception as e:
                        logger.warning(f"Error dropping table {table}: {e}")

                conn.commit()
            logger.info(f"All tables in schema '{self.dwh_schema}' dropped")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Error dropping tables: {e}")
            return False

    def check_tables_exist(self):
        """Проверка существования таблиц в схеме DWH"""
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names(schema=self.dwh_schema)

            required_tables = [
                "dim_ratings",
                "dim_categories",
                "dim_product_types",
                "fact_books",
            ]

            missing_tables = [table for table in required_tables if table not in tables]

            if missing_tables:
                logger.warning(
                    f"Missing tables in schema '{self.dwh_schema}': {missing_tables}"
                )
                return False, missing_tables
            else:
                logger.info(f"All required tables exist in schema '{self.dwh_schema}'")
                return True, []

        except SQLAlchemyError as e:
            logger.error(f"Error checking tables: {e}")
            return False, []

    def create_tables(self):
        """Создание всех таблиц в схеме DWH"""
        try:
            # Создаем все таблицы по одной с отдельными транзакциями
            logger.info("Starting DWH table creation...")
            tables_sql = [
                (
                    "dim_ratings",
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.dwh_schema}.dim_ratings (
                        rating_id INTEGER PRIMARY KEY,
                        rating_value INTEGER NOT NULL UNIQUE
                        CHECK (rating_value BETWEEN 0 AND 5),
                        rating_name VARCHAR(10) NOT NULL,
                        rating_description VARCHAR(100),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """,
                ),
                (
                    "dim_categories",
                    f"""
                    CREATE TABLE IF NOT EXISTS
                        {self.dwh_schema}.dim_categories (
                        category_id INTEGER PRIMARY KEY,
                        category VARCHAR(200) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """,
                ),
                (
                    "dim_product_types",
                    f"""
                    CREATE TABLE IF NOT EXISTS
                        {self.dwh_schema}.dim_product_types (
                        product_type_id INTEGER PRIMARY KEY,
                        product_type VARCHAR(200) NOT NULL UNIQUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """,
                ),
                (
                    "fact_books",
                    f"""
                    CREATE TABLE IF NOT EXISTS
                        {self.dwh_schema}.fact_books (
                        book_id VARCHAR(50) PRIMARY KEY,
                        title VARCHAR(500) NOT NULL,
                        description TEXT,
                        price NUMERIC(10,2),
                        in_stock BOOLEAN DEFAULT FALSE,
                        available_quantity INTEGER DEFAULT 0,
                        reviews_count INTEGER,
                        rating_id INTEGER,
                        category_id INTEGER,
                        product_type_id INTEGER,
                        image_url VARCHAR(500),
                        url VARCHAR(500),
                        scraped_at TIMESTAMP,
                        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """,
                ),
            ]

            for table_name, sql in tables_sql:
                try:
                    with self.engine.connect() as conn:
                        conn.execute(text(sql))
                        conn.commit()
                    logger.info(f"✓ Таблица {table_name} создана")
                except SQLAlchemyError as e:
                    logger.error(f"✗ Ошибка создания таблицы {table_name}: {e}")
                    # Продолжаем с другими таблицами

            logger.info("Adding foreign keys...")
            self.add_foreign_keys()

            logger.info("Populating ratings table...")
            self.populate_ratings()

            logger.info(f"All DWH tables created in schema '{self.dwh_schema}'")
            return True

        except SQLAlchemyError as e:
            logger.error(f"Error in create_tables: {e}")
            return False

    def add_foreign_keys(self):
        """Добавление foreign key constraints"""
        try:
            with self.engine.connect() as conn:
                # Добавляем foreign keys к fact_books
                conn.execute(
                    text(f"""
                    ALTER TABLE {self.dwh_schema}.fact_books
                    ADD CONSTRAINT fk_fact_books_ratings
                    FOREIGN KEY (rating_id)
                    REFERENCES {self.dwh_schema}.dim_ratings(rating_id)
                """)
                )

                conn.execute(
                    text(f"""
                    ALTER TABLE {self.dwh_schema}.fact_books
                    ADD CONSTRAINT fk_fact_books_categories
                    FOREIGN KEY (category_id)
                    REFERENCES {self.dwh_schema}.dim_categories(category_id)
                """)
                )

                conn.execute(
                    text(f"""
                    ALTER TABLE {self.dwh_schema}.fact_books
                    ADD CONSTRAINT fk_fact_books_product_types
                    FOREIGN KEY (product_type_id)
                    REFERENCES
                    {self.dwh_schema}.dim_product_types(product_type_id)
                """)
                )

                conn.commit()
            logger.info("✓ Foreign key constraints added")
            return True
        except SQLAlchemyError as e:
            logger.warning(f"⚠️  Error adding foreign keys (might already exist): {e}")
            return False

    def populate_ratings(self):
        """Заполнение таблицы рейтингов статичными данными"""
        try:
            ratings_data = [
                (1, 0, "Zero", "Без рейтинга"),
                (2, 1, "One", "Очень плохо"),
                (3, 2, "Two", "Плохо"),
                (4, 3, "Three", "Средне"),
                (5, 4, "Four", "Хорошо"),
                (6, 5, "Five", "Отлично"),
            ]

            with self.engine.connect() as conn:
                # Используем ON CONFLICT DO NOTHING для избежания дубликатов
                for rating in ratings_data:
                    conn.execute(
                        text(f"""
                        INSERT INTO {self.dwh_schema}.dim_ratings
                        (rating_id, rating_value, rating_name,
                        rating_description)
                        VALUES (:id, :value, :name, :desc)
                        ON CONFLICT (rating_id) DO NOTHING
                    """),
                        {
                            "id": rating[0],
                            "value": rating[1],
                            "name": rating[2],
                            "desc": rating[3],
                        },
                    )

                conn.commit()
            logger.info("✓ Ratings table populated with static data")
            return True
        except SQLAlchemyError as e:
            logger.error(f"✗ Error populating ratings table: {e}")
            return False

    def initialize_dwh(self):
        """
        Инициализация DWH: создание схемы и таблиц

        Returns:
            bool: True если успешно, False если ошибка
        """
        try:
            # Создаем схему
            if not self.create_schema():
                return False

            # Проверяем существование таблиц
            tables_exist, missing_tables = self.check_tables_exist()

            # Если требуется удаление существующих таблиц
            if self.drop_existing and tables_exist:
                logger.info("Dropping existing tables...")
                if not self.drop_tables():
                    logger.warning("Failed to drop tables, attempting to create anyway")
                tables_exist = False

            # Если таблицы не существуют или некоторые отсутствуют, создаем их
            if not tables_exist or missing_tables:
                logger.info("Creating DWH tables...")
                if not self.create_tables():
                    return False

            logger.info("✓ DWH initialization completed successfully")
            return True

        except Exception as e:
            logger.error(f"✗ Error initializing DWH: {e}")
            return False


def main():
    """Основная функция для запуска из командной строки"""

    parser = argparse.ArgumentParser(description="Initialize DWH schema and tables")
    parser.add_argument(
        "--drop", action="store_true", help="Drop existing tables before creation"
    )
    parser.add_argument(
        "--check", action="store_true", help="Only check if tables exist"
    )
    parser.add_argument(
        "--quality-history", type=int, help="Show data quality history (last N records)"
    )

    args = parser.parse_args()

    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    manager = SchemaManager(drop_existing=args.drop)

    if args.quality_history:
        # Показать историю качества данных
        history = manager.get_data_quality_history(args.quality_history)
        print(
            f"\n📊 История качества данных (последние {args.quality_history} записей):"
        )
        print("-" * 80)
        for record in history:
            print(
                f"ID: {record['id']}, Оценка: {record['score']}%, "
                f"Дата: {record['created_at']}, Run ID: {record['run_id']}"
            )
            if record["notes"]:
                print(f"  Примечания: {record['notes']}")
        print("-" * 80)
    elif args.check:
        # Только проверка
        tables_exist, missing = manager.check_tables_exist()
        if tables_exist:
            print("✅ All DWH tables exist")
        else:
            print(f"❌ Missing tables: {missing}")
    else:
        # Полная инициализация
        if manager.initialize_dwh():
            print("✅ DWH initialized successfully")
        else:
            print("❌ DWH initialization failed")
            exit(1)


if __name__ == "__main__":
    main()
