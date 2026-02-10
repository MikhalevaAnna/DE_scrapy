"""
Модуль для продвинутого логирования ETL процесса PySpark
"""

import logging
from pathlib import Path
from datetime import datetime
import sys
import json
from typing import Dict, Any


class ETLLogger:
    """Кастомный логгер для ETL процессов с записью в файл"""

    def __init__(self, etl_name: str, log_dir: str = "logs/etl"):
        self.etl_name = etl_name
        self.log_dir = Path(log_dir)
        self.stats = {}
        self.start_time = datetime.now()
        self.setup_logging()

    def setup_logging(self):
        """Настройка логирования с записью в файл и выводом в консоль"""
        # Создаем директорию для логов если не существует
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Формируем имя файла с датой
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.log_dir / f"{self.etl_name}_{timestamp}.log"

        # Создаем логгер
        self.logger = logging.getLogger(f"etl.{self.etl_name}")
        self.logger.setLevel(logging.DEBUG)

        # Очищаем существующие обработчики
        self.logger.handlers.clear()

        # Форматтер для логов
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # 1. Обработчик для записи в файл (все сообщения)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        # 2. Обработчик для вывода в консоль (только INFO и выше)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)

        # Добавляем обработчики
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        self.log_file_path = log_file
        self.logger.info("=" * 70)
        self.logger.info(f"🚀 ЗАПУСК ETL ПРОЦЕССА: {self.etl_name}")
        self.logger.info(f"Логи сохраняются в: {log_file}")
        self.logger.info(f"Время запуска: {self.start_time}")
        self.logger.info("=" * 70)

    def log_etl_start(self, config: Dict[str, Any]):
        """Логирование начала ETL процесса"""
        self.logger.info("📋 КОНФИГУРАЦИЯ ETL:")
        self.logger.info(f"  - Имя: {self.etl_name}")
        self.logger.info(f"  - Drop existing: {config.get('drop_existing', False)}")
        self.logger.info(f"  - DWH схема: {config.get('dwh_schema', 'dwh')}")
        self.logger.info(f"  - RAW схема: {config.get('raw_schema', 'public')}")
        self.logger.info("-" * 50)

    def log_stage_start(self, stage_name: str, stage_description: str = ""):
        """Логирование начала этапа ETL"""
        self.logger.info("▶️" * 25)
        self.logger.info(f"🔧 ЭТАП: {stage_name}")
        if stage_description:
            self.logger.info(f"📝 {stage_description}")
        self.logger.info("▶️" * 25)

    def log_stage_complete(self, stage_name: str, stats: Dict[str, Any] = None):
        """Логирование завершения этапа ETL"""
        self.logger.info("✅" * 25)
        self.logger.info(f"✓ ЗАВЕРШЕН ЭТАП: {stage_name}")
        if stats:
            for key, value in stats.items():
                self.logger.info(f"  {key}: {value}")
        self.logger.info("✅" * 25)

    def log_data_extraction(self, count: int, source: str, duration: float = None):
        """Логирование извлечения данных"""
        self.logger.info(f"📥 ИЗВЛЕЧЕНИЕ ДАННЫХ: {count} записей из {source}")
        if duration:
            self.logger.info(f"  Время извлечения: {duration:.2f} сек")
            self.logger.info(f"  Скорость: {count / duration:.1f} записей/сек")

    def log_data_transformation(self, transformation_stats: Dict[str, Any]):
        """Логирование трансформации данных"""
        self.logger.info("🔄 ТРАНСФОРМАЦИЯ ДАННЫХ:")
        for key, value in transformation_stats.items():
            self.logger.info(f"  {key}: {value}")

    def log_data_quality(self, quality_stats: Dict[str, Any]):
        """Логирование качества данных"""
        self.logger.info("📊 КАЧЕСТВО ДАННЫХ:")
        completeness = quality_stats.get("completeness_rate", 0)
        if completeness >= 90:
            self.logger.info(f"  ✓ Полнота данных: {completeness:.1f}%")
        elif completeness >= 70:
            self.logger.warning(f"  ⚠️  Полнота данных: {completeness:.1f}% (ниже 90%)")
        else:
            self.logger.error(
                f"  ❌ Полнота данных: {completeness:.1f}% (критически низко)"
            )

        for key, value in quality_stats.items():
            if key != "completeness_rate":
                self.logger.info(f"  {key}: {value}")

    def log_data_load(self, table_name: str, count: int, duration: float = None):
        """Логирование загрузки данных"""
        self.logger.info(f"📤 ЗАГРУЗКА: {count} записей в {table_name}")
        if duration:
            self.logger.info(f"  Время загрузки: {duration:.2f} сек")

    def log_error(self, stage: str, error: str, details: str = ""):
        """Логирование ошибок"""
        self.logger.error("❌" * 25)
        self.logger.error(f"ОШИБКА НА ЭТАПЕ '{stage}': {error}")
        if details:
            self.logger.error(f"Детали: {details}")
        self.logger.error("❌" * 25)

    def log_warning(self, warning: str, details: str = ""):
        """Логирование предупреждений"""
        self.logger.warning(f"⚠️  ПРЕДУПРЕЖДЕНИЕ: {warning}")
        if details:
            self.logger.warning(f"  Детали: {details}")

    def log_debug_info(self, info: str, data: Any = None):
        """Логирование отладочной информации"""
        if data:
            self.logger.debug(f"{info}: {data}")
        else:
            self.logger.debug(info)

    def log_completion(self, success: bool, total_stats: Dict[str, Any]):
        """Логирование завершения ETL процесса"""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()

        self.logger.info("=" * 70)
        if success:
            self.logger.info("🎉 ETL ПРОЦЕСС УСПЕШНО ЗАВЕРШЕН")
        else:
            self.logger.error("💥 ETL ПРОЦЕСС ЗАВЕРШИЛСЯ С ОШИБКАМИ")

        self.logger.info("=" * 70)
        self.logger.info("📈 СТАТИСТИКА ВЫПОЛНЕНИЯ:")
        self.logger.info(f"  Время начала: {self.start_time}")
        self.logger.info(f"  Время окончания: {end_time}")
        self.logger.info(f"  Общее время: {duration:.2f} сек")

        for key, value in total_stats.items():
            self.logger.info(f"  {key}: {value}")

        # Сохраняем статистику в JSON файл
        stats_file = (
            self.log_dir
            / f"{self.etl_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_stats.json"
        )
        stats_data = {
            "etl_name": self.etl_name,
            "success": success,
            "start_time": self.start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "statistics": total_stats,
        }

        try:
            with open(stats_file, "w", encoding="utf-8") as f:
                json.dump(stats_data, f, indent=2, ensure_ascii=False, default=str)
            self.logger.info(f"📁 Статистика сохранена в: {stats_file}")
        except Exception as e:
            self.logger.error(f"Ошибка сохранения статистики: {e}")

        self.logger.info("=" * 70)

    def add_statistic(self, key: str, value: Any):
        """Добавление статистики"""
        self.stats[key] = value

    def get_statistics(self) -> Dict[str, Any]:
        """Получение всей статистики"""
        return self.stats.copy()


def setup_etl_logging(etl_name: str):
    """Фабрика для создания ETL логгера"""
    return ETLLogger(etl_name)
