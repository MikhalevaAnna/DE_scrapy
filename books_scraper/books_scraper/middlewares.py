import random
import time
from scrapy import signals
import logging


class RandomDelayMiddleware:
    """
    Middleware для добавления случайных задержек между запросами с
    детальным логированием.
    """

    def __init__(self, delay_range):
        """
        Инициализация middleware случайных задержек.

        Args:
            delay_range (tuple): Диапазон задержек в секундах (min, max)
        """
        self.delay_range = delay_range
        self.logger = None  # Будет инициализирован при spider_opened
        self.total_delay = 0.0
        self.request_count = 0
        self.delays_by_url = {}  # Для статистики по URL

    @classmethod
    def from_crawler(cls, crawler):
        """
        Фабричный метод для создания экземпляра middleware из настроек Crawler.

        Получает конфигурацию задержек из settings.py и регистрирует обработчики
        сигналов spider_opened и spider_closed.

        Args:
            crawler: Объект Scrapy Crawler

        Returns:
            RandomDelayMiddleware: Экземпляр middleware
        """
        delay_range = crawler.settings.get("DOWNLOAD_DELAY_RANGE", (0.5, 2.0))

        # Если значение получено как список, преобразуем в tuple
        if isinstance(delay_range, list):
            delay_range = tuple(delay_range)

        middleware = cls(delay_range)

        # Подключаем сигналы
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)

        return middleware

    def spider_opened(self, spider):
        """
        Вызывается при открытии паука (начало парсинга).

        Инициализирует логгер и логирует все настройки, связанные с задержками:
        - Диапазон случайных задержек
        - Базовую задержку Scrapy
        - Настройки автотроттлинга (если включен)

        Args:
            spider: Объект Scrapy Spider
        """
        # Создаем логгер с именем паука.middlewares
        self.logger = logging.getLogger(f"{spider.name}.middlewares")

        # Логируем настройки
        self.logger.info("=" * 50)
        self.logger.info("🎯 ИНИЦИАЛИЗАЦИЯ RANDOM DELAY MIDDLEWARE")
        self.logger.info("=" * 50)
        self.logger.info(
            f"Диапазон задержек: "
            f"{self.delay_range[0]:.2f} - {self.delay_range[1]:.2f} сек"
        )
        self.logger.info(
            f"Базовая задержка: {spider.settings.get('DOWNLOAD_DELAY', 0):.2f} сек"
        )
        self.logger.info(
            f"Случайные задержки: "
            f"{spider.settings.get('RANDOMIZE_DOWNLOAD_DELAY', True)}"
        )
        self.logger.info(
            f"Автотроттлинг: {spider.settings.get('AUTOTHROTTLE_ENABLED', False)}"
        )

        if spider.settings.get("AUTOTHROTTLE_ENABLED"):
            self.logger.info(
                f"  Начальная задержка: "
                f"{spider.settings.get('AUTOTHROTTLE_START_DELAY', 5.0)} сек"
            )
            self.logger.info(
                f"  Макс. задержка: "
                f"{spider.settings.get('AUTOTHROTTLE_MAX_DELAY', 60.0)} сек"
            )

        self.logger.info("=" * 50)

    def spider_closed(self, spider):
        """
        Вызывается при завершении работы паука.

        Выводит детальную статистику по задержкам:
        - Общее количество запросов
        - Суммарное время всех задержек
        - Среднюю задержку на запрос
        - Топ-5 самых долгих запросов

        Args:
            spider: Объект Scrapy Spider
        """
        try:
            if self.request_count > 0:
                avg_delay = self.total_delay / self.request_count

                self.logger.info("=" * 50)
                self.logger.info("📊 СТАТИСТИКА ЗАДЕРЖЕК")
                self.logger.info("=" * 50)
                self.logger.info(f"Всего запросов: {self.request_count}")
                self.logger.info(f"Общая задержка: {self.total_delay:.2f} сек")
                self.logger.info(f"Средняя задержка: {avg_delay:.2f} сек")

                if self.delays_by_url:
                    min_delay = min(self.delays_by_url.values())
                    max_delay = max(self.delays_by_url.values())
                    self.logger.info(f"Минимальная задержка: {min_delay:.2f} сек")
                    self.logger.info(f"Максимальная задержка: {max_delay:.2f} сек")

                    # Логируем топ-5 самых долгих запросов
                    sorted_delays = sorted(
                        self.delays_by_url.items(), key=lambda x: x[1], reverse=True
                    )[:5]
                    self.logger.info("Топ-5 самых долгих запросов:")
                    for url, delay in sorted_delays:
                        self.logger.info(f"  {delay:.2f} сек: {url[:80]}...")
                else:
                    self.logger.info("Нет данных о задержках по URL")

                self.logger.info("=" * 50)
        except Exception as e:
            self.logger.error(f"Ошибка при логировании статистики задержек: {e}")

    def process_request(self, request, spider):
        """
        Обрабатывает каждый исходящий запрос, добавляя случайную задержку.

        Логика работы:
        1. Использует задержку из meta['download_delay'] или генерирует случайную
        2. Применяет задержку через time.sleep()
        3. Собирает статистику по задержкам
        4. Логирует прогресс каждые 50 запросов

        Args:
            request: Scrapy Request объект
            spider: Объект Scrapy Spider

        Returns:
            None: Позволяет продолжить обработку запроса
        """
        try:
            # Используем задержку из метаданных или генерируем случайную
            delay = request.meta.get(
                "download_delay", random.uniform(*self.delay_range)
            )

            # Логируем детали задержки (только для DEBUG или каждую 20-ю)
            if self.request_count % 20 == 0:
                self.logger.info(
                    f"⏱️  Задержка {delay:.2f} сек для {request.url[:60]}... "
                    f"(мета: {request.meta.get('download_delay', 'генерация')})"
                )
            else:
                self.logger.debug(f"Задержка {delay:.2f} сек для {request.url[:60]}...")

            # Применяем задержку
            time.sleep(delay)

            # Обновляем статистику
            self.total_delay += delay
            self.request_count += 1

            # Сохраняем задержку для UR
            url_key = (
                request.url.split("/")[-1] if len(request.url) > 50 else request.url
            )
            self.delays_by_url[url_key] = delay

            # Логируем прогресс каждые 50 запросов
            if self.request_count % 50 == 0:
                avg_delay = self.total_delay / self.request_count
                self.logger.info(
                    f"[Прогресс задержек] Запросов: {self.request_count}, "
                    f"Общая задержка: {self.total_delay:.1f} сек, "
                    f"Средняя: {avg_delay:.2f} сек"
                )

            return None
        except Exception as e:
            # Если произошла ошибка, просто пропускаем задержку и логирование
            spider.logger.error(f"Ошибка в RandomDelayMiddleware: {e}")
            return None


class SeleniumLoggingMiddleware:
    """Middleware для мониторинга и логирования использования Selenium запросов."""

    def __init__(self):
        """
        Инициализация middleware для логирования Selenium.

        Создает счетчики для мониторинга соотношения Selenium и обычных запросов.
        """
        self.logger = None
        self.selenium_requests = 0
        self.regular_requests = 0

    @classmethod
    def from_crawler(cls, crawler):
        """
        Фабричный метод для создания экземпляра middleware.

        Регистрирует обработчики сигналов spider_opened и spider_closed.

        Args:
            crawler: Объект Scrapy Crawler

        Returns:
            SeleniumLoggingMiddleware: Экземпляр middleware
        """
        middleware = cls()
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware

    def spider_opened(self, spider):
        """
        Вызывается при открытии паука.

        Проверяет и логирует конфигурацию Selenium из settings.py:
        - Включен ли Selenium
        - Тип драйвера (Chrome/Firefox)
        - Headless режим
        - Аргументы драйвера

        Args:
            spider: Объект Scrapy Spider
        """
        self.logger = logging.getLogger(f"{spider.name}.middlewares.selenium")

        # Проверяем настройки Selenium
        selenium_enabled = spider.settings.get("SELENIUM_DRIVER_NAME") is not None

        self.logger.info("=" * 50)
        self.logger.info("🔧 НАСТРОЙКИ SELENIUM")
        self.logger.info("=" * 50)
        self.logger.info(f"Selenium включен: {selenium_enabled}")

        if selenium_enabled:
            driver_name = spider.settings.get("SELENIUM_DRIVER_NAME", "chrome")
            driver_args = spider.settings.getlist("SELENIUM_DRIVER_ARGUMENTS", [])
            headless = "--headless" in driver_args

            self.logger.info(f"Драйвер: {driver_name}")
            self.logger.info(f"Headless режим: {headless}")
            self.logger.info(f"Аргументы: {driver_args[:3]}...")
        else:
            self.logger.warning("Selenium не настроен в settings.py")

        self.logger.info("=" * 50)

    def spider_closed(self, spider):
        """
        Вызывается при завершении работы паука.

        Анализирует и логирует статистику использования Selenium:
        - Общее количество запросов
        - Количество Selenium vs обычных запросов
        - Процентное соотношение типов запросов

        Args:
            spider: Объект Scrapy Spider
        """
        try:
            total_requests = self.selenium_requests + self.regular_requests

            if total_requests > 0:
                selenium_percent = (self.selenium_requests / total_requests) * 100

                self.logger.info("=" * 50)
                self.logger.info("📊 СТАТИСТИКА SELENIUM")
                self.logger.info("=" * 50)
                self.logger.info(f"Всего запросов: {total_requests}")
                self.logger.info(
                    f"Selenium запросов: "
                    f"{self.selenium_requests} "
                    f"({selenium_percent:.1f}%)"
                )
                self.logger.info(
                    f"Обычных запросов: {self.regular_requests} "
                    f"({100 - selenium_percent:.1f}%)"
                )

                if self.selenium_requests > 0:
                    self.logger.info(
                        "💡 Примечание: Selenium использовался "
                        "для детальных страниц книг"
                    )
                    self.logger.info(
                        "   для демонстрации работы с динамическим контентом"
                    )

                self.logger.info("=" * 50)
        except Exception as e:
            self.logger.error(f"Ошибка при логировании статистики Selenium: {e}")

    def process_request(self, request, spider):
        """
        Анализирует каждый запрос для определения типа (Selenium/обычный).

        Проверяет атрибуты запроса, чтобы определить, является ли он
        SeleniumRequest. Подсчитывает статистику по типам запросов.

        Args:
            request: Scrapy Request объект
            spider: Объект Scrapy Spider

        Returns:
            None: Позволяет продолжить обработку запроса
        """
        try:
            # Проверяем, является ли это SeleniumRequest
            is_selenium = hasattr(request, "wait_time") or "selenium" in request.meta

            if is_selenium:
                self.selenium_requests += 1
                # Логируем только каждую 10-ю Selenium запрос
                if self.selenium_requests % 10 == 0:
                    wait_time = getattr(request, "wait_time", "N/A")
                    self.logger.info(
                        f"🚗 Selenium запрос #{self.selenium_requests}: "
                        f"{request.url[:60]}... (ожидание: {wait_time} сек)"
                    )
            else:
                self.regular_requests += 1

            return None
        except Exception:
            return None
