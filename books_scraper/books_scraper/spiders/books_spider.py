import scrapy
import re
import os
import random
import time
from datetime import datetime
from urllib.parse import urljoin

from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from ..settings import FEED_SETTINGS
from .spider_logger import setup_spider_logging


class BooksToscrapeFullSpider(scrapy.Spider):
    name = "books_toscrape"
    allowed_domains = ["books.toscrape.com"]
    start_urls = ["http://books.toscrape.com/"]

    custom_settings = FEED_SETTINGS

    def __init__(self, *args, **kwargs):
        """
        Конструктор класса паука.

        Инициализирует базовый класс Scrapy Spider, устанавливает счетчики
        для мониторинга, задает временные параметры и настраивает систему
        логирования.

        Args:
            *args: Позиционные аргументы для родительского класса
            **kwargs: Именованные аргументы для родительского класса
        """
        super().__init__(*args, **kwargs)

        self.request_counter = 0
        self.item_counter = 0
        self.start_time = time.time()
        self.delay_range = (0.5, 2.0)  # Значение по умолчанию

        # Инициализация логгера
        self.custom_logger_manager = setup_spider_logging(self.name)
        self.custom_logger = self.custom_logger_manager.get_logger()
        self.pipeline_logger = self.custom_logger_manager.get_pipeline_logger()

    def start_requests(self):
        """
        Переопределенный метод start_requests для инициализации парсинга.

        Получает настройки из конфигурации Scrapy (задержки между запросами),
        логирует начало работы с информацией о конфигурации и запускает
        начальные запросы к стартовым URL.

        Yields:
            scrapy.Request: Начальный запрос к главной странице сайта
        """
        self.delay_range = self.settings.get("DOWNLOAD_DELAY_RANGE", (0.5, 2.0))

        settings_info = (
            f"Потоков: {self.settings.get('CONCURRENT_REQUESTS', 20)}, "
            f"Задержки: {self.delay_range[0]}-{self.delay_range[1]} сек"
        )

        # Логируем начало работы
        self.custom_logger_manager.log_start(settings_info)

        # Вызываем стандартный start_requests
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def get_random_delay(self):
        """
        Генератор случайных задержек между запросами.

        Создает случайное значение в заданном диапазоне для имитации
        человеческого поведения и предотвращения блокировки сервером.

        Returns:
            float: Случайное значение задержки в секундах
        """
        return random.uniform(*self.delay_range)

    def clean_price(self, price_text):
        """
        Очистка и нормализация текстового представления цены.

        Удаляет все нечисловые символы (символы валют, пробелы и т.д.)
        из строки с ценой и преобразует ее в float.

        Args:
            price_text (str): Сырое текстовое представление цены

        Returns:
            float: Числовое значение цены или None при ошибке
        """
        if price_text:
            cleaned = re.sub(r"[^\d.]", "", price_text)
            try:
                return float(cleaned)
            except ValueError:
                self.custom_logger.warning(f"Ошибка преобразования цены: {price_text}")
                return None
        return None

    def parse(self, response):
        """
        Основной метод парсинга списка книг на странице каталога.

        Обрабатывает страницу с перечнем книг:
        1. Извлекает ссылки на детальные страницы книг
        2. Логирует прогресс парсинга
        3. Для каждой книги создает SeleniumRequest для рендеринга JS
        4. Обрабатывает пагинацию для перехода на следующую страницу

        Args:
            response: Scrapy Response объект со страницей каталога

        Yields:
            SeleniumRequest: Запросы к детальным страницам книг
            scrapy.Request: Запрос для следующей страницы пагинации
        """
        self.request_counter += 1

        book_links = response.css(".product_pod h3 a::attr(href)").getall()

        # Логируем найденные ссылки
        self.custom_logger.debug(
            f"Найдено {len(book_links)} книг на странице {self.request_counter}"
        )

        # Логируем прогресс
        elapsed = time.time() - self.start_time
        self.custom_logger_manager.log_progress(
            self.request_counter, self.item_counter, elapsed
        )

        for book_link in book_links:
            absolute_url = urljoin(response.url, book_link)

            # Логируем отправку Selenium запроса
            self.custom_logger.debug(f"Отправка SeleniumRequest для: {absolute_url}")

            yield SeleniumRequest(
                url=absolute_url,
                callback=self.parse_book_detail,
                wait_time=3,
                script="""
                    // Прокрутка для имитации поведения пользователя
                    window.scrollTo(0, document.body.scrollHeight / 3);
                    setTimeout(() => {
                        window.scrollTo(0, document.body.scrollHeight * 2/3);
                    }, 500);
                    setTimeout(() => {
                        window.scrollTo(0, document.body.scrollHeight);
                    }, 1000);
                    return true;
                """,
                wait_until="domcontentloaded",
            )

        # Пагинация
        next_page = response.css(".next a::attr(href)").get()
        if next_page:
            next_page_url = urljoin(response.url, next_page)
            self.custom_logger.info(f"Переход на следующую страницу: {next_page_url}")

            yield scrapy.Request(
                next_page_url,
                callback=self.parse,
                meta={"download_delay": self.get_random_delay()},
                priority=10,
            )
        else:
            self.custom_logger.info("Пагинация завершена. Все страницы обработаны.")

    def parse_book_detail(self, response):
        """
        Детальный парсинг страницы отдельной книги.

        Извлекает всю информацию о книге со страницы товара:
        1. Демонстрация работы WebDriverWait для динамических элементов
        2. Извлечение основных атрибутов (название, цена, описание)
        3. Парсинг таблицы с техническими характеристиками
        4. Обработка информации о наличии товара
        5. Расчет качества собранных данных
        6. Логирование процесса

        Args:
            response: Scrapy Response объект со страницей книги

        Yields:
            dict: Словарь с полной информацией о книге в структурированном виде
        """
        self.item_counter += 1

        # Проверяем, доступен ли Selenium WebDriver
        driver = response.meta.get("driver", None)

        # Логируем обработку книги
        self.custom_logger_manager.log_book_detail(
            self.item_counter, response.url, driver is not None
        )

        # ДЕМОНСТРАЦИЯ ОЖИДАНИЯ ЭЛЕМЕНТОВ (WebDriverWait) - ДЛЯ ЗАДАНИЯ
        if driver and self.item_counter % 5 == 0:
            self.custom_logger.info(
                f"[Selenium Demo #{self.item_counter}] Начало "
                f"демонстрации WebDriverWait"
            )

            try:
                # Ждем появления заголовка книги
                wait = WebDriverWait(driver, 3)
                title_element = wait.until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".product_main h1")
                    )
                )
                title_text = (
                    title_element.text[:30] + "..."
                    if len(title_element.text) > 30
                    else title_element.text
                )
                self.custom_logger_manager.log_selenium_demo(
                    self.item_counter,
                    True,
                    f"WebDriverWait: заголовок '{title_text}' найден",
                )

                # Дополнительно: ждем таблицу с характеристиками
                wait.until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".table.table-striped")
                    )
                )
                self.custom_logger_manager.log_selenium_demo(
                    self.item_counter,
                    True,
                    "WebDriverWait: таблица характеристик найдена",
                )

                # Симуляция AJAX-взаимодействия
                try:
                    dynamic_elements = driver.find_elements(
                        By.CSS_SELECTOR, '[data-ajax-loaded="true"]'
                    )
                    if dynamic_elements:
                        self.custom_logger_manager.log_selenium_demo(
                            self.item_counter,
                            True,
                            f"Найдено AJAX-элементов: {len(dynamic_elements)}",
                        )
                except Exception as e:
                    self.custom_logger.debug(f"Поиск AJAX элементов не удался: {e}")

            except Exception as e:
                if "TimeoutException" in str(type(e)):
                    self.custom_logger_manager.log_selenium_demo(
                        self.item_counter,
                        False,
                        "Таймаут WebDriverWait (нормально для статичного сайта)",
                    )
                else:
                    self.custom_logger_manager.log_selenium_demo(
                        self.item_counter,
                        False,
                        f"Ошибка WebDriverWait: {str(e)[:100]}",
                    )

        item = {}

        # Обязательные поля
        item["url"] = response.url
        item["scraped_at"] = datetime.utcnow().isoformat()

        # Основная информация
        item["title"] = response.css(".product_main h1::text").get()

        # Цена (очищенная)
        price_text = response.css(".product_main .price_color::text").get()
        item["price"] = self.clean_price(price_text)
        item["price_raw"] = price_text

        # НАЛИЧИЕ
        availability = None

        # 1. Пробуем разные селекторы для таблицы
        table_selectors = [
            'table.table-striped th:contains("Availability") + td::text',
            'table th:contains("Availability") ~ td::text',
            'table tr:contains("Availability") td::text',
            'table tr:has(th:contains("Availability")) td::text',
        ]

        for selector in table_selectors:
            availability = response.css(selector).get()
            if availability:
                self.custom_logger.debug(
                    f"Найдено availability через селектор: {selector}"
                )
                break

        # 2. Если не нашли в таблице, ищем в основном блоке
        if not availability:
            availability = response.css(".product_main .availability::text").get()
            if availability:
                self.custom_logger.debug("Найдено availability в основном блоке")

        # 3. Дополнительные варианты
        if not availability:
            availability = response.css(".availability ::text").get()
            if availability:
                self.custom_logger.debug("Найдено availability через общий селектор")

        if availability:
            availability = availability.strip()
            item["availability_raw"] = availability

            # УЛУЧШЕННЫЙ ПОИСК КОЛИЧЕСТВА
            quantity = 0

            patterns = [
                (r"\((\d+)\s+available\)", 1),
                (r"available\s*[:\-]?\s*(\d+)", 1),
                (r"(\d+)\s+available", 1),
                (r"\((\d+)\s*in\s*stock\)", 1),
                (r"in\s+stock\s*[:\-]?\s*(\d+)", 1),
                (r"(\d+)\s+in\s+stock", 1),
                (r"stock\s*[:\-]?\s*(\d+)", 1),
                (r"\b(\d+)\s*(?:pcs?|items?|units?)\b", 1),
                (r"\b(\d+)\s+left\b", 1),
                (r"\b(\d+)\s+remaining\b", 1),
                (r"\b(\d+)\b", 1),
            ]

            for pattern, group in patterns:
                match = re.search(pattern, availability, re.IGNORECASE)
                if match:
                    try:
                        found = int(match.group(group))
                        if 0 < found < 1000:
                            quantity = found
                            self.custom_logger.debug(
                                f"Найдено количество {quantity} по паттерну: {pattern}"
                            )
                            break
                    except (ValueError, IndexError):
                        continue

            item["available_quantity"] = quantity

            # ОПРЕДЕЛЯЕМ НАЛИЧИЕ
            availability_lower = availability.lower()

            if any(
                phrase in availability_lower
                for phrase in [
                    "out of stock",
                    "not available",
                    "sold out",
                    "нет в наличии",
                ]
            ):
                item["in_stock"] = False
                item["availability_status"] = "out_of_stock"
                if quantity > 0:
                    item["available_quantity"] = 0
                    self.custom_logger.debug("Сброс количества для out_of_stock товара")

            elif any(
                phrase in availability_lower
                for phrase in ["in stock", "available", "есть в наличии"]
            ):
                item["in_stock"] = True
                item["availability_status"] = "in_stock"
                if quantity == 0:
                    item["available_quantity"] = 1
                    self.custom_logger.debug(
                        "Установка количества 1 для in_stock товара"
                    )

            else:
                if quantity > 0:
                    item["in_stock"] = True
                    item["availability_status"] = "in_stock"
                else:
                    item["in_stock"] = False
                    item["availability_status"] = "out_of_stock"

            # Очищаем текст
            cleaned = re.sub(r"\([^)]*\)", "", availability)
            cleaned = re.sub(r"\s+", " ", cleaned).strip()

            if not cleaned:
                if item["in_stock"]:
                    if item["available_quantity"] > 1:
                        cleaned = f"In stock ({item['available_quantity']} available)"
                    elif item["available_quantity"] == 1:
                        cleaned = "In stock (1 available)"
                    else:
                        cleaned = "In stock"
                else:
                    cleaned = "Out of stock"

            item["availability"] = cleaned

        else:
            item["availability"] = "Unknown"
            item["availability_raw"] = None
            item["available_quantity"] = 0
            item["in_stock"] = False
            item["availability_status"] = "unknown"
            self.custom_logger.warning(
                f"Для книги #{self.item_counter} не найдено информации о наличии"
            )

        # Описание
        item["description"] = response.css("#product_description + p::text").get()

        # Информация из таблицы
        rows = response.css(".table.table-striped tr")
        table_data = {}
        for row in rows:
            header = row.css("th::text").get()
            value = row.css("td::text").get()

            if header and value:
                table_data[header] = value

                if header == "UPC":
                    item["upc"] = value
                elif header == "Product Type":
                    item["product_type"] = value
                elif header == "Price (excl. tax)":
                    item["price_excl_tax"] = self.clean_price(value)
                    item["price_excl_tax_raw"] = value
                elif header == "Price (incl. tax)":
                    item["price_incl_tax"] = self.clean_price(value)
                    item["price_incl_tax_raw"] = value
                elif header == "Tax":
                    item["tax"] = self.clean_price(value)
                    item["tax_raw"] = value
                elif header == "Number of reviews":
                    item["reviews_count"] = int(value) if value else 0

        self.custom_logger.debug(f"Из таблицы извлечено {len(table_data)} полей")

        # Рейтинг
        rating_classes = response.css(".star-rating::attr(class)").get()
        if rating_classes:
            rating = rating_classes.split()[-1]
            rating_map = {
                "One": 1,
                "Two": 2,
                "Three": 3,
                "Four": 4,
                "Five": 5,
                "Zero": 0,
            }
            item["rating"] = rating_map.get(rating, 0)
            item["rating_text"] = rating
            self.custom_logger.debug(
                f"Найден рейтинг: {rating} -> {item['rating']} звезд"
            )

        # Категория
        item["category"] = response.css(
            ".breadcrumb li:nth-last-child(2) a::text"
        ).get()

        # Изображение
        image_url = response.css("#product_gallery img::attr(src)").get()
        if image_url:
            item["image_url"] = urljoin(response.url, image_url)
            item["image_filename"] = os.path.basename(image_url)
            self.custom_logger.debug(f"Найдено изображение: {item['image_filename']}")

        # Логируем полную информацию о книге
        self.custom_logger_manager.log_book_data(self.item_counter, item)

        # Расчет качества данных
        required_fields = [
            "title",
            "price",
            "upc",
            "category",
            "url",
            "product_type",
            "rating",
            "price_excl_tax",
            "price_incl_tax",
            "image_url",
            "description",
        ]
        filled_fields = sum(1 for field in required_fields if item.get(field))
        data_quality = (
            (filled_fields / len(required_fields)) * 100 if required_fields else 100
        )
        item["data_quality_score"] = round(data_quality, 2)

        self.custom_logger.debug(f"Качество данных: {data_quality:.1f}%")

        yield item

    def closed(self, reason):
        """
        Метод, вызываемый при завершении работы паука.

        Выполняет финальные операции:
        1. Расчет общего времени выполнения
        2. Логирование статистики выполнения
        3. Завершение работы логгера

        Args:
            reason (str): Причина завершения работы паука
                         ('finished', 'cancelled' и т.д.)
        """
        elapsed = time.time() - self.start_time

        # Логируем завершение через наш менеджер
        self.custom_logger_manager.log_completion(
            reason, self.request_counter, self.item_counter, elapsed
        )
