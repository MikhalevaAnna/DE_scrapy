### Запуск проекта
```
1. Установка зависимостей
pip install -r requirements.txt

2. Отредактируйте .env файл, добавьте свои ключи
cp .env.example .env

3. Запуск инфраструктуры
docker-compose up -d

4. Запуск парсера
cd books_scraper

5. Запуск только 5 записей /  Запуск всех записей
# scrapy crawl books_toscrape -s CLOSESPIDER_ITEMCOUNT=5 -L INFO
scrapy crawl books_toscrape

6. Запуск ETL процесса
python pyspark_etl.py
```
