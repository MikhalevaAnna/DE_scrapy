import scrapy
from itemloaders.processors import TakeFirst, MapCompose
import re


def clean_price(value):
    if value:
        return float(re.sub(r"[^\d.]", "", value))
    return None


def clean_text(value):
    if value:
        return value.strip()
    return None


class BookItem(scrapy.Item):
    # Обязательные поля
    url = scrapy.Field(output_processor=TakeFirst())
    scraped_at = scrapy.Field(output_processor=TakeFirst())

    # Основная информация
    title = scrapy.Field(
        input_processor=MapCompose(clean_text), output_processor=TakeFirst()
    )
    price = scrapy.Field(
        input_processor=MapCompose(clean_price), output_processor=TakeFirst()
    )
    availability = scrapy.Field(output_processor=TakeFirst())
    description = scrapy.Field(output_processor=TakeFirst())

    # Детальная информация
    upc = scrapy.Field(output_processor=TakeFirst())
    product_type = scrapy.Field(output_processor=TakeFirst())
    price_excl_tax = scrapy.Field(
        input_processor=MapCompose(clean_price), output_processor=TakeFirst()
    )
    price_incl_tax = scrapy.Field(
        input_processor=MapCompose(clean_price), output_processor=TakeFirst()
    )
    tax = scrapy.Field(
        input_processor=MapCompose(clean_price), output_processor=TakeFirst()
    )
    reviews_count = scrapy.Field(output_processor=TakeFirst())

    # Дополнительные поля
    rating = scrapy.Field(output_processor=TakeFirst())
    category = scrapy.Field(output_processor=TakeFirst())
    image_url = scrapy.Field(output_processor=TakeFirst())
