import scrapy
import logging
import json
import time

class FixpriceSpider(scrapy.Spider):
    name = "fixprice"
    allowed_domains = ["fix-price.com"]

    start_urls = [
        'https://fix-price.com/catalog/kosmetika-i-gigiena/gigienicheskie-sredstva',
        'https://fix-price.com/catalog/dlya-zdorovykh-nachinaniy',
        'https://fix-price.com/catalog/pochti-darom'
    ]


    # start_urls = [
    #     'https://fix-price.com/catalog/kosmetika-i-gigiena/gigienicheskie-sredstva'
    # ]

    api_base_url = "https://api.fix-price.com/buyer/v1/product/in/"
    page = 1  # Начальный номер страницы
    limit = 24  # Количество товаров на странице
    sort = "sold"

    def start_requests(self):
        cookies = {
            'locality': '%7B%22city%22%3A%22%D0%95%D0%BA%D0%B0%D1%82%D0%B5%D1%80%D0%B8%D0%BD%D0%B1%D1%83%D1%80%D0%B3%22%2C%22cityId%22%3A55%2C%22longitude%22%3A60.597474%2C%22latitude%22%3A56.838011%2C%22prefix%22%3A%22%D0%B3%22%7D'
        }

        for url in self.start_urls:
            alias = url.split('catalog/')[-1] 
            api_url = f"{self.api_base_url}{alias}?page={self.page}&limit={self.limit}&sort={self.sort}"

            body = {
                "page": self.page,
                "limit": self.limit,
                "sort": self.sort
            }

            request = scrapy.Request(url=api_url, method="POST", cookies=cookies, body=json.dumps(body), callback=self.parse)
            logging.info(f"Отправка POST запроса на: {request.url} с cookies: {request.cookies} и телом: {request.body}")
            yield request

    
    def parse(self, response):
        logging.info(f"Получен ответ от: {response.url} со статусом: {response.status}")
    
        data = json.loads(response.body)
        total_items = len(data)
        logging.info(f"Общее количество товаров на странице: {total_items}")
        logging.info(f"___________________________________________________")
    
        all_product_data = []
    
        for item in data:
            try:
                product_data = {
                    "timestamp": int(time.time()),
                    "RPC": item["sku"],
                    "url": f"https://fix-price.com/catalog/{item['url']}",
                    "title": item["title"],
                    "marketing_tags": [],
                    "brand": item["brand"]["title"] if item["brand"] else "",
                    "section": self.get_section_hierarchy(item["category"]),
                    "price_data": self.get_price_data(item),
                    "stock": {
                        "in_stock": item["inStock"] > 0,
                        "count": item["inStock"]
                    },
                    "assets": {
                        "main_image": item["images"][0]["src"] if item["images"] else "",
                        "set_images": [img["src"] for img in item["images"]],
                        "view360": [],
                        "video": []
                    },
                    "metadata": {
                        "__description": "",
                    },
                    "variants": item["variantCount"]
                }
    
                all_product_data.append(product_data)
                time.sleep(3)
                
                yield scrapy.Request(
                    url=product_data["url"],
                    callback=self.parse_product_page,
                    meta={"product_data": product_data}
                )

                logging.info(f"Отправлен запрос на страницу товара: {product_data['url']}") 

            except Exception as e:
                logging.error(f"Ошибка при обработке товара: {item.get('sku', 'Unknown SKU')}, ошибка: {e}")
    
        # Проверяем, обработаны ли все товары без ошибок
        if len(all_product_data) == total_items:
            logging.info("Все товары обработаны без ошибок.")
        else:
            errors_count = total_items - len(all_product_data)
            logging.warning(f"Обработано с ошибками: {errors_count} товаров.")
    
        # Проверяем, если количество товаров равно лимиту, продолжаем запросы на следующую страницу
        if total_items == self.limit:
            self.page += 1  # Увеличиваем страницу
            alias = response.url.split('product/in/')[-1].split('?')[0]
            next_api_url = f"{self.api_base_url}{alias}?page={self.page}&limit={self.limit}&sort={self.sort}"
    
            body = {
                "page": self.page,
                "limit": self.limit,
                "sort": self.sort
            }
    
            logging.info(f"Запрашиваем следующую страницу: {self.page}")
            logging.info(f"___________________________________________")
            next_request = scrapy.Request(url=next_api_url, method="POST", cookies=response.request.cookies, body=json.dumps(body), callback=self.parse)
            yield next_request

    def get_section_hierarchy(self, category):
        hierarchy = [category["title"]]
        parent = category.get("parentCategory")
        while parent:
            hierarchy.insert(0, parent["title"])
            parent = parent.get("parentCategory")
        return hierarchy


    def get_price_data(self, item):
        special_price = item.get("specialPrice")
        price = item["price"]

        if isinstance(special_price, str):
            current_price = float(special_price)
        elif isinstance(price, str):
            current_price = float(price)
        elif isinstance(special_price, dict) and "value" in special_price:
            current_price = float(special_price["value"])
        elif isinstance(price, dict) and "value" in price:
            current_price = float(price["value"])
        else:
            self.logger.warning(f"Неизвестный формат цены: specialPrice={special_price}, price={price}")
            current_price = None 


        original_price = float(item["price"])
        sale_tag = ""
        if current_price < original_price:
            discount_percentage = round((original_price - current_price) / original_price * 100)
            sale_tag = f"Скидка {discount_percentage}%"
        return {
            "current": current_price,
            "original": original_price,
            "sale_tag": sale_tag
        }

    def parse_product_page(self, response):
        product_data = response.meta["product_data"]

        # Парсинг описания (исправлено)
        description = response.css(".product-details meta[itemprop='description']::attr(content)").get()
        product_data["metadata"]["__description"] = description.strip() if description else ""

        # Парсинг характеристик (исправлено)
        properties = response.css(".properties .property")
        for prop in properties:
            key = prop.css(".title::text").get().strip()
            value = prop.css(".value ::text").get()  # Добавляем пробел перед ::text
            if value:
                value = value.strip()
            else:
                value = ""
            product_data["metadata"][key] = value

        yield product_data
