# встроенные модули
import json
import re
from datetime import datetime
import time
import concurrent.futures

# модули для парсинга
import requests
from bs4 import BeautifulSoup

from tqdm import tqdm
import pandas as pd


class WebsiteAccessError(Exception):
    """Ошибка, используемая в случае, если по какой-то причине сайт не вернет ожиданемую страницу"""
    def __init__(self, value):
        self.value = value


def get_html_soup(url):
    """Делает запрос к сайту, используя адрес и возвращает веб-страницу как объект BeautifulSoup"""
    response = requests.get(url)

    if response.status_code != 200:
        raise WebsiteAccessError(f"Request to {url} failed. Status code: {response.status_code}")

    return BeautifulSoup(response.content, "html.parser")


def parse_ad_urls_by_brand(brand):
    """Собирает с сайта ссылки на объявления о продаже автомобилей в Москве определенной марки"""
    base_url = f"https://auto.ru/moskva/cars/{brand}/used/"
    first_page_soup = get_html_soup(base_url)
    page_nums = first_page_soup.find_all("a", class_="ListingPagination-module__page")
    page_count = int(page_nums[-1].text) if page_nums else 1
    urls_by_brand = []

    for page in tqdm(range(1, page_count + 1)):
        if page == 1:
            soup = first_page_soup
        else:
            url = base_url + f"?page={page}"
            soup = get_html_soup(url)

        a_tags = soup.find_all("a", class_="ListingItemTitle-module__link")
        links = [tag["href"] for tag in a_tags]
        urls_by_brand.extend(links)

    return urls_by_brand


def parse_ad_urls(brands):
    """Собирает ссылки на объявления для разных марок машин"""
    urls = []

    for brand in brands:
        urls_by_brand = parse_ad_urls_by_brand(brand)
        urls.extend(urls_by_brand)

    return urls


def get_ad_urls():
    """
    Возвращает список ссылок на объявления.
    Если уже был создан файл, содержащий список, то возвращает его содержимое.
    Если файл не был создан, парсит список с сайта, сохраняет в файл и возвращает его.
    """
    try:
        with open("ad_urls.json") as f:
            urls = json.loads(f.read())
    except FileNotFoundError:
        urls = []

    if not urls:
        with open("car_brands.json") as f:
            car_brands = json.loads(f.read())

        urls = parse_ad_urls(car_brands)
        with open("ad_urls.json", "w+") as f:
            f.write(json.dumps(urls))

    return urls


def parse_model_specs(url):
    """Парсит характеристики модели автомобиля"""
    try:
        soup = get_html_soup(url)
    except (WebsiteAccessError, Exception):
        return {
            "number_of_doors": None,
            "country_of_brand": None,
            "acceleration": None,
            "clearance_min": None,
            "fuel_rate": None
        }

    labels_elements = soup.find_all("dt", class_="list-values__label")
    labels = [item.text for item in labels_elements]

    values_elements = soup.find_all("dd", class_="list-values__value")
    values = [item.text for item in values_elements]

    def get_value_by_label(label, *, value_type="str"):
        """Возвращает значение характеристики по ее названию и преобразовывает его в нужный тип данных"""
        if label not in labels:
            return None

        text = values[labels.index(label)]
        if value_type == "str":
            return text
        if value_type == "int":
            return int("".join(re.findall(r"\d", text)))
        if value_type == "float":
            return float("".join(re.findall(r"[\d.]", text)))

    return {
        "number_of_doors": get_value_by_label("Количество дверей", value_type="int"),
        "country_of_brand": get_value_by_label("Страна марки"),
        "acceleration": get_value_by_label("Разгон", value_type="float"),
        "clearance_min": get_value_by_label("Клиренс", value_type="int"),
        "fuel_rate": get_value_by_label("Расход", value_type="float")
    }


def parse_ad(url):
    """Парсит объявление и возвращает объект с информацией о автомобиле"""

    def get_value_from_info_row(row_name, *, value_type="str"):
        """Возвращает значение показателя автомобиля по ее названию и преобразовывает его в нужный тип данных"""
        elements = soup.select(f".CardInfoRow_{row_name} span:last-child")
        if not elements:
            return None

        text = elements[0].text
        if value_type == "str":
            return text.replace("\xa0", " ")
        if value_type == "int":
            return int("".join(re.findall(r"\d", text)))

    soup = get_html_soup(url)

    price_element = soup.find("span", "OfferPriceCaption__price")

    # если цены нет, то возвращаем None, т.к. нам нужны только объявления с ценой
    if price_element is None:
        return None

    price = int(price_element.text[:-1].replace("\xa0", ""))
    price_currency = price_element.text[-1]

    engine_str = get_value_from_info_row("engine")
    engine_list = engine_str.split("/") if engine_str else []

    breadcrumbs_elements = soup.find_all("div", class_="CardBreadcrumbs__item")
    breadcrumbs = [item.text.strip() for item in breadcrumbs_elements]

    description_elements = soup.select(".CardDescription__textInner span")
    description = description_elements[0].text if description_elements else None

    equipment_elements = soup.select(".ComplectationGroups__itemContentEl")
    equipment = [item.text for item in equipment_elements]

    specs_url_element = soup.find("a", class_="CardCatalogLink")
    specs_url = specs_url_element["href"] if specs_url_element is not None else None
    model_specs = parse_model_specs(specs_url)

    ad_info = {
        "body_type": get_value_from_info_row("bodytype"),
        "car_url": url,
        "color": get_value_from_info_row("color"),
        "mileage": get_value_from_info_row("kmAge", value_type="int"),
        "production_year": get_value_from_info_row("year", value_type="int"),
        "vehicle_transmission": get_value_from_info_row("transmission"),
        "owners": get_value_from_info_row("ownersCount"),
        "ownership": get_value_from_info_row("owningTime"),
        "vehicle_passport": get_value_from_info_row("pts"),
        "drive": get_value_from_info_row("drive"),
        "steering_wheel": get_value_from_info_row("wheel"),
        "condition": get_value_from_info_row("state"),
        "customs": get_value_from_info_row("customs"),
        "engine_displacement": float(re.findall(r"(\d+\.?\d*)", engine_list[0])[0]) if engine_list else None,
        "engine_power": int("".join(re.findall(r"\d.", engine_list[1]))) if len(engine_list) >= 2 else None,
        "fuel_type": engine_list[2].strip() if len(engine_list) >= 3 else None,
        "brand": breadcrumbs[1],
        "model_name": breadcrumbs[2],
        "name": breadcrumbs[5],
        "description": description,
        "parsing_unixtime": int(datetime.now().timestamp()),
        "sell_id": int(url.split("/")[-2].split("-")[0]),
        "equipment": equipment,
        "number_of_doors": model_specs["number_of_doors"],
        "country_of_brand": model_specs["country_of_brand"],
        "super_gen": {
            "acceleration": model_specs["acceleration"],
            "clearance_min": model_specs["clearance_min"],
            "fuel_rate": model_specs["fuel_rate"]
        },
        "price": price,
        "price_currency": price_currency
    }

    return ad_info


def parse_car_ads(urls):
    """
    Парсит объявления, используя переданные ссылки и возвращает информацию по каждому объявлению.
    В случае ошибки доступа к сайту или проблем с соединением, прекращает парсинг и возвращает уже полученные данные.
    В случае любой другой ошибки, печатает ошибку и адрес сайта, чтобы можно было разобраться, в чем проблема.
    """
    ads = []

    for i, url in enumerate(tqdm(urls)):
        try:
            ad = parse_ad(url)
            if ad is not None:
                ads.append(ad)
        except (WebsiteAccessError, ConnectionError):
            return ads
        except Exception as err:
            print(err)
            print(url)

    return ads


def parallelize(func, elements, n_processes):
    """
    Выполняет функцию, используя несколько процессов.

    @:param func: функция, которую нужно выполнить
    @:param elements: список элементов, которые нужно передать функции
    @:param n_processes: количество процессов
    @:returns результат выполнения функции
    """
    count = len(elements)
    start_indexes = [int(count / n_processes * i) for i in range(n_processes)]
    end_indexes = [int(count / n_processes * (i+1)) for i in range(n_processes)]
    splits = [elements[start_indexes[i]:end_indexes[i]] for i in range(n_processes)]

    overall_results = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = executor.map(func, splits)
        for result in results:
            overall_results.extend(result)

    return overall_results


def manage_parsing_car_ads():
    """Определяет, что нужно парсить и в каком порядке"""
    try:
        with open("car_ads.json") as f:
            ads = json.loads(f.read())
    except FileNotFoundError:
        ads = []

    # определяет ссылки, которые нужно парсить, не включая те, которые уже были использованы
    ad_urls = get_ad_urls()
    ad_urls_parsed = [ad["car_url"] for ad in ads]
    ad_urls_to_parse = list(set(ad_urls) - set(ad_urls_parsed))
    print(f"Осталось спарсить {len(ad_urls_to_parse)} объявлений")

    # сохраняет результаты в файл после каждой 1000 итераций
    while ad_urls_to_parse:
        ad_urls_part = ad_urls_to_parse[:1000]
        new_ads = parallelize(parse_car_ads, ad_urls_part, 4)

        ads.extend(new_ads)

        with open("car_ads.json", "w+") as f2:
            json.dump(ads_to_save, f2, ensure_ascii=False, indent=2)

        ad_urls_to_parse = ad_urls_to_parse[1000:]
        print(f"Осталось спарсить {len(ad_urls_to_parse)} объявлений")


def car_ads_to_csv():
    """Преобразует полученные объявления в csv формат"""
    with open("car_ads.json") as f1:
        car_ads = json.loads(f1.read())

    # в получившемся в результате парсинга файле поле description содержит символы конца строки
    # и при преобразовании файла в csv, программа считает, что начинается новая строка
    # поэтому заменим эти символы на пробелы и заново сохраним файл
    for i in range(len(car_ads)):
        if car_ads[i]["description"] is not None:
            car_ads[i]["description"] = re.sub(r"\r\n|\r|\n", " ", car_ads[i]["description"])

    with open("car_ads.json", "w") as f2:
        json.dump(car_ads, f2, ensure_ascii=False, indent=2)

    df = pd.read_json("car_ads.json")
    df.to_csv("train.csv", index=False)


if __name__ == "__main__":
    manage_parsing_car_ads()
    car_ads_to_csv()
