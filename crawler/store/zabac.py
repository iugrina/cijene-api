import datetime
import logging
from collections import defaultdict

from bs4 import BeautifulSoup
from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class ZabacCrawler(BaseCrawler):
    """Crawler for Žabac store prices."""

    CHAIN = "zabac"
    BASE_URL = "https://zabacfoodoutlet.hr/cjenik/"

    # Mapping for price fields from CSV columns
    PRICE_MAP = {
        # field: (column_name, is_required)
        "price": ("MPC", False),
        "unit_price": ("MPC", False),  # Use same as price
        "best_price_30": ("Najniža cijena u posljednjih 30 dana", False),
        "anchor_price": ("Sidrena cijena na 2.5.2025", False),
    }

    # Mapping for other product fields from CSV columns
    FIELD_MAP = {
        "product_id": ("Šifra artikla", True),
        "barcode": ("Barcode", False),
        "product": ("Naziv artikla", True),
        "brand": ("Marka", False),
        "quantity": ("Gramaža", False),
        "category": ("Naziv grupe artikala", False),
    }

    # Location pages on the Žabac website, mapped to store metadata.
    # The website uses a ?lokacija= query parameter to switch between stores.
    LOCATIONS = {
        "dubrava-256l": {
            "store_id": "PJ-7",
            "name": "Žabac PJ-7",
            "store_type": "Supermarket",
            "street_address": "Dubrava 256L",
            "city": "Zagreb",
            "zipcode": "10000",
        },
        "velika-gorica": {
            "store_id": "PJ-VG",
            "name": "Žabac PJ-VG",
            "store_type": "Supermarket",
            "street_address": "Trg Grada Vukovara 8",
            "city": "Velika Gorica",
            "zipcode": "10410",
        },
    }

    def parse_index(self, content: str) -> list[str]:
        """
        Parse the Žabac index page to extract CSV links.

        Args:
            content: HTML content of the index page

        Returns:
            List of absolute CSV URLs on the page
        """
        soup = BeautifulSoup(content, "html.parser")
        urls = []

        for link_tag in soup.select('a[href$=".csv"]'):
            href = str(link_tag.get("href"))
            urls.append(href)

        return list(set(urls))  # Return unique URLs

    def get_store_prices(self, csv_url: str) -> list[Product]:
        """
        Fetch and parse store prices from a Žabac CSV URL.

        Args:
            csv_url: URL to the CSV file containing prices

        Returns:
            List of Product objects
        """
        try:
            content = self.fetch_text(csv_url)
            return self.parse_csv(content)
        except Exception as e:
            logger.error(
                f"Failed to get Žabac store prices from {csv_url}: {e}",
                exc_info=True,
            )
            return []

    def get_index(self, date: datetime.date) -> list[tuple[str, str]]:
        """
        Fetch and parse all Žabac location pages to get CSV URLs for given date.

        Args:
            date: The date parameter

        Returns:
            List of (csv_url, location_key) tuples for the given date.
        """
        results = []
        url_date = f"{date.day}.{date.month}.{date.year}"
        url_date_padded = f"{date.day:02d}.{date.month:02d}.{date.year}"

        for location_key in self.LOCATIONS:
            page_url = f"{self.BASE_URL}?lokacija={location_key}"
            content = self.fetch_text(page_url)
            if not content:
                logger.warning(f"No content at {page_url}")
                continue
            for url in self.parse_index(content):
                if url_date in url or url_date_padded in url:
                    results.append((url, location_key))

        return results

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all Žabac store, product, and price info.

        Args:
            date: The date parameter

        Returns:
            List of Store objects with their products.
        """
        csv_links = self.get_index(date)

        if not csv_links:
            logger.warning("No Žabac CSV links found")
            return []

        # Group URLs by location to create one Store per location
        location_urls: dict[str, list[str]] = defaultdict(list)
        for url, location_key in csv_links:
            location_urls[location_key].append(url)

        stores = []
        for location_key, urls in location_urls.items():
            loc = self.LOCATIONS[location_key]
            store = Store(
                chain=self.CHAIN,
                store_type=loc["store_type"],
                store_id=loc["store_id"],
                name=loc["name"],
                street_address=loc["street_address"],
                zipcode=loc["zipcode"],
                city=loc["city"],
                items=[],
            )

            for url in urls:
                products = self.get_store_prices(url)
                store.items.extend(products)

            if not store.items:
                logger.warning(f"No products for {store.name}, skipping")
                continue

            stores.append(store)

        return stores

    def fix_product_data(self, data: dict) -> dict:
        """
        Clean and fix Žabac-specific product data.

        Args:
            data: Dictionary containing the row data

        Returns:
            The cleaned data
        """
        if "product" in data and data["product"]:
            data["product"] = data["product"].strip()

        # Unit is not available in the CSV
        data["unit"] = ""

        return super().fix_product_data(data)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = ZabacCrawler()
    stores = crawler.crawl(datetime.date.today())
    for store in stores:
        print(store)
        print(store.items[0])
