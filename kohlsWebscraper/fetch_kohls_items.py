import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import concurrent.futures

# Departments list
departments = [
    {'name': 'accessories', 'url': 'https://www.kohls.com/catalog/clearance-accessories.jsp?CN=Promotions:Clearance+Department:Accessories&kls_sbp=90595572255358814293312850185484413129&pfm=browse%20refine&PPP=48&S=7&sks=true'},
    {'name': 'beauty', 'url': 'https://www.kohls.com/catalog/beauty.jsp?CN=Department:Beauty&pfm=browse%20refine&kls_sbp=05834574949241830242529155580587980870&PPP=48&S=7&sks=true'},
    {'name': 'bed-bath', 'url': 'https://www.kohls.com/catalog/bed-bath.jsp?CN=Department:Bed%20%26%20Bath&pfm=browse%20refine&kls_sbp=05834574949241830242529155580587980870&PPP=48&S=7&sks=true'},
    {'name': 'electronics', 'url': 'https://www.kohls.com/catalog/electronics.jsp?CN=Department:Electronics&pfm=browse&kls_sbp=05834574949241830242529155580587980870&PPP=48&S=7&sks=true'},
    {'name': 'health', 'url': 'https://www.kohls.com/catalog/clearance-health-personal-care.jsp?CN=Promotions:Clearance+Department:Health%20%26%20Personal%20Care&pfm=browse%20refine&kls_sbp=90595572255358814293312850185484413129&PPP=48&S=7&sks=true'},
    {'name': 'home', 'url': 'https://www.kohls.com/catalog/clearance-home-decor.jsp?CN=Promotions:Clearance+Department:Home%20Decor&kls_sbp=90595572255358814293312850185484413129&pfm=browse%20refine&PPP=48&S=7&sks=true'},
    {'name': 'kitchen', 'url': 'https://www.kohls.com/catalog/kitchen-dining.jsp?CN=Department:Kitchen%20%26%20Dining&CC=for_thehome-LN1.0-S-kitchendining&kls_sbp=05834574949241830242529155580587980870&PPP=48&S=7&sks=true'},
    {'name': 'sephora', 'url': 'https://www.kohls.com/catalog/sephora.jsp?CN=Partnership:Sephora&cc=Sephora-LN1.0-S-shopallsephorabeauty&kls_sbp=05834574949241830242529155580587980870&PPP=48&S=7&sks=true'},
    {'name': 'sports', 'url': 'https://www.kohls.com/catalog/sports-fitness.jsp?CN=Department:Sports%20%26%20Fitness&kls_sbp=05834574949241830242529155580587980870&pfm=browse%20refine&PPP=48&S=7&sks=true'},
    {'name': 'shoes', 'url': 'https://www.kohls.com/catalog/shoes.jsp?CN=Department:Shoes&kls_sbp=05834574949241830242529155580587980870&pfm=browse%20refine&PPP=48&S=7&sks=true'},
    {'name': 'toys', 'url': 'https://www.kohls.com/catalog/toys.jsp?CN=Department:Toys&pfm=browse%20refine&kls_sbp=05834574949241830242529155580587980870&PPP=48&S=7&sks=true'},
]

class KohlsScraper:
    def __init__(self, departments, max_pages=20):
        self.departments = departments
        self.max_pages = max_pages
        self.product_data = []

    def fetch_page(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.content
        except requests.exceptions.HTTPError as err:
            print(f"HTTP error: {err}")
        except Exception as err:
            print(f"Error: {err}")
        return None

    def parse_product(self, product):
        try:
            title = product.find('div', {'class': 'prod_nameBlock'}).text.strip()
            prod_price = product.find('div', {'class': 'prod_priceBlock'}).text.strip()
            prod_link = product.find('div', {'class': 'prod_nameBlock'}).find('p').get('rel')
            return {
                'title': title,
                'price': prod_price,
                'link': prod_link
            }
        except Exception as e:
            print(f"Error parsing product: {e}")
        return None

    def parse_page(self, content):
        soup = BeautifulSoup(content, 'html.parser')
        products = soup.find_all('li', {'class': 'products_grid'})
        return [self.parse_product(product) for product in products if self.parse_product(product) is not None]

    def scrape_department(self, department):
        department_data = []
        for page in range(1, self.max_pages + 1):
            if page == 1:
                url = department['url']
            else:
                url = f"{department['url']}&WS={48 * (page - 1)}"

            content = self.fetch_page(url)
            if not content:
                break

            products = self.parse_page(content)
            if not products:
                break

            department_data.extend(products)
            print(f"Scraped {len(products)} products from {department['name']} page {page}")

            time.sleep(1)  # Respectful scraping
        return department_data

    def scrape(self):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(self.scrape_department, dept): dept for dept in self.departments}
            for future in concurrent.futures.as_completed(futures):
                dept = futures[future]
                try:
                    data = future.result()
                    self.product_data.extend(data)
                except Exception as e:
                    print(f"Error scraping {dept['name']}: {e}")

    def save_to_csv(self, filename):
        df = pd.DataFrame(self.product_data)
        df.to_csv(filename, index=False)

if __name__ == "__main__":
    scraper = KohlsScraper(departments)
    scraper.scrape()
    scraper.save_to_csv("kohls_products.csv")