#!/usr/bin/env python3

import asyncio
import sys
import os
import requests
import pandas as pd
from datetime import datetime, date
import time
import logging
import gzip
import xml.etree.ElementTree as ET
from fake_useragent import UserAgent
import re
from typing import Dict, List, Optional
from dataclasses import dataclass
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
import csv
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta
from category_mapper import CategoryMapper
import socket

# Configure logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/gumroad_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

@dataclass
class Product:
    url: str
    image_url: str = ""
    title: str = ""
    description: str = ""
    price: str = ""
    size: str = ""
    seller_name: str = ""
    seller_url: str = ""
    rating_count: str = ""
    rating_score: str = ""
    sales_count: str = ""
    category_tree: str = ""
    last_updated: str = ""

    def clean_text(self, text: str) -> str:
        """Clean text while preserving full content"""
        if not text:
            return ""
        # Remove extra whitespace and newlines but keep full text
        return " ".join(text.split())

    def to_dict(self) -> Dict[str, str]:
        """Convert to dictionary with cleaned data"""
        return {
            'url': str(self.url),
            'image_url': str(self.image_url),
            'title': self.clean_text(self.title),
            'description': self.clean_text(self.description),
            'price': str(self.price),
            'size': str(self.size),
            'seller_name': str(self.seller_name),
            'seller_url': str(self.seller_url),
            'rating_count': str(self.rating_count),
            'rating_score': str(self.rating_score),
            'sales_count': str(self.sales_count),
            'category_tree': str(self.category_tree),
            'last_updated': str(self.last_updated)
        }

class GumroadScraper:
    def __init__(self):
        self.max_retries = int(os.getenv('MAX_RETRIES', 3))
        self.delay_min = float(os.getenv('DELAY_MIN', 1))
        self.delay_max = float(os.getenv('DELAY_MAX', 3))
        self.max_products = int(os.getenv('MAX_PRODUCTS', 2000000))
        self.batch_size = int(os.getenv('BATCH_SIZE', 10000))
        self.output_dir = 'output'
        self.debug_mode = os.getenv('DEBUG_MODE', 'False').lower() == 'true'
        
        # Add new configuration parameters
        self.connection_timeout = int(os.getenv('CONNECTION_TIMEOUT', 30))
        self.request_timeout = int(os.getenv('REQUEST_TIMEOUT', 30))
        self.retry_backoff = int(os.getenv('RETRY_BACKOFF', 2))
        self.skip_gzip_errors = os.getenv('SKIP_GZIP_ERRORS', 'True').lower() == 'true'
        self.skip_dns_errors = os.getenv('SKIP_DNS_ERRORS', 'True').lower() == 'true'
        
        self.user_agent = UserAgent()
        self.session = requests.Session()
        self.category_mapper = CategoryMapper()
        
        # Get total count immediately upon initialization
        self.total_products = self.count_total_products()
        logger.info(f"Initialized scraper. Total products available: {self.total_products}")
        
        self._initialize_driver()

    def get_sitemap_urls_for_period(self) -> List[str]:
        """Get all sitemap URLs for the specified time period"""
        start_year = int(os.getenv('START_YEAR', 2024))
        start_month = int(os.getenv('START_MONTH', 1))
        end_year = int(os.getenv('END_YEAR', 2025))
        end_month = int(os.getenv('END_MONTH', 2))

        start_date = date(start_year, start_month, 1)
        end_date = date(end_year, end_month, 1)
        
        sitemap_urls = []
        current_date = start_date
        
        while current_date <= end_date:
            base_url = f"https://public-files.gumroad.com/sitemap/products/monthly/{current_date.year}/{current_date.month}/sitemap.xml.gz"
            sitemap_urls.append(base_url)
            current_date += relativedelta(months=1)
            
        return sitemap_urls

    def count_total_products(self) -> int:
        """Count total number of products across all sitemaps"""
        print("Counting total available products...")
        total_products = 0
        
        sitemap_urls = self.get_sitemap_urls_for_period()
        print(f"Found {len(sitemap_urls)} monthly sitemaps to process")
        
        for i, base_sitemap_url in enumerate(sitemap_urls, 1):
            retries = 0
            while retries < self.max_retries:
                try:
                    print(f"Processing sitemap {i} of {len(sitemap_urls)}: {base_sitemap_url}")
                    response = self.session.get(
                        base_sitemap_url, 
                        timeout=(self.connection_timeout, self.request_timeout),
                        allow_redirects=True  # Explicitly allow redirects
                    )
                    
                    # Check response status
                    if response.status_code == 301:
                        logger.info(f"Following redirect for {base_sitemap_url} to {response.headers.get('Location')}")
                        response = self.session.get(
                            response.headers['Location'],
                            timeout=(self.connection_timeout, self.request_timeout)
                        )
                    
                    try:
                        content = gzip.decompress(response.content)
                    except gzip.BadGzipFile:
                        if self.skip_gzip_errors:
                            logger.warning(f"Skipping bad gzip file: {base_sitemap_url}")
                            break
                        raise
                    
                    root = ET.fromstring(content)
                    
                    # Get sub-sitemaps for this month
                    sub_sitemap_urls = []
                    for sitemap in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
                        sub_sitemap_urls.append(sitemap.text)
                    
                    # Count products in each sub-sitemap
                    for sub_url in sub_sitemap_urls:
                        sub_retries = 0
                        while sub_retries < self.max_retries:
                            try:
                                response = self.session.get(
                                    sub_url, 
                                    timeout=(self.connection_timeout, self.request_timeout),
                                    allow_redirects=True
                                )
                                
                                # Handle redirects explicitly
                                if response.status_code == 301:
                                    logger.info(f"Following redirect for {sub_url} to {response.headers.get('Location')}")
                                    response = self.session.get(
                                        response.headers['Location'],
                                        timeout=(self.connection_timeout, self.request_timeout)
                                    )
                                
                                # Check if this is a product page or a sitemap
                                if 'gumroad.com/l/' in sub_url:
                                    if response.status_code == 200:
                                        total_products += 1
                                        print(f"Found product: {sub_url}")
                                    else:
                                        logger.warning(f"Product page returned status {response.status_code}: {sub_url}")
                                    break
                                else:
                                    try:
                                        content = gzip.decompress(response.content)
                                        sub_root = ET.fromstring(content)
                                        product_count = len(sub_root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"))
                                        total_products += product_count
                                        print(f"Found {product_count} products in sitemap {sub_url}")
                                    except gzip.BadGzipFile:
                                        if self.skip_gzip_errors:
                                            logger.warning(f"Not a gzipped file, counting as single product: {sub_url}")
                                            total_products += 1
                                            break
                                        raise
                                break
                                
                            except (requests.exceptions.ConnectionError, socket.gaierror) as e:
                                if self.skip_dns_errors:
                                    logger.warning(f"DNS resolution failed for {sub_url}: {e}")
                                    break
                                sub_retries += 1
                                if sub_retries == self.max_retries:
                                    raise
                                time.sleep(self.retry_backoff ** sub_retries)
                            except Exception as e:
                                logger.error(f"Error processing URL {sub_url}: {e}")
                                break
                    
                    break  # Successfully processed this sitemap, move to next
                    
                except (requests.exceptions.ConnectionError, socket.gaierror) as e:
                    if self.skip_dns_errors:
                        logger.warning(f"DNS resolution failed for {base_sitemap_url}: {e}")
                        break
                    retries += 1
                    if retries == self.max_retries:
                        raise
                    time.sleep(self.retry_backoff ** retries)
                except Exception as e:
                    logger.error(f"Error processing monthly sitemap {base_sitemap_url}: {e}")
                    break
            
        print(f"\nTotal products found across all periods: {total_products}")
        return total_products

    def _initialize_driver(self):
        """Initialize the Chrome driver with improved options"""
        try:
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-infobars")
            chrome_options.add_argument("--disable-notifications")
            chrome_options.add_argument("--disable-popup-blocking")
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--disable-features=IsolateOrigins,site-per-process")
            chrome_options.add_argument(f"user-agent={self.user_agent.random}")
            
            # Add performance options
            chrome_options.add_argument("--disable-javascript")  # Disable JS if not needed
            chrome_options.add_argument("--blink-settings=imagesEnabled=false")  # Disable images
            chrome_options.add_argument("--disk-cache-size=1")  # Minimize disk cache
            
            self.driver = webdriver.Chrome(
                service=ChromeService(ChromeDriverManager().install()),
                options=chrome_options
            )
            self.driver.set_page_load_timeout(self.connection_timeout)
            logger.info("Chrome driver initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Chrome driver: {e}")
            raise

    def get_sitemap_urls(self) -> List[str]:
        """Extract product URLs and categories from Gumroad sitemaps"""
        try:
            base_sitemap_url = "https://public-files.gumroad.com/sitemap/products/monthly/2025/1/sitemap.xml.gz"
            response = self.session.get(base_sitemap_url)
            
            content = gzip.decompress(response.content)
            root = ET.fromstring(content)
            
            sub_sitemap_urls = []
            for sitemap in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
                sub_sitemap_urls.append(sitemap.text)
            
            logger.info(f"Found {len(sub_sitemap_urls)} sub-sitemaps")
            
            if sub_sitemap_urls:
                response = self.session.get(sub_sitemap_urls[0])
                content = gzip.decompress(response.content)
                sub_root = ET.fromstring(content)
                
                product_urls = []
                for url in sub_root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
                    product_urls.append(url.text)
                    if len(product_urls) >= 100:
                        break
                
                logger.info(f"Extracted {len(product_urls)} product URLs")
                return product_urls
            
            return []
            
        except Exception as e:
            logger.error(f"Error fetching sitemap: {e}")
            return []

    def scrape_product(self, url: str) -> Product:
        """Scrape individual product page with improved timeout handling"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Set shorter page load timeout
                self.driver.set_page_load_timeout(20)
                self.driver.get(url)
                
                # Wait for product page with explicit timeout
                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="product_page"]'))
                    )
                except Exception as wait_error:
                    logger.warning(f"Timeout waiting for product page: {wait_error}")
                    retry_count += 1
                    time.sleep(self.retry_backoff ** retry_count)
                    continue
                
                product = Product(url=url)
                
                # Extract product details with individual try-except blocks
                try:
                    # Get product image with shorter timeout
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'img.preview[itemprop="image"]'))
                    )
                    img_element = self.driver.find_element(
                        By.CSS_SELECTOR, 'img.preview[itemprop="image"]'
                    )
                    product.image_url = img_element.get_attribute('src')
                except Exception as e:
                    logger.debug(f"No image found: {e}")
                
                try:
                    # Get title
                    title_xpath = '//main/section[2]/article/section[1]/header/h1'
                    product.title = self.driver.find_element(By.XPATH, title_xpath).text
                except Exception as e:
                    logger.warning(f"Could not find title for {url}: {e}")
                    
                try:
                    # Get description
                    desc_xpath = '//main/section[2]/article/section[1]/section[2]/div'
                    product.description = self.driver.find_element(By.XPATH, desc_xpath).text
                except Exception as e:
                    logger.warning(f"Could not find description for {url}: {e}")
                    
                try:
                    # Get price
                    price_xpath = '//main/section[2]/article/section[1]/section[1]/div[1]/div[1]/div[1]'
                    price_element = self.driver.find_element(By.XPATH, price_xpath)
                    product.price = price_element.text.strip()
                except Exception as e:
                    logger.warning(f"Could not find price for {url}: {e}")
                    
                try:
                    # Get product size
                    size_xpath = '//main/section[2]/article/section[2]/section/div[1]/div[1]/div'
                    size_element = self.driver.find_element(By.XPATH, size_xpath)
                    product.size = size_element.text.strip()
                except Exception as e:
                    logger.warning(f"Could not find size for {url}: {e}")
                    
                try:
                    # Get seller name
                    seller_xpath = '//main/section[2]/article/section[1]/section[1]/div[2]/a'
                    seller_element = self.driver.find_element(By.XPATH, seller_xpath)
                    product.seller_name = seller_element.text
                    product.seller_url = seller_element.get_attribute('href')
                except Exception as e:
                    logger.warning(f"Could not find seller details for {url}: {e}")

                # New fields extraction
                try:
                    rating_count_xpath = '//main/section[2]/article/section[1]/section[1]/div[3]/span[6]'
                    rating_count_element = self.driver.find_element(By.XPATH, rating_count_xpath)
                    rating_count_text = rating_count_element.text.strip()
                    # Extract number from text (e.g., "123 ratings" -> 123)
                    product.rating_count = int(''.join(filter(str.isdigit, rating_count_text)))
                except Exception as e:
                    logger.warning(f"Could not find rating count for {url}: {e}")

                try:
                    rating_score_xpath = '//main/section[2]/article/section[2]/section[2]/header/div/div'
                    rating_score_element = self.driver.find_element(By.XPATH, rating_score_xpath)
                    rating_score_text = rating_score_element.text.strip()
                    # Convert text to float (e.g., "4.5" -> 4.5)
                    product.rating_score = float(rating_score_text)
                except Exception as e:
                    logger.warning(f"Could not find rating score for {url}: {e}")

                try:
                    # Updated XPath for sales count
                    sales_xpath = "//span/strong[contains(following-sibling::text(), 'sale')]"
                    sales_element = self.driver.find_element(By.XPATH, sales_xpath)
                    sales_text = sales_element.text.strip()
                    # Remove commas from numbers like "4,472"
                    product.sales_count = sales_text.replace(',', '')
                except Exception as e:
                    logger.debug(f"Could not find sales count for {url}: {e}")
                    product.sales_count = ""
                
                product.last_updated = datetime.now().isoformat()
                
                # After extracting title and description, detect category
                product.category_tree = self.category_mapper.detect_category(
                    title=product.title,
                    description=product.description
                )
                
                # Add a randomized delay between requests
                time.sleep(random.uniform(self.delay_min, self.delay_max))
                
                return product
                
            except Exception as e:
                retry_count += 1
                logger.warning(f"Attempt {retry_count} failed for {url}: {str(e)}")
                
                if retry_count >= max_retries:
                    logger.error(f"Failed to scrape product {url} after {max_retries} attempts")
                    return Product(url=url)  # Return empty product after all retries fail
                
                # Exponential backoff between retries
                time.sleep(self.retry_backoff ** retry_count)
                
                # Reset Chrome driver if we get timeout errors
                if "timeout" in str(e).lower():
                    try:
                        self.driver.quit()
                        self._initialize_driver()
                        logger.info("Reset Chrome driver due to timeout")
                    except Exception as driver_error:
                        logger.error(f"Error resetting driver: {driver_error}")

    def scrape_all_products(self) -> pd.DataFrame:
        """Scrape products from sitemaps"""
        # Create output directory immediately
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(f"Created/verified output directory at {self.output_dir}")
        
        all_products = []
        processed_count = 0
        
        try:
            sitemap_urls = self.get_sitemap_urls_for_period()
            
            for base_sitemap_url in sitemap_urls:
                try:
                    response = self.session.get(base_sitemap_url)
                    content = gzip.decompress(response.content)
                    root = ET.fromstring(content)
                    
                    sub_sitemap_urls = []
                    for sitemap in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
                        sub_sitemap_urls.append(sitemap.text)
                    
                    for sub_url in sub_sitemap_urls:
                        try:
                            response = self.session.get(sub_url)
                            content = gzip.decompress(response.content)
                            sub_root = ET.fromstring(content)
                            
                            product_urls = []
                            for url in sub_root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
                                product_urls.append(url.text)
                            
                            # Process in batches
                            for i in range(0, len(product_urls), self.batch_size):
                                batch_urls = product_urls[i:i + self.batch_size]
                                
                                for url in batch_urls:
                                    if processed_count >= self.max_products:
                                        logger.info(f"Reached maximum product limit of {self.max_products}")
                                        # Save any remaining products before returning
                                        if all_products:
                                            self._save_batch(pd.DataFrame(all_products))
                                        return pd.DataFrame(all_products)
                                    
                                    product = self.scrape_product(url)
                                    all_products.append(product.to_dict())
                                    processed_count += 1
                                    
                                    # Log progress every 100 products but don't save
                                    if processed_count % 100 == 0:
                                        logger.info(f"Processed {processed_count} products")
                                    
                                    # Only save when we reach the batch size
                                    if len(all_products) >= self.batch_size:
                                        logger.info(f"Saving batch of {len(all_products)} products")
                                        self._save_batch(pd.DataFrame(all_products))
                                        all_products = []  # Clear the list after saving
                                
                        except Exception as e:
                            logger.error(f"Error processing sub-sitemap {sub_url}: {e}")
                            # Save current progress if we have products
                            if all_products:
                                self._save_interrupted_batch(pd.DataFrame(all_products))
                            continue
                        
                except Exception as e:
                    logger.error(f"Error processing monthly sitemap {base_sitemap_url}: {e}")
                    # Save current progress if we have products
                    if all_products:
                        self._save_interrupted_batch(pd.DataFrame(all_products))
                    continue
            
            # Save any remaining products
            if all_products:
                self._save_batch(pd.DataFrame(all_products))
            
            return pd.DataFrame(all_products)
            
        except Exception as e:
            logger.error(f"Unexpected error during scraping: {e}")
            # Save current progress if we have products
            if all_products:
                self._save_interrupted_batch(pd.DataFrame(all_products))
            raise

    def _save_batch(self, df: pd.DataFrame):
        """Save intermediate results"""
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{self.output_dir}/gumroad_products_batch_{timestamp}.csv"
            
            df.to_csv(
                filename,
                index=False,
                encoding='utf-8-sig',
                escapechar='\\',
                quoting=csv.QUOTE_ALL
            )
            
            logger.info(f"Saved batch to {filename}")
            
        except Exception as e:
            logger.error(f"Error saving batch: {e}")

    def _save_interrupted_batch(self, df: pd.DataFrame):
        """Save products when scraping is interrupted"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{self.output_dir}/gumroad_products_interrupted_{timestamp}.csv"
            
            df.to_csv(
                filename,
                index=False,
                encoding='utf-8-sig',
                escapechar='\\',
                quoting=csv.QUOTE_ALL
            )
            
            logger.info(f"Saved interrupted batch of {len(df)} products to {filename}")
            
        except Exception as e:
            logger.error(f"Error saving interrupted batch: {e}")

    def cleanup(self):
        """Clean up resources"""
        try:
            if hasattr(self, 'driver'):
                self.driver.quit()
                logger.info("Browser closed successfully")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

async def main():
    print("\nStarting Gumroad Scraper...")
    print("=" * 50)

    scraper = None
    try:
        scraper = GumroadScraper()
        
        print("\nBeginning product scraping...")
        print(f"Will scrape products out of {scraper.total_products} total available")
        print("=" * 50)
        
        df = scraper.scrape_all_products()

        print("\nScraping Complete!")
        print("=" * 50)
        print(f"\nTotal products scraped: {len(df)}")
        print("\nSample of products:")
        print(df[['title', 'price', 'sales_count']].head())
        print("\nResults have been saved to the 'output' directory.")

    except KeyboardInterrupt:
        print("\nScraping cancelled by user.")
        logger.warning("Scraping interrupted by user - saving current progress")
        if scraper and hasattr(scraper, '_save_interrupted_batch'):
            # Try to save any unsaved data
            try:
                current_data = pd.DataFrame(scraper.all_products) if hasattr(scraper, 'all_products') else None
                if current_data is not None and not current_data.empty:
                    scraper._save_interrupted_batch(current_data)
            except Exception as e:
                logger.error(f"Could not save interrupted data: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in main: {str(e)}")
        if scraper and hasattr(scraper, '_save_interrupted_batch'):
            # Try to save any unsaved data
            try:
                current_data = pd.DataFrame(scraper.all_products) if hasattr(scraper, 'all_products') else None
                if current_data is not None and not current_data.empty:
                    scraper._save_interrupted_batch(current_data)
            except Exception as save_error:
                logger.error(f"Could not save interrupted data: {save_error}")
        raise
    finally:
        if scraper:
            scraper.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nScraping cancelled by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)