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
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
import random
import csv
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta
from category_mapper import CategoryMapper
import socket
import aiohttp
import concurrent.futures
from bs4 import BeautifulSoup
import queue
from functools import partial

# Configure logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
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
        self.delay_min = float(os.getenv('DELAY_MIN', 0.5))
        self.delay_max = float(os.getenv('DELAY_MAX', 1.5))
        self.max_products = int(os.getenv('MAX_PRODUCTS'))
        self.batch_size = int(os.getenv('BATCH_SIZE'))
        self.output_dir = 'output'
        self.debug_mode = os.getenv('DEBUG_MODE', 'False').lower() == 'true'
        
        # Network configuration
        self.connection_timeout = int(os.getenv('CONNECTION_TIMEOUT', 20))
        self.request_timeout = int(os.getenv('REQUEST_TIMEOUT', 20))
        self.retry_backoff = int(os.getenv('RETRY_BACKOFF', 2))
        self.skip_gzip_errors = os.getenv('SKIP_GZIP_ERRORS', 'True').lower() == 'true'
        self.skip_dns_errors = os.getenv('SKIP_DNS_ERRORS', 'True').lower() == 'true'
        
        # Concurrency settings
        self.max_workers = int(os.getenv('MAX_WORKERS', 50))
        self.max_connections = int(os.getenv('MAX_CONNECTIONS', 50))
        
        self.user_agent = UserAgent()
        self.session = requests.Session()
        self.headers = {'User-Agent': self.user_agent.random}
        self.category_mapper = CategoryMapper()
        
        # Create a cache for processed URLs to avoid duplicates
        self.processed_urls = set()
        
        # Create a thread-safe queue for results
        self.results_queue = queue.Queue()
        
        # Create output directory immediately
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Get sitemap URLs once
        self.sitemap_urls = self.get_sitemap_urls_for_period()
        logger.info(f"Found {len(self.sitemap_urls)} monthly sitemaps to process")

    def get_sitemap_urls_for_period(self) -> List[str]:
        """Get all sitemap URLs for the specified time period"""
        start_year = int(os.getenv('START_YEAR'))
        start_month = int(os.getenv('START_MONTH'))
        end_year = int(os.getenv('END_YEAR'))
        end_month = int(os.getenv('END_MONTH'))

        start_date = date(start_year, start_month, 1)
        end_date = date(end_year, end_month, 1)
        
        sitemap_urls = []
        current_date = start_date
        
        while current_date <= end_date:
            base_url = f"https://public-files.gumroad.com/sitemap/products/monthly/{current_date.year}/{current_date.month}/sitemap.xml.gz"
            sitemap_urls.append(base_url)
            current_date += relativedelta(months=1)
            
        return sitemap_urls

    async def fetch_url(self, session, url):
        """Fetch URL with retry logic using aiohttp"""
        retries = 0
        while retries < self.max_retries:
            try:
                headers = {
                    'User-Agent': self.user_agent.random,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Cache-Control': 'max-age=0',
                }
                
                async with session.get(url, headers=headers, timeout=self.request_timeout) as response:
                    if response.status == 200:
                        return await response.read()  # Return raw binary content
                    elif response.status == 429:
                        retry_after = int(response.headers.get('Retry-After', self.retry_backoff ** (retries + 2)))
                        logger.warning(f"Rate limited on {url}, waiting for {retry_after} seconds")
                        await asyncio.sleep(retry_after)
                    else:
                        logger.warning(f"Failed to fetch {url}, status: {response.status}")
                
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                logger.warning(f"Error fetching {url}: {e}")
            
            retries += 1
            if retries < self.max_retries:
                wait_time = self.retry_backoff ** (retries + 1)
                logger.info(f"Retrying {url} in {wait_time} seconds (attempt {retries+1}/{self.max_retries})")
                await asyncio.sleep(wait_time)
        
        return None
    
    async def process_sitemap(self, session, sitemap_url):
        """Process a sitemap and return product URLs"""
        try:
            # Fetch the raw binary content
            content = await self.fetch_url(session, sitemap_url)
            if not content:
                return []
            
            try:
                # Decompress the gzip content
                decompressed = gzip.decompress(content)
                # Parse the XML from the decompressed content
                root = ET.fromstring(decompressed)
                
                # If this is a sitemap index (contains other sitemaps)
                if root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap"):
                    sub_sitemap_tasks = []
                    for sitemap in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
                        sub_url = sitemap.text
                        sub_sitemap_tasks.append(self.process_sitemap(session, sub_url))
                    
                    results = await asyncio.gather(*sub_sitemap_tasks)
                    # Flatten the list of lists
                    return [url for sublist in results for url in sublist]
                
                # If this is a product sitemap (contains product URLs)
                product_urls = []
                for url_element in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
                    url = url_element.text
                    if 'gumroad.com/l/' in url and url not in self.processed_urls:
                        product_urls.append(url)
                        if len(product_urls) >= self.max_products:
                            break
                
                return product_urls
                
            except gzip.BadGzipFile:
                if self.skip_gzip_errors:
                    logger.warning(f"Skipping bad gzip file: {sitemap_url}")
                    return []
                raise
            
        except Exception as e:
            logger.error(f"Error processing sitemap {sitemap_url}: {e}")
            return []

    async def process_all_sitemaps(self, session):
        """Process all sitemaps in parallel"""
        sitemap_tasks = [self.process_sitemap(session, sitemap_url) for sitemap_url in self.sitemap_urls]
        results = await asyncio.gather(*sitemap_tasks)
        return [url for sublist in results for url in sublist]

    def scrape_product_html(self, html, url):
        """Parse product details from HTML using BeautifulSoup"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            product = Product(url=url)
            
            # Extract product details
            try:
                # Extract title
                title_element = soup.select_one('main section article section header h1')
                if title_element:
                    product.title = title_element.text.strip()
            except Exception as e:
                logger.warning(f"Could not find title for {url}: {e}")
                
            try:
                # Extract price
                price_element = soup.select_one('main section article section section div div div')
                if price_element:
                    product.price = price_element.text.strip()
            except Exception as e:
                logger.warning(f"Could not find price for {url}: {e}")
                
            try:
                # Extract size
                size_headers = soup.find_all('h5')
                for header in size_headers:
                    if header.text.strip() == 'Size':
                        size_element = header.find_next('div')
                        if size_element:
                            product.size = size_element.text.strip()
                            break
            except Exception as e:
                logger.warning(f"Could not find size for {url}: {e}")
                
            try:
                # Extract seller details
                seller_element = soup.select_one('main section article section section div a')
                if seller_element:
                    product.seller_name = seller_element.text.strip()
                    product.seller_url = seller_element.get('href', '')
            except Exception as e:
                logger.warning(f"Could not find seller details for {url}: {e}")

            # Extract sales count
            try:
                # Find sales count in info div
                info_div = soup.select_one('div.info')
                if info_div:
                    strong_element = info_div.select_one('span strong')
                    if strong_element:
                        sales_text = strong_element.text.strip()
                        product.sales_count = sales_text.replace(',', '')
            except Exception as e:
                logger.debug(f"Could not find sales count for {url}: {e}")

            # Extract rating count
            try:
                rating_count_element = soup.select_one('main section article section section div span:nth-of-type(6)')
                if rating_count_element:
                    rating_count_text = rating_count_element.text.strip()
                    # Extract number from text (e.g., "123 ratings" -> 123)
                    product.rating_count = int(''.join(filter(str.isdigit, rating_count_text)))
            except Exception as e:
                logger.warning(f"Could not find rating count for {url}: {e}")

            # Extract rating score
            try:
                rating_score_element = soup.select_one('main section article section section header div div')
                if rating_score_element:
                    rating_score_text = rating_score_element.text.strip()
                    # Convert text to float (e.g., "4.5" -> 4.5)
                    product.rating_score = float(rating_score_text)
            except Exception as e:
                logger.warning(f"Could not find rating score for {url}: {e}")

            # Extract image URL
            try:
                image_element = soup.select_one('link[rel="preload"][as="image"]')
                if image_element:
                    product.image_url = image_element.get('href', '')
            except Exception as e:
                logger.warning(f"Could not find image URL for {url}: {e}")

            # Extract description
            try:
                description_element = soup.select_one('meta[name="description"]')
                if description_element:
                    product.description = description_element.get('content', '')
            except Exception as e:
                logger.warning(f"Could not find description for {url}: {e}")

            # Category detection
            product.category_tree = self.category_mapper.detect_category(
                title=product.title,
                description=product.description
            )
            
            product.last_updated = datetime.now().isoformat()
            return product
            
        except Exception as e:
            logger.error(f"Error parsing product HTML for {url}: {e}")
            return Product(url=url)

    async def fetch_product(self, session, url):
        """Fetch and process a product page using aiohttp and BeautifulSoup"""
        try:
            # Add random delay to avoid rate limiting
            await asyncio.sleep(random.uniform(self.delay_min, self.delay_max))
            
            # Fetch the page content
            content = await self.fetch_url(session, url)
            if not content:
                return Product(url=url)
            
            # Use ThreadPoolExecutor for CPU-bound HTML parsing
            with concurrent.futures.ThreadPoolExecutor() as executor:
                loop = asyncio.get_event_loop()
                product = await loop.run_in_executor(
                    executor, 
                    partial(self.scrape_product_html, content, url)
                )

            self.processed_urls.add(url)
            self.results_queue.put(product.to_dict())

            return product

        except Exception as e:
            logger.error(f"Error fetching product {url}: {e}")
            return Product(url=url)

    async def process_batch(self, product_urls):
        """Process a batch of product URLs concurrently"""
        try:
            import aiodns
            resolver = aiohttp.AsyncResolver(nameservers=["8.8.8.8", "1.1.1.1"])  # Use Google DNS and Cloudflare DNS
        except ImportError:
            logger.warning("aiodns library not found. Falling back to default resolver.")
            resolver = None

        connector = aiohttp.TCPConnector(
            limit=self.max_connections,
            resolver=resolver  # Use custom resolver if available, otherwise use default
        )
        timeout = aiohttp.ClientTimeout(total=self.request_timeout)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = []
            for url in product_urls:
                if url not in self.processed_urls:
                    tasks.append(self.fetch_product(session, url))

                # Limit concurrent tasks
                if len(tasks) >= self.max_workers:
                    await asyncio.gather(*tasks)
                    tasks = []

            # Process any remaining tasks
            if tasks:
                await asyncio.gather(*tasks)

    def save_results_from_queue(self):
        """Save results from the queue to CSV"""
        products = []
        while not self.results_queue.empty():
            try:
                products.append(self.results_queue.get_nowait())
            except queue.Empty:
                break

        if products:
            df = pd.DataFrame(products)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{self.output_dir}/gumroad_products_batch_{timestamp}.csv"

            df.to_csv(
                filename,
                index=False,
                encoding='utf-8-sig',
                escapechar='\\',
                quoting=csv.QUOTE_ALL
            )

            logger.info(f"Saved batch of {len(products)} products to {filename}")
            return len(products)

        return 0

    async def scrape_all_products(self):
        """Main method to scrape all products using async approach"""
        total_processed = 0
        total_saved = 0

        try:
            # Setup aiohttp session for sitemaps
            connector = aiohttp.TCPConnector(
                limit=self.max_connections,
                resolver=aiohttp.AsyncResolver(nameservers=["8.8.8.8", "1.1.1.1"])  # Use Google DNS and Cloudflare DNS
            )
            timeout = aiohttp.ClientTimeout(total=self.request_timeout)
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                # Process all sitemaps in parallel to get product URLs
                all_product_urls = await self.process_all_sitemaps(session)
                logger.info(f"Found {len(all_product_urls)} unique product URLs to process")

                # Process product URLs in batches
                for i in range(0, len(all_product_urls), self.batch_size):
                    batch = all_product_urls[i:i + self.batch_size]
                    logger.info(f"Processing batch of {len(batch)} products ({i+1}-{i+len(batch)} of {len(all_product_urls)})")
                    
                    await self.process_batch(batch)
                    
                    # Save results periodically
                    saved = self.save_results_from_queue()
                    total_saved += saved
                    total_processed += len(batch)
                    
                    logger.info(f"Progress: {total_processed}/{len(all_product_urls)} processed, {total_saved} saved")
                    
                    if total_processed >= self.max_products:
                        logger.info(f"Reached maximum product limit of {self.max_products}")
                        break
            
            # Save any remaining results
            final_saved = self.save_results_from_queue()
            total_saved += final_saved
            
            logger.info(f"Scraping complete. Total processed: {total_processed}, Total saved: {total_saved}")
            
            # Combine all CSV files into one final file
            self.combine_csv_files()
            
            return total_processed
            
        except Exception as e:
            logger.error(f"Error in scrape_all_products: {e}")
            # Save any results in queue before exiting
            self.save_results_from_queue()
            raise

    def combine_csv_files(self):
        """Combine all CSV files in output directory into one final file"""
        try:
            csv_files = [f for f in os.listdir(self.output_dir) if f.endswith('.csv')]
            
            if not csv_files:
                logger.warning("No CSV files found to combine")
                return
            
            # Read and combine all CSV files
            dfs = []
            for file in csv_files:
                file_path = os.path.join(self.output_dir, file)
                try:
                    df = pd.read_csv(file_path, encoding='utf-8-sig')
                    dfs.append(df)
                except Exception as e:
                    logger.error(f"Error reading CSV file {file_path}: {e}")
            
            if not dfs:
                logger.warning("No valid CSV data found to combine")
                return
            
            # Combine all dataframes
            combined_df = pd.concat(dfs, ignore_index=True)
            
            # Remove duplicates
            combined_df.drop_duplicates(subset=['url'], inplace=True)
            
            # Save combined file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            combined_file = f"{self.output_dir}/gumroad_products_combined_{timestamp}.csv"
            
            combined_df.to_csv(
                combined_file,
                index=False,
                encoding='utf-8-sig',
                escapechar='\\',
                quoting=csv.QUOTE_ALL
            )
            
            logger.info(f"Combined {len(csv_files)} CSV files into {combined_file} with {len(combined_df)} unique products")
            
        except Exception as e:
            logger.error(f"Error combining CSV files: {e}")

    def count_total_products(self) -> int:
        """Count total number of products across all sitemaps - simplified version that runs faster"""
        total_count = 0
        sitemap_count = 0
        
        for base_sitemap_url in self.sitemap_urls:
            sitemap_count += 1
            if sitemap_count > 1 and total_count > 0:
                # Extrapolate based on first sitemap to save time
                return total_count * len(self.sitemap_urls)
            
            try:
                response = self.session.get(
                    base_sitemap_url, 
                    timeout=(self.connection_timeout, self.request_timeout),
                    allow_redirects=True,
                    headers=self.headers
                )
                
                try:
                    content = gzip.decompress(response.content)
                    root = ET.fromstring(content)
                    
                    # Get sub-sitemaps for this month
                    sub_sitemap_urls = []
                    for sitemap in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
                        sub_sitemap_urls.append(sitemap.text)
                    
                    # Only process a sample of sub-sitemaps for speed
                    sample_size = min(3, len(sub_sitemap_urls))
                    sample_sitemaps = random.sample(sub_sitemap_urls, sample_size) if sample_size > 0 else []
                    
                    sample_total = 0
                    for sub_url in sample_sitemaps:
                        try:
                            response = self.session.get(
                                sub_url, 
                                timeout=(self.connection_timeout, self.request_timeout),
                                headers=self.headers
                            )
                            
                            content = gzip.decompress(response.content)
                            sub_root = ET.fromstring(content)
                            product_count = len(sub_root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"))
                            sample_total += product_count
                            
                        except Exception as e:
                            logger.warning(f"Error processing sub-sitemap {sub_url}: {e}")
                    
                    # Extrapolate from the sample
                    if sample_size > 0:
                        estimated_total = (sample_total / sample_size) * len(sub_sitemap_urls)
                        total_count += int(estimated_total)
                        logger.info(f"Estimated {int(estimated_total)} products in sitemap {base_sitemap_url}")
                        
                except Exception as e:
                    logger.warning(f"Error processing sitemap {base_sitemap_url}: {e}")
                
            except Exception as e:
                logger.warning(f"Error fetching sitemap {base_sitemap_url}: {e}")
        
        return total_count

async def main():
    print("\nStarting Gumroad Scraper (Optimized Version)...")
    print("=" * 50)

    scraper = None
    try:
        scraper = GumroadScraper()
        
        print("\nBeginning product scraping...")
        print("=" * 50)
        
        start_time = time.time()
        total_processed = await scraper.scrape_all_products()
        end_time = time.time()
        
        duration = end_time - start_time
        products_per_second = total_processed / duration if duration > 0 else 0

        print("\nScraping Complete!")
        print("=" * 50)
        print(f"\nTotal products scraped: {total_processed}")
        print(f"Time taken: {duration:.2f} seconds")
        print(f"Speed: {products_per_second:.2f} products/second")
        print("\nResults have been saved to the 'output' directory.")

    except KeyboardInterrupt:
        print("\nScraping cancelled by user.")
        logger.warning("Scraping interrupted by user - saving current progress")
        if scraper:
            # Try to save any unsaved data
            try:
                scraper.save_results_from_queue()
            except Exception as e:
                logger.error(f"Could not save interrupted data: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in main: {str(e)}")
        if scraper:
            # Try to save any unsaved data
            try:
                scraper.save_results_from_queue()
            except Exception as save_error:
                logger.error(f"Could not save interrupted data: {save_error}")
        raise

if __name__ == "__main__":
    try:
        if sys.platform == 'win32':
            # For Windows, ensure proper asyncio event loop policy
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nScraping cancelled by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)
