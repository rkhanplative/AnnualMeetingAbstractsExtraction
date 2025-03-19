import argparse
import concurrent.futures
import time
import os
import requests
import logging
import csv
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
from typing import List
import undetected_chromedriver as uc
import time


# Set up logging
logging.basicConfig(filename='abstract_downloader.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Ensure the download directory exists
DOWNLOAD_DIR: str = "downloaded_abstracts"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# CSV file to log downloads and manage cache
DOWNLOAD_LOG: str = "download_log.csv"

class Webdriver:
    BASE_URL = "https://ascopubs.org"

    def __init__(self) -> None:
        self.__start()
        self.download_history = self.__load_download_history()

    def __start(self) -> None:
        '''Private: Start the undetected Chrome WebDriver'''
        logging.debug("Starting undetected Chrome WebDriver...")
        self.driver = uc.Chrome()
        self.driver.delete_all_cookies()

    def __consent_to_cookies(self) -> None:
        '''Private: Consent to cookies if prompted'''
        try:
            logging.debug("Waiting for consent banner to appear...")
            consent_banner_locator = (By.CSS_SELECTOR, 'button#onetrust-accept-btn-handler')
            consent_banner = WebDriverWait(self.driver, 10).until(
                EC.visibility_of_element_located(consent_banner_locator)
            )
            logging.debug("Clicking consent banner...")
            consent_banner.click()
            logging.debug("Consent banner clicked.")
        except Exception as e:
            logging.error(f"Consent banner not found or could not be clicked: {str(e)}")

    def __load_download_history(self) -> set:
        '''Load download history from the CSV log file'''
        if os.path.exists(DOWNLOAD_LOG):
            with open(DOWNLOAD_LOG, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                return {row['download_url'] for row in reader if row['status'] == 'success'}
        return set()

    def __log_download(self, start_url: str, download_url: str, file_name: str, status: str) -> None:
        '''Log the download attempt to the CSV log file'''
        with open(DOWNLOAD_LOG, 'a', newline='') as csvfile:
            fieldnames = ['datetime', 'start_url', 'download_url', 'file_name', 'status']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if os.stat(DOWNLOAD_LOG).st_size == 0:
                writer.writeheader()
            writer.writerow({
                'datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'start_url': start_url,
                'download_url': download_url,
                'file_name': file_name,
                'status': status
            })

    def get_abstract_urls(self, url: str, max_pages: int) -> list:
        '''Retrieve all abstract URLs from multiple pages using Selenium to handle complex interactions'''
        logging.debug(f"Fetching abstract URLs from {url}")
        abstract_urls = []
        try:
            self.driver.get(url)
            self.__consent_to_cookies()
            for page in range(max_pages):
                logging.debug(f"Processing page {page + 1} of abstracts...")
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'card-format'))
                )
                logging.debug("Abstracts loaded successfully.")
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                abstract_urls.extend([urljoin(url, a['href']) for a in soup.find_all('a', class_='card-format') if 'abstract' in a.get_text().lower()])
                
                # Check if there's a "Next" button and click it
                next_button = self.driver.find_elements(By.CSS_SELECTOR, "li.page-item__arrow--next a.page-link")
                if next_button:
                    next_href = next_button[0].get_attribute('href')
                    next_url = urljoin(url, next_href)
                    logging.debug(f"Next button found, moving to {next_url}...")
                    self.driver.get(next_url)
                    logging.debug("Waiting for next page to load...")
                    time.sleep(2)  # Wait for the next page to load
                    logging.debug("Next page loaded.")
                else:
                    logging.debug("No more pages found.")
                    break
        except Exception as e:
            logging.error(f"Error fetching abstract URLs: {str(e)}")
        return abstract_urls

    def convert_to_download_url(self, abstract_url: str) -> str:
        '''Convert the abstract URL to the download URL'''
        return abstract_url.replace("/doi/abs/", "/doi/pdfdirect/")

    def download_abstract(self, abstract_url: str) -> None:
        '''Download the abstract by converting its URL to the download URL'''
        logging.debug(f"Downloading abstract from {abstract_url}")
        try:
            download_url = self.convert_to_download_url(abstract_url)
            file_name = download_url.split("/")[-1] + ".pdf"

            if download_url in self.download_history:
                logging.info(f"Skipping already downloaded file: {file_name}")
                return

            self.__save_article(abstract_url, download_url, file_name)
        except Exception as e:
            logging.error(f"Error downloading abstract: {str(e)}")
            self.__log_download(abstract_url, download_url, file_name, 'failed')

    def __save_article(self, start_url: str, download_url: str, file_name: str) -> None:
        '''Private: Save the article to the local directory and log the result'''
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
        }
        attempt = 0
        while attempt <= 5:
            try:
                logging.debug(f"Sending request to download PDF from {download_url}...")
                response = requests.get(download_url, headers=headers, stream=True)
                response.raise_for_status()
                if response.status_code == 200:
                    file_path = os.path.join(DOWNLOAD_DIR, file_name)
                    logging.debug(f"Saving PDF to {file_path}...")
                    with open(file_path, 'wb') as pdf_file:
                        for chunk in response.iter_content(chunk_size=1024):
                            if chunk:
                                pdf_file.write(chunk)
                    logging.info(f"Downloaded and saved to {file_path}")
                    self.__log_download(start_url, download_url, file_name, 'success')
                    self.download_history.add(download_url)
                    return
                else:
                    logging.error(f"Failed to download file from {download_url}. Status code: {response.status_code}")
                    self.__log_download(start_url, download_url, file_name, 'failed')
            except Exception as e:
                logging.error(f"Error saving article retrying: {str(e)}")
                attempt += 1
                wait_time = 2 ** attempt
                print(f"Attempt {attempt} failed: {e}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                
        self.__log_download(start_url, download_url, file_name, 'failed')    
        

    def close(self):
        '''Close the WebDriver'''
        logging.debug("Closing Safari WebDriver...")
        self.driver.quit()

def main():
    parser = argparse.ArgumentParser(description="Download abstracts from a specified URL.")
    parser.add_argument('url', type=str, help='URL to scrape for abstracts')
    parser.add_argument('--max_pages', type=int, default=10, help='Maximum number of pages to scrape')
    parser.add_argument('--max_threads', type=int, default=5, help='Maximum number of threads for downloading')
    args = parser.parse_args()

    driver = Webdriver()
    try:
        urls = driver.get_abstract_urls(args.url, args.max_pages)
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_threads) as executor:
            list(tqdm(executor.map(driver.download_abstract, urls), total=len(urls), desc="Downloading abstracts"))
    finally:
        driver.close()

if __name__ == '__main__':
    main()
