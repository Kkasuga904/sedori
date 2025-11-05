from __future__ import annotations

import logging
from typing import Dict, Optional

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from src.common.models import ProductListing


logger = logging.getLogger(__name__)


class ListingAutomationError(RuntimeError):
    """Raised when the Selenium automation fails."""


class AmazonListingAutomation:
    def __init__(self, config: Dict[str, str], driver_path: Optional[str] = None) -> None:
        self._config = config
        self._driver_path = driver_path

    def publish_listing(self, listing: ProductListing) -> None:
        options = ChromeOptions()
        options.add_argument("--headless=new")
        try:
            driver = webdriver.Chrome(options=options)
        except WebDriverException as exc:
            raise ListingAutomationError(f"Failed to start Chrome driver: {exc}") from exc

        try:
            self._login(driver)
            self._navigate_to_listing_page(driver)
            self._fill_listing_form(driver, listing)
            self._submit_listing(driver)
        finally:
            driver.quit()

    def _login(self, driver: webdriver.Chrome) -> None:
        driver.get(self._config["seller_central_url"])
        try:
            email = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "ap_email")))
            email.send_keys(self._config["username"])
            driver.find_element(By.ID, "continue").click()
            password = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "ap_password")))
            password.send_keys(self._config["password"])
            driver.find_element(By.ID, "signInSubmit").click()
        except (TimeoutException, NoSuchElementException) as exc:
            raise ListingAutomationError(f"Login failed: {exc}") from exc

    def _navigate_to_listing_page(self, driver: webdriver.Chrome) -> None:
        try:
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='newlisting']"))).click()
        except (TimeoutException, NoSuchElementException) as exc:
            raise ListingAutomationError(f"Unable to locate listing page: {exc}") from exc

    def _fill_listing_form(self, driver: webdriver.Chrome, listing: ProductListing) -> None:
        try:
            driver.find_element(By.NAME, "asin").send_keys(listing.asin)
            driver.find_element(By.NAME, "item_name").send_keys(listing.title)
            driver.find_element(By.NAME, "price").send_keys(str(listing.price))
            driver.find_element(By.NAME, "item_description").send_keys(listing.description)
            image_element = driver.find_element(By.NAME, "image_url")
            image_element.clear()
            image_element.send_keys(listing.image_urls[0] if listing.image_urls else "")
        except NoSuchElementException as exc:
            raise ListingAutomationError(f"Listing form structure changed: {exc}") from exc

    def _submit_listing(self, driver: webdriver.Chrome) -> None:
        try:
            driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CLASS_NAME, "successMessage")))
        except (TimeoutException, NoSuchElementException) as exc:
            raise ListingAutomationError(f"Listing submission failed: {exc}") from exc
