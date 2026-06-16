'''After our research on the notebook, 
Here is the .py file for scraping hotels'''

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy_selenium import SeleniumRequest
import logging
import re

cities = [
    "Mont Saint Michel", "St Malo", "Bayeux", "Le Havre", "Rouen",
    "Paris", "Amiens", "Lille", "Strasbourg", "Chateau du Haut Koenigsbourg",
    "Colmar", "Eguisheim", "Besancon", "Dijon", "Annecy",
    "Grenoble", "Lyon", "Gorges du Verdon", "Bormes les Mimosas", "Cassis",
    "Marseille", "Aix en Provence", "Avignon", "Uzes", "Nimes",
    "Aigues Mortes", "Saintes Maries de la mer", "Collioure", "Carcassonne",
    "Ariege", "Toulouse", "Montauban", "Biarritz", "Bayonne", "La Rochelle"
]

# Scraping from booking.com
# Class that inherits from scrapy.spider
class BookingSpider(scrapy.Spider):
    name = "booking"
    
    # starting method
    # Scrapy is asynchronous
    # Start generates the URLs and loads the pages
    async def start(self):
        for city in cities:
            url = f"https://www.booking.com/searchresults.html?ss={city}&lang=en-gb"
            # Yield allows you to start a query and come back to it later
            # We use it because the loading takes some time
            # This allows us to start tasks and come back when they are ready
            yield SeleniumRequest(
                url=url,
                callback=self.parse,
                wait_time=8,
                cb_kwargs={"city": city}
            )
    
    # Parse takes the loaded page and performs the scraping 
    def parse(self, response, city):
        # Scrapy passes the loaded page to parse by "response"
        cards = response.css('[data-testid="property-card"]')
        
        # For each hotel, 3 pieces of information are extracted
        # The name, the score and the url
        for card in cards:
            name = card.css('[data-testid="title"]::text').get()
            score = card.css('.f63b14ab7a.dff2e52086::text').get()
            url = card.css('[data-testid="title-link"]::attr(href)').get()
            
            if url:
                url = url.split("?")[0]
                # We follow the link for the second page
                yield SeleniumRequest(
                    url=url,
                    callback=self.parse_hotel,
                    wait_time=8,
                    cb_kwargs={
                        "city": city,
                        "name": name,
                        "score": score,
                        "url": url
                    }
                )
    # Function that extracts hotel descriptions and their GPS coordinates
    def parse_hotel(self, response, city, name, score, url):
        # hotel descriptions
        description = response.css('[data-testid="property-description"]::text').getall()
        # GPS coordinates
        lat = re.search(r'b_map_center_latitude\s*=\s*([\d.]+)', response.text)
        lon = re.search(r'b_map_center_longitude\s*=\s*([\d.]+)', response.text)
        
        # and we return a dictionary with all the collected information
        yield {
            "city": city,
            "name": name,
            "score": score,
            "url": url,
            "description": " ".join(description),
            "latitude": float(lat.group(1)) if lat else None,
            "longitude": float(lon.group(1)) if lon else None
        }

# Scrapy settings configuration
process = CrawlerProcess(settings={
    "USER_AGENT": "Chrome/97.0",
    "LOG_LEVEL": logging.INFO,  # displays information, warnings and errors
    # Saved results as JSON
    "FEEDS": {
        "hotels_raw.json": {"format": "json"}
    },
    # Selenium settings
    "SELENIUM_DRIVER_NAME": "chrome",
    "SELENIUM_DRIVER_EXECUTABLE_PATH": r"C:\Users\axelv\.wdm\drivers\chromedriver\win64\148.0.7778.178\chromedriver-win64/chromedriver.exe",
    "SELENIUM_DRIVER_ARGUMENTS": [
        "--no-sandbox",  # For Docker
        "--disable-dev-shm-usage",  # avoids memory crashes
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    ],
    # Limiting scraping to avoid crashes
    "CONCURRENT_REQUESTS": 1,  # Only one driver at a time to avoid overloading
    "DOWNLOAD_DELAY": 8,  
    "DOWNLOADER_MIDDLEWARES": {
        "scrapy_selenium.SeleniumMiddleware": 800
    }
})

process.crawl(BookingSpider)
process.start()