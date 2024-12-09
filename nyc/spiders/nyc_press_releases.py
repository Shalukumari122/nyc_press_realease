import os
import time
from datetime import datetime

import evpn
import pandas as pd
import scrapy
from scrapy.cmdline import execute

def extract_heading(response):
    # Extracts the text content of the first <h1> element from the given response using XPath.
    # If no <h1> element is found, it returns 'N/A'.
    heading = response.xpath('//h1/text()').extract_first()
    if heading:
        return heading
    else:
        return 'N/A'


def extract_description(response):
    # Extracts the text content of the first <p> element within a <div>
    # having the class "span6 about-description" from the given response using XPath.
    # If no matching element is found, it returns 'N/A'.
    description = response.xpath('//div[@class="span6 about-description"]//p/text()').extract_first()
    if description:
        return description
    else:
        return 'N/A'


def extract_date(row):
    # Extracts and parses a date from the given row using XPath.
    # The function checks for specific years ('2024' and '2023') in the extracted text
    # and attempts to parse the date. If parsing fails or no matching date is found, it returns 'N/A'.

    # Attempt to extract the date text directly from a <li> element
    date = row.xpath('./li/text()').extract_first()
    if '2024' in date:
        try:
            # Parse the date assuming the format is '%B %d, %Y' (e.g., 'January 1, 2024')
            parsed_date = datetime.strptime(date.strip(), '%B %d, %Y')
            return parsed_date.strftime('%Y-%m-%d')  # Return in 'YYYY-MM-DD' format
        except ValueError:
            # If parsing fails, return the original date string
            return date.strip()

    # Attempt to extract the date text from an <a> element within the <li>
    date = row.xpath('./li/a/text()').extract_first()
    if '2024' in date:
        try:
            # Parse the date assuming the same format as above
            parsed_date = datetime.strptime(date.strip(), '%B %d, %Y')
            return parsed_date.strftime('%Y-%m-%d')  # Return in 'YYYY-MM-DD' format
        except ValueError:
            # If parsing fails, return the original date string
            return date.strip()

    # Check again in the <li> text for dates with the year '2023'
    date = row.xpath('./li/text()').extract_first()
    if '2023' in date:
        try:
            # Parse the date assuming the same format as above
            parsed_date = datetime.strptime(date.strip(), '%B %d, %Y')
            return parsed_date.strftime('%Y-%m-%d')  # Return in 'YYYY-MM-DD' format
        except ValueError:
            # If parsing fails, return the original date string
            return date.strip()

    # Return 'N/A' if no matching date is found or if the input does not contain '2023' or '2024'
    return 'N/A'


def extract_pdf_link(row):
    # Extracts the href attribute of the first <a> element within a <li> in the given row.
    # Constructs the full URL by prepending 'https://www.nyc.gov' to the extracted link.
    # If no link is found, returns 'N/A'.

    # Attempt to extract the href attribute of the <a> element within the <li>
    pdf_link = row.xpath('./li/a/@href').extract_first()
    if pdf_link:
        # Prepend the base URL to construct the full PDF link
        return 'https://www.nyc.gov' + pdf_link
    else:
        # Return 'N/A' if no link is found
        return 'N/A'

def extract_text(row):
    # Extracts text content from an <a> element within a <li> in the given row.
    # If the extracted text does not contain '2024', it is returned as is.
    # Otherwise, it attempts to extract text directly from the <li> element.
    # If no valid text is found, returns 'N/A'.

    # Extract the text content of the <a> element within the <li>
    text = row.xpath('./li/a/text()').extract_first()
    if '2024' not in text:
        # Return the text if it does not contain '2024'
        return text

    # If '2024' is in the text, try extracting text directly from the <li> element
    text = row.xpath('./li/text()').extract_first()
    if text:
        # Return the text if it exists
        return text
    else:
        # Return 'N/A' if no valid text is found
        return 'N/A'

class NycPressReleasesSpider(scrapy.Spider):
    # Name of the spider
    name = "nyc_press_releases"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Initialize an empty list to store scraped data
        self.data_list = []
        # Start timer for measuring execution time
        self.start = time.time()
        super().__init__()

        # Connecting to VPN (USA) using ExpressVpnApi
        print('Connecting to VPN (USA)')
        self.api = evpn.ExpressVpnApi()
        self.api.connect(country_id='207')  # USA country code for VPN
        time.sleep(5)  # Delay to ensure VPN connection is established
        print('VPN Connected!' if self.api.is_connected else 'VPN Not Connected!')

        # Cookies for the request
        self.cookies = {
            # Add relevant cookies here
            '_ga': 'GA1.1.1134427543.1732860263',
            'ak_bmsc': 'D78F05B2DB08BC7C68823D9B1584BDD6~...',
            'JSESSIONID': 'CF657968B501D066AA254A8E8E41781D',
            # Other cookies omitted for brevity
        }

        # Headers for the request
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,...',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/...'
        }

    def start_requests(self):
        # Send the initial request to the target URL
        yield scrapy.Request(
            url='https://www.nyc.gov/site/doi/newsroom/press-releases.page',
            cookies=self.cookies,  # Use cookies for authentication if needed
            headers=self.headers,  # Include headers
            callback=self.extract_link,  # Process response with extract_link method
            dont_filter=True  # Ignore Scrapy's duplicate filter
        )

    def extract_link(self, response):
        # Extract links to individual press release pages
        links = response.xpath('//div[@class="row"]/div[@class="container"]/div[@class="span6"]/ul/li')
        for each_link in links:
            link = each_link.xpath('./a/@href').extract_first()
            link = 'https://www.nyc.gov' + link  # Form the full URL
            yield scrapy.Request(
                url=link,
                cookies=self.cookies,
                headers=self.headers,
                callback=self.parse_data,  # Process the individual page
                dont_filter=True
            )

    def parse_data(self, response):
        # Extract the heading and description of the press release
        heading = extract_heading(response)
        description = extract_description(response)

        # Extract rows containing additional information (e.g., date, text, links)
        all_rows = response.xpath('//div[@class="span6 about-description"]/ul')
        for row in all_rows:
            # Extract individual data points
            date = extract_date(row)
            text = extract_text(row)
            pdf_link = extract_pdf_link(row)

            # Append extracted data as a dictionary to the data list
            self.data_list.append({
                'date': date,
                'heading': heading,
                'description': description,
                'text': text,
                'pdf_link': pdf_link
            })

    def closed(self, reason):
        """Generate an Excel file with the scraped data when the spider closes."""
        if self.data_list:
            # Ensure the 'output' directory exists
            output_dir = '../output'
            os.makedirs(output_dir, exist_ok=True)

            # Define the file path
            filename = os.path.join(output_dir, 'nyc_press_releases.xlsx')

            # Create a DataFrame from the scraped data
            df = pd.DataFrame(self.data_list)

            # Add an 'id' column as the first column
            df.insert(0, 'id', range(1, len(df) + 1))  # Add a unique ID starting from 1

            # Save the DataFrame to an Excel file
            df.to_excel(filename, index=False)

            self.logger.info(f"Data saved to {filename}")
        else:
            self.logger.info("No data was scraped to save.")

        # Disconnect VPN if it is still connected
        if self.api.is_connected:
            self.api.disconnect()

        # Calculate and print the total scraping time
        end = time.time()
        print(f'Scraping done in {end - self.start} seconds.')


if __name__ == '__main__':
    execute('scrapy crawl nyc_press_releases'.split())