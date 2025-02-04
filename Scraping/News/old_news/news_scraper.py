import feedparser
import csv
import requests
from bs4 import BeautifulSoup
from googlenewsdecoder import new_decoderv1
import re
from datetime import datetime


class newsScraper:
    def __init__(self, query, after, before, site, max_articles, csv_path):
        self.query = query  # eg. 'GME'
        self.after = after  # eg 'YYYY-mm-dd'
        self.before = before
        self.site = site    # eg. 'ft.com'
        self.max_articles = max_articles  # how many articles to retrieve

        self.csv_name = f'{csv_path}/{self.site}/{self.query}_{self.after}_{self.before}.csv'


    def scrape_google_news_feed(self):
        # fetch rss feed from google news
        rss_url = f'https://news.google.com/rss/search?q="{self.query}"+site:{self.site}.com+after:{self.after}+before:{self.before}&hl=en-US&gl=US&ceid=US:en'
        feed = feedparser.parse(rss_url)
        # make csv file
        with open(self.csv_name, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, quoting = csv.QUOTE_ALL)
            writer.writerow(['Title', 'Link', 'Published', 'Source', 'Content'])

            if feed.entries:
                count = 0
                for entry in feed.entries:
                    title = entry.title.split(' - ')[0] # remove source from title
                    pubdate = entry.published
                    source = entry.source
                    link = self.decode_url(entry.link) # maybe use web archive instead? url = 'https://web.archive.org/*/' + decoded_url["decoded_url"]
                    content = self.scrape_article_content(link)

                    writer.writerow([title, link, pubdate, source.title, content])
                    
                    # up to max responses
                    count += 1
                    if count >= self.max_articles:
                        break
            else:
                print("Nothing Found!")


    def decode_url(self, url):
        try:
            decoded_url = new_decoderv1(url, interval=5)
            if decoded_url.get("status"):
                return decoded_url["decoded_url"]
            else:
                return "error decoding url"
        except Exception as e:
            return f"error decoding url" 
        

    def scrape_article_content(self, link):
        try:
            response = requests.get(link)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
             # extract main article content
            # (using p tags ensures we extract all content for different sites but if we  
            # are going from a set number of specific sites, we can adjust to make sure we're not 
            # getting irrelavant extra stuff, based on how their website works)
            paragraphs = soup.find_all('p')
            content = '\n'.join([p.get_text() for p in paragraphs])
            return content
        except Exception as e:
            return f"error scraping article content"



        
if __name__ == "__main__":
   
    scraper = newsScraper(query='Apple', after='2021-01-01', before='2021-02-01', site='ign', max_articles=10, csv_path='sample_news/')
    scraper.scrape_google_news_feed()
    scraper = newsScraper(query='Apple', after='2021-02-01', before='2021-03-01', site='ign', max_articles=10, csv_path='sample_news/')
    scraper.scrape_google_news_feed()
    scraper = newsScraper(query='Apple', after='2021-03-01', before='2021-04-01', site='ign', max_articles=10, csv_path='sample_news/')

    # # get user input for variables
    # def validate_date(date_text):
    #     try:
    #         datetime.strptime(date_text, '%Y-%m-%d')
    #         return True
    #     except ValueError:
    #         return False

    # query = input("Enter the search query: ").strip()
    # while not query:
    #     print("Search query cannot be empty.")
    #     query = input("Enter the search query: ").strip()

    # after = input("Enter the start date (YYYY-MM-DD): ").strip()
    # while not validate_date(after):
    #     print("Invalid date format. Please enter the date in YYYY-MM-DD format.")
    #     after = input("Enter the start date (YYYY-MM-DD): ").strip()

    # before = input("Enter the end date (YYYY-MM-DD): ").strip()
    # while not validate_date(before):
    #     print("Invalid date format. Please enter the date in YYYY-MM-DD format.")
    #     before = input("Enter the end date (YYYY-MM-DD): ").strip()

    # site = input("Enter the site to search: ").strip()
    # while not site:
    #     print("Site cannot be empty.")
    #     site = input("Enter the site to search: ").strip()

    # max_articles = input("Enter the maximum number of articles to retrieve: ").strip()
    # while not max_articles.isdigit() or int(max_articles) <= 0:
    #     print("Please enter a valid positive integer for the maximum number of articles.")
    #     max_articles = input("Enter the maximum number of articles to retrieve: ").strip()
    # max_articles = int(max_articles)


    # scraper = newsScraper(query, after, before, site, max_articles, csv_path='sample_news/')
    # scraper.scrape_google_news_feed()
