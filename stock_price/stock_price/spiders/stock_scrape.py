import scrapy

class StockScrape(scrapy.Spider):
    name = 'stock_scrape'

    def start_requests(self):
        urls = ['https://www.google.com/finance/historical?q=LON%3AOPM']

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        filename = 'what.html'
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log('Saved file %s' % filename)
