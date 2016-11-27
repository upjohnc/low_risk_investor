import scrapy
import urllib.parse as url_parse


class TorontoSpider(scrapy.Spider):
    name = 'toronto_ticker'
    alphabet = [chr(x) for x in range(97, 123)]
    toronto_exchange_code = 'TSE'

    def start_requests(self):
        url_string = 'http://www.tmxmoney.com/TMX/HttpController?GetPage=ListedCompanyDirectory&SearchCriteria=Name&SearchKeyword={0}&SearchType=StartWith&Page=1&SearchIsMarket=Yes&Market=T&Language=en'

        urls = [url_string.format(x) for x in self.alphabet]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        qs = url_parse.urlparse(response._url)[4]
        page_number = url_parse.parse_qs(qs)['Page'][0]
        search_letter = url_parse.parse_qs(qs)['SearchKeyword'][0]
        file_name = 'toronto_{search_letter}_{page_number}.html'.format(page_number=page_number, search_letter=search_letter)
        with open(file_name, 'wb') as f:
            f.write(response.body)

        links = response.css('a').extract()
        for link in links:
            if 'Next' in link:
                next_page = response.urljoin(link.split('href=')[1].split('"')[1].replace('&amp;', '&'))
                yield scrapy.Request(url=next_page, callback=self.parse)
