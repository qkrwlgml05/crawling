# -*- coding: utf-8 -*
import os
import gridfs
import pymongo
import jpype
import scrapy
import re
from crawlNKDB.items import CrawlnkdbItem
from tika import parser
from tempfile import NamedTemporaryFile
from itertools import chain
control_chars = ''.join(map(chr, chain(range(0, 9), range(11, 32), range(127, 160))))
CONTROL_CHAR_RE = re.compile('[%s]' % re.escape(control_chars))

import configparser
config = configparser.ConfigParser()
config.read('./../lib/config.cnf')

class Kinu42Spider(scrapy.Spider):
    name = 'kinu4-2'
    allowed_domains = ['http://www.kinu.or.kr/']
    start_urls = ['https://www.kinu.or.kr/brd/board/636/L/CATEGORY/689/menu/681?brdCodeField=CATEGORY&brdCodeValue=689']

    def __init__(self):
        scrapy.Spider.__init__(self)
        self.start_urls = 'https://www.kinu.or.kr/brd/board/636/L/CATEGORY/689/menu/681?brdCodeField=CATEGORY&brdCodeValue=689'
        # 몽고에 넣겠
        self.client = pymongo.MongoClient(config['DB']['MONGO_URI'])
        self.db = self.client['attchment']
        self.fs = gridfs.GridFS(self.db)
        # jpype, java lib 연결
        jarpath = os.path.join(os.path.abspath('.'), './../lib/hwp-crawl.jar')
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=%s" % jarpath)

    def start_requests(self):
        yield scrapy.Request(self.start_urls, self.parse, dont_filter=True)

    def parse(self, response):
        # print(response)
        # 페이지 개수
        #total_page_text = response.xpath('//*[@id="pageNation_wrap"]/div/a[13]/@onclick').extract()
        total_page_text = ['10']
        last_page_no = re.findall("\d+", str(total_page_text))
        page_no = 1
        # last_page_no[-1]
        last_page_no = int(last_page_no[-1])
        while True:
            if page_no > last_page_no:
                break
            link = "https://www.kinu.or.kr/brd/board/636/L/CATEGORY/689/menu/681?brdType=L&searchField=&searchText=&thisPage=" + str(page_no)
            yield scrapy.Request(link, callback=self.parse_each_pages,
                                 meta={'page_no': page_no, 'last_page_no': last_page_no},
                                 dont_filter=True)

            page_no += 1


    def parse_each_pages(self, response):
        page_no = response.meta['page_no']
        last_page_no = response.meta['last_page_no']
        #print(last_page_no)
        last = response.xpath('//*[@id="cmsContent"]/div[3]/table/tbody/tr[1]/td[1]/text()').get()
        if page_no == last_page_no:
            category_last_no = int(last)
        else:
            first = response.xpath('//*[@id="cmsContent"]/div[3]/table/tbody/tr[10]/td[1]/text()').get()
            # first = re.findall("\d+", str(first))
            category_last_no = int(last) - int(first) + 1
        #print(category_last_no)
        category_no = 1
        while True:
            if (category_no > category_last_no):
                break
            # category_link = response.xpath('// *[ @ id = "board_list"] / table / tbody / tr[' + str(category_no) + '] / td[2]').xpath('string()').get()
            # onclick_text = response.xpath(category_link).extract()
            # url = re.findall("\d+" ,str(onclick_text))

            #url = response.xpath('//*[@id="cmsContent"]/div[2]/table/tbody/tr[' + str(category_no) + ']/td[2]/a/@href').get()
            url = "https://www.kinu.or.kr/brd/board/636/L/CATEGORY/688/menu/680?brdType=L&searchField=&searchText=&thisPage=" + str(page_no)

            yield scrapy.Request(url, callback=self.parse_post, meta={'category_no': category_no},
                                 dont_filter=True)
            category_no += 1




    def parse_post(self, response):
        item = CrawlnkdbItem()
        category_no = response.meta['category_no']
        # title = response.css('#main > table > thead > tr > th font::text').get()
        title = response.xpath('//*[@id="cmsContent"]/div[3]/table/tbody/tr[' + str(category_no) + ']/td[2]/text()').get()
        print(title)

        # table_text = response.css('#main > table > tbody > tr.boardview2 td::text').extract()
        # body = response.css('.descArea')[0].get_text()

        #body = response.xpath('//*[@id="tab_con"]').get()
        body = "1960년대 이후 1990년대 초까지 북한당국은 공식통계를 체계적으로 하지 않았다. 동 통계집은 이러한 북한통계의 공백기를 메우기 위해 한국 통일부가 북한의 각 문헌자료 속에 산재한 당시의 통계들을 하나로 모아 간행한 것이다. 여기에 수록된 통계들은 1990년대 이전까지의 북한과 관련된 거의 유일한 통계자료라고 할 수 있다."
        # body = response.css('.descArea').xpath('string()').extract()

        #date = response.xpath('//*[@id="cmsContent"]/div[2]/div[2]/table/tbody/tr[2]/td[1]/text()').get()
        date = "1960-90"
        #print(date)

        #writer = response.xpath('//*[@id="cmsContent"]/div[2]/div[2]/table/tbody/tr[1]/td/text()').get()
        writer = "통일부"
        #print(writer)

        body_text = ''.join(body)

        top_category = response.xpath('//*[@id="container"]/div[3]/div[1]/div/h2/text()').get()

        item['post_title'] = title.strip()
        item['post_date'] = date.strip()
        item['post_writer'] = writer.strip()
        item['post_body'] = body_text.strip()
        item['published_institution'] = "통일연구원"
        item['published_institution_url'] = "http://www.kinu.or.kr/www/jsp/prg/"
        item[config['VARS']['VAR7']] = top_category


        file_name = title
        file_icon = response.xpath('//*[@id="cmsContent"]/div[3]/table/tbody/tr[1]/td[3]/a[2]/img').get()
        if file_icon:
            file_download_url = response.xpath('//*[@id="cmsContent"]/div[3]/table/tbody/tr[1]/td[3]/a[2]/@href').extract()
            file_download_url = file_download_url[0]
            file_download_url = "http://www.kinu.or.kr/" + file_download_url
            item[config['VARS']['VAR10']] = file_download_url
            item[config['VARS']['VAR9']] = file_name
            print("@@@@@@file name ", file_name)
            if file_icon.find("hwp") != -1:
                print('find hwp')
                yield scrapy.Request(file_download_url, callback=self.save_file_hwp, meta={'item': item})  #
            else:
                yield scrapy.Request(file_download_url, callback=self.save_file,
                                     meta={'item': item, 'file_download_url': file_download_url,
                                           'file_name': file_icon}, dont_filter=True)
        else:
            print("###############file does not exist#################")
            yield item

    def save_file(self, response):
        # import wget
        item = response.meta['item']
        print("save_file")
        # file_download_url = response.meta['file_download_url']
        # file_name = '/FileDownload/' + response.meta['file_name']
        # wget.download(file_download_url, out=file_name)

        file_id = self.fs.put(response.body)
        item[config['VARS']['VAR11']] = file_id

        tempfile = NamedTemporaryFile()
        tempfile.write(response.body)
        tempfile.flush()

        extracted_data = parser.from_file(tempfile.name)
        print("hey")
        extracted_data = extracted_data["content"]
        if str(type(extracted_data)) == "<class 'str'>":
            extracted_data = CONTROL_CHAR_RE.sub('', extracted_data)
            extracted_data = extracted_data.replace('\n\n', '')
        tempfile.close()
        item[config['VARS']['VAR12']] = extracted_data
        yield item


    def save_file_hwp(self, response):
        item = response.meta['item']
        file_id = self.fs.put(response.body)
        item[config['VARS']['VAR11']] = file_id

        tempfile = NamedTemporaryFile()
        tempfile.write(response.body)
        tempfile.flush()

        testPkg = jpype.JPackage('com.argo.hwp') # get the package
        JavaCls = testPkg.Main # get the class
        hwp_crawl = JavaCls() # create an instance of the class
        extracted_data = hwp_crawl.getStringTextFromHWP(tempfile.name)
        if str(type(extracted_data)) == "<class 'str'>":
            extracted_data = CONTROL_CHAR_RE.sub('', extracted_data)
            extracted_data = extracted_data.replace('\n\n', '')
        print(extracted_data)
        print("###############get the hwp content###############")
        tempfile.close()
        item[config['VARS']['VAR12']] = extracted_data
        yield item

