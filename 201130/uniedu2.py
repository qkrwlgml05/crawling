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

class Uniedu2Spider(scrapy.Spider):
    name = 'uniedu2'
    allowed_domains = ['https://www.uniedu.go.kr/']
    start_urls = ['https://www.uniedu.go.kr/uniedu/home/pds/pdsatcl/list.do?mid=SM00000532&limit=20&eqOdrby=false&eqViewYn=true&odr=news&hc=TY']

    def __init__(self):
        scrapy.Spider.__init__(self)
        self.start_urls = 'https://www.uniedu.go.kr/uniedu/home/pds/pdsatcl/list.do?mid=SM00000532&limit=20&eqOdrby=false&eqViewYn=true&odr=news&hc=TY'
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
        #total_page_text = response.xpath('//*[@id="content_section"]/div[5]/ol/li[14]/a/@href').extract()
        total_page_text = ['47']
        last_page_no = re.findall("\d+", str(total_page_text))
        page_no = 1
        # last_page_no[-1]
        last_page_no = int(last_page_no[-1])
        while True:
            if page_no > last_page_no:
                break
            link = "https://www.uniedu.go.kr/uniedu/home/pds/pdsatcl/list.do?page=" + str(page_no) + "&mid=SM00000532&limit=20&eqOdrby=false&eqViewYn=true&odr=news&hc=TY"
            yield scrapy.Request(link, callback=self.parse_each_pages,
                                 meta={'page_no': page_no, 'last_page_no': last_page_no},
                                 dont_filter=True)

            page_no += 1


    def parse_each_pages(self, response):
        page_no = response.meta['page_no']
        last_page_no = response.meta['last_page_no']
        #print(last_page_no)

        #last = response.xpath('//*[@id="content_section"]/ul[2]/li[1]/text()').get()
        last = 20
        if page_no == last_page_no:
            last = 9
        if page_no == last_page_no:
            category_last_no = int(last)
        else:
            #first = response.xpath('//*[@id="content_section"]/ul[2]/li[20]/text()').get()
            first = 1
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

            url = response.xpath('//*[@id="content_section"]/ul[2]/li[' + str(category_no) + ']/ul/li[2]/a/@href').get()
            url = "https://www.uniedu.go.kr/uniedu/home/pds/pdsatcl/" + url
            #print(url)
            yield scrapy.Request(url, callback=self.parse_post,
                                 dont_filter=True)
            category_no += 1




    def parse_post(self, response):
        item = CrawlnkdbItem()
        # title = response.css('#main > table > thead > tr > th font::text').get()
        title = response.xpath('//*[@id="content_section"]/div[2]/div[1]/h4/text()').get()
        #print(title)

        # table_text = response.css('#main > table > tbody > tr.boardview2 td::text').extract()
        # body = response.css('.descArea')[0].get_text()

        body = response.xpath('//*[@id="content_section"]/div[2]/div[2]').get()
        if body is None:
            test1 = response.xpath('//*[@id="content_section"]/div[2]/dl[1]').get()
            test1 = re.sub('<script.*?>.*?</script>', '', test1, 0, re.I | re.S)
            test1 = re.sub('<.+?>', '', test1, 0, re.I | re.S)
            test1 = re.sub('&nbsp;| |\t|\r|\n', " ", test1)
            test1 = re.sub('\"', "'", test1)

            test2 = response.xpath('//*[@id="content_section"]/div[2]/dl[2]').get()
            test2 = re.sub('<script.*?>.*?</script>', '', test2, 0, re.I | re.S)
            test2 = re.sub('<.+?>', '', test2, 0, re.I | re.S)
            test2 = re.sub('&nbsp;| |\t|\r|\n', " ", test2)
            test2 = re.sub('\"', "'", test2)
            body = test1 + test2
        else:
            body = re.sub('<script.*?>.*?</script>', '', body, 0, re.I | re.S)
            body = re.sub('<.+?>', '', body, 0, re.I | re.S)
            body = re.sub('&nbsp;| |\t|\r|\n', " ", body)
            body = re.sub('\"', "'", body)
        if body is None:
            body = "No text"
        if body == '':
            body = "No text"

        #print(body)

        # body = response.css('.descArea').xpath('string()').extract()

        date = response.xpath('//*[@id="content_section"]/div[2]/div/div[2]/p[1]/span/text()').get()
        #print(date)
        if date is None:
            date = "No date"

        writer = response.xpath('//*[@id="content_section"]/div[2]/div/div[2]/p[2]/span/text()').get()
        #print(writer)
        if writer is None:
            writer = "No writer"

        body_text = ''.join(body)

        top_category = "도서/동영상자료"

        item[config['VARS']['VAR1']] = title.strip()
        item[config['VARS']['VAR4']] = date.strip()
        item[config['VARS']['VAR3']] = writer.strip()
        item[config['VARS']['VAR2']] = body_text.strip()
        item[config['VARS']['VAR5']] = "통일부"
        item[config['VARS']['VAR6']] = "https://www.uniedu.go.kr/"
        item[config['VARS']['VAR7']] = top_category
        file_name = title
        file_icon = response.xpath('//*[@id="content_section"]/div[2]/div/div[3]/p[1]/a/text()').get()
        if not file_icon:
            file_icon = response.xpath('//*[@id="content_section"]/div[2]/div[1]/div[2]/p/a/text()').get()
        print(file_icon)
        file_icon = None
        if file_icon:
            file_download_url = response.xpath('//*[@id="content_section"]/div[2]/div/div[3]/p[1]/a/@href').extract()
            if file_download_url is None:
                file_download_url = response.xpath('//*[@id="content_section"]/div[2]/div[1]/div[2]/p/a/@href').extract()
            file_download_url = file_download_url[0]
            file_download_url = "https://www.uniedu.go.kr" + file_download_url
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

