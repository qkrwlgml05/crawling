# -*- coding: utf-8 -*
import os
import gridfs
import pymongo
import jpype
import scrapy
import re
from ecommerce.items import EcommerceItem
from tika import parser
from tempfile import NamedTemporaryFile
from itertools import chain
control_chars = ''.join(map(chr, chain(range(0, 9), range(11, 32), range(127, 160))))
CONTROL_CHAR_RE = re.compile('[%s]' % re.escape(control_chars))

import configparser
config = configparser.ConfigParser()
config.read('./../lib/config.cnf')

class Peacewomen5Spider(scrapy.Spider):
    name = 'peacewomen5'
    allowed_domains = ['http://www.peacewomen.or.kr/']
    start_urls = ['http://www.peacewomen.or.kr/index.php?mid=wmp_pds_unity']

    def __init__(self):
        scrapy.Spider.__init__(self)
        self.start_urls = 'http://www.peacewomen.or.kr/index.php?mid=wmp_pds_unity'
        # 몽고에 넣겠다
        self.client = pymongo.MongoClient(config['DB']['MONGO_URI'])
        self.db = self.client['attchment']
        self.fs = gridfs.GridFS(self.db)
        # jpype, java lib 연결
        jarpath = os.path.join(os.path.abspath('.'), './../lib/hwp-crawl.jar')
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=%s" % jarpath)

    def start_requests(self):
        yield scrapy.Request(self.start_urls, self.parse, dont_filter=True)

    def parse(self, response):
        # 페이지 개수
        total_page_text = response.xpath('//*[@id="s_mid21_wrap0"]/div/div[2]/div[1]/a[4]/@href').extract()
        #print(total_page_text)
        last_page_no = re.findall("\d+", str(total_page_text))
        page_no = 1
        # last_page_no[-1]
        last_page_no = int(last_page_no[-1])
        while True:
            if page_no > last_page_no:
                break
            link = "http://www.peacewomen.or.kr/index.php?mid=wmp_pds_unity&page=" + str(page_no)
            yield scrapy.Request(link, callback = self.parse_each_pages, meta={'page_no': page_no, 'last_page_no': last_page_no},
                dont_filter=True)

            page_no += 1


    def parse_each_pages(self, response):
        page_no = response.meta['page_no']
        last_page_no = response.meta['last_page_no']
        last = response.xpath('//*[@id="board_list"]/table/tbody/tr[3]/td[1]/text()').get()
        if page_no == last_page_no:
            category_last_no = int(last) + 2
        else:
            first = response.xpath('//*[@id="board_list"]/table/tbody/tr[20]/td[1]/text()').get()
            #first = re.findall("\d+", str(first))
            category_last_no = int(last) - int(first) + 3
        if page_no == 1:
            category_no = 1
        else:
            category_no = 3
        while True:
            if(category_no > category_last_no):
                break
            #category_link = response.xpath('// *[ @ id = "board_list"] / table / tbody / tr[' + str(category_no) + '] / td[2]').xpath('string()').get()
            #onclick_text = response.xpath(category_link).extract()
            #url = re.findall("\d+" ,str(onclick_text))
            url = response.xpath('//*[@id="board_list"]/table/tbody/tr[' + str(category_no) + ']/td[2]/a/@href').get()
            url = "http://www.peacewomen.or.kr/index.php?mid=wmp_pds_cul&page=" + url
            print(url)
            yield scrapy.Request(url, callback=self.parse_post,
                dont_filter=True)
            category_no += 1




    def parse_post(self, response):
        item = EcommerceItem()
        #title = response.css('#main > table > thead > tr > th font::text').get()
        title = response.xpath('//*[@id="s_mid21_wrap0"]/div/div[1]/div[1]/h1/a/span/text()').get()
        if title is None:
            title = response.xpath('//*[@id="s_mid21_wrap0"]/div/div[1]/div[1]/h1/a/text()').get()
        #table_text = response.css('#main > table > tbody > tr.boardview2 td::text').extract()
        # body = response.css('.descArea')[0].get_text()
        body = response.xpath('// *[ @ id = "s_mid21_wrap0"] / div / div[1] / div[2]').get()
        #body = re.search('<body.*/body>', body, re.I | re.S)
        body = re.sub('<script.*?>.*?</script>', '', body, 0, re.I | re.S)
        body = re.sub('<.+?>', '', body, 0, re.I | re.S)
        body = re.sub('&nbsp;| |\t|\r|\n', " ", body)
        body = re.sub('\"', "'", body)
        print(body)


        #body = response.css('.descArea').xpath('string()').extract()

        date = response.xpath('//*[@id="s_mid21_wrap0"]/div/div[1]/div[1]/p[1]/text()').get()

        writer = response.xpath('//*[@id="s_mid21_wrap0"]/div/div[1]/div[1]/p[2]/a/text()').get()

        body_text = ''.join(body)

        top_category = "평화통일운동"

        item['post_title'] = title.strip()
        item['post_date'] = date.strip()
        item['post_writer'] = writer.strip()
        item['post_body'] = body_text.strip()
        item['published_institution'] = "평화를 만드는 여성회"
        item['published_institution_url'] = "http://www.peacewomen.or.kr/index.php?mid=wmp_pds_mil"
        item[config['VARS']['VAR7']] = top_category


        yield item

        file_name = title

        file_icon = response.xpath('//*[@id="s_mid21_wrap0"]/div/div[1]/div[3]/div[1]/ul/li/a/text()').get()
        file_icon = None

        if file_icon:
            file_download_url = response.xpath('//*[@id="s_mid21_wrap0"]/div/div[1]/div[3]/div[1]/ul/li/a/@href').extract()
            file_download_url = file_download_url[0]
            file_download_url = "http://www.peacewomen.or.kr/" + file_download_url

            item[config['VARS']['VAR10']] = file_download_url
            item[config['VARS']['VAR9']] = file_name
            print("@@@@@@file name ", file_name)
            if file_icon.find("hwp") != -1:
                print('find hwp')
                yield scrapy.Request(file_download_url, callback=self.save_file_hwp, meta={'item': item}, dont_filter=True)  #
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

        testPkg = jpype.JPackage('com.argo.hwp')  # get the package
        JavaCls = testPkg.Main  # get the class
        hwp_crawl = JavaCls()  # create an instance of the class
        extracted_data = hwp_crawl.getStringTextFromHWP(tempfile.name)
        if str(type(extracted_data)) == "<class 'str'>":
            extracted_data = CONTROL_CHAR_RE.sub('', extracted_data)
            extracted_data = extracted_data.replace('\n\n', '')
        print(extracted_data)
        print("###############get the hwp content###############")
        tempfile.close()
        item[config['VARS']['VAR12']] = extracted_data
        yield item
