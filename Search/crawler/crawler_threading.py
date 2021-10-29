# SJTU EE208
import time
import os
import re
import string
import sys
import urllib.error
import urllib.parse
import urllib.request
from bs4 import BeautifulSoup
import threading
import queue
from tqdm import tqdm


import Bitarray

q=queue.Queue()
NUM=4 # 4个线程
varLock=threading.Lock()
crawled=Bitarray.BloomFilters(size=20*5000)

def valid_filename(s):
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    s = ''.join(c for c in s if c in valid_chars)+'.txt'
    return s


def get_page(page):
    try:
        req=urllib.request.Request(page)
        req.add_header('User-Agent','Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36')
        response=urllib.request.urlopen(req,timeout=1)
        
        # 在实际爬取的过程中，需要判断超链接必须为网址而非下载链接，否则会出现很多不可预知的错误
        HttpMessage=response.info()
        ContentType = HttpMessage.get_content_type()
        if "text/html" != ContentType:return
        codetype=HttpMessage.get_content_charset()
        content=response.read()
        content=content.decode(codetype,'ignore') # 全部转成unicode的形式
        if response.getcode() == 200:
            return content
    # except urllib.error.URLError:
    except:
        return

def get_all_links(content, page):
    links = []
    soup=BeautifulSoup(content,features='html.parser')
    a_labels=soup.findAll('a',{'href' : re.compile('^http|^/')})
    for a in a_labels:
        link=a['href']
        link=urllib.parse.urljoin(page,link)
        links.append(link)
    title=soup.title
    if title==None:
        add_page_to_folder(page,soup.get_text(),'')
    else:
        add_page_to_folder(page,soup.get_text(),title.text)
    
    return links

def add_page_to_folder(page,content,title):  
    index_filename = 'index.txt'  
    folder = 'crawlerPage' 
    filename = valid_filename(page)  
    #print("adding %s"%filename)
    index = open(index_filename, 'a')
    index.write(str(filename) + '\t' + str(page) + '\t' + title + '\n')
    index.close()
    if not os.path.exists(folder):  
        os.mkdir(folder)
    f = open(os.path.join(folder, filename), 'w') # windows 下要改成 encoding='utf-8'
    f.write(content)
    f.close()


def working(max_page,ID):
    qbar=tqdm(total=max_page,desc='线程{}'.format(ID),leave=True)
    while True:
        page=q.get()
        if not crawled.lookup(page):
            # print(page)
            # 在爬取大量网页的时候，对于无效网页，同样应该记录下来，否则会重复爬取无效网站
            if varLock.acquire():
                crawled.add(page)
                varLock.release()
            
            if len(page)>100:continue # 网址过长，文件名不能存储，舍弃

            content=get_page(page)
            if content==None:continue
            outlinks=get_all_links(content,page)
            for link in outlinks:
                q.put(link)

            qbar.update()
            max_page-=1
            if max_page==0:break # 达到需要爬取的任务数，停止程序
            
        # q.task_done() 此操作无意义，因为队列中爬取的网址总是无限的
        
            

            


def main(seed,method,max_page):
    start = time.time()
    #seed,method,max_page='https://baike.baidu.com/','bfs',5000
    q.put(seed)
    ts=[threading.Thread(target=working, args=(max_page//NUM,i+1))for i in range(NUM)]    

    print('commit crawle')

    for t in ts:
        t.setDaemon(True)
        t.start()
    # 当所有线程爬到指定个数的page时线程结束
    # 当所有线程结束时，主程序继续运行，否则阻塞等待
    for t in ts:
        t.join()

    end=time.time()
    print(end-start)
    print('crawler done.')

if __name__ == '__main__':
    main(sys.argv[1],sys.argv[2],int(sys.argv[3]))