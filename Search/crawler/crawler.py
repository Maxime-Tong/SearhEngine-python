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

from HashTable import HashTable
import Bitarray

def valid_filename(s):
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    s = ''.join(c for c in s if c in valid_chars)+'.html' # 修改文件后缀为.html 方便直接查看
    return s


def get_page(page):
    req=urllib.request.Request(page)
    req.add_header('User-Agent','Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36')
    try:
        response=urllib.request.urlopen(req,timeout=3)
        content=response.read()
        print(content)
        if response.getcode() == 200:
            return content
    # except urllib.error.URLError:
    except:
        return None

def get_all_links(content, page):
    links = []
    soup=BeautifulSoup(content,features='html.parser')
    a_labels=soup.findAll('a',{'href' : re.compile('^http|^/')})
    for a in a_labels:
        link=a['href']
        if link[0]!='h':link=urllib.parse.urljoin(page,link)
        links.append(link)
    return links


def union_dfs(a, b):
    for e in b:
        if e not in a:
            a.append(e)


def union_bfs(a, b):
    a[0:0]=list(set(b))


def add_page_to_folder(page, content):  # 将网页存到文件夹里，将网址和对应的文件名写入index.txt中
    index_filename = 'index.txt'  # index.txt中每行是'网址 对应的文件名'
    folder = 'html'  # 存放网页的文件夹
    filename = valid_filename(page)  # 将网址变成合法的文件名
    index = open(index_filename, 'ab')
    index.write(page.encode('utf8', 'ignore')+ b'\t' + filename.encode('utf8','ignore') + b'\n') # write() argument must be bytes, not str
    index.close()
    if not os.path.exists(folder):  # 如果文件夹不存在则新建
        os.mkdir(folder)
    f = open(os.path.join(folder, filename), 'wb')
    f.write(content)  # 将网页存入文件
    f.close()

def crawl_set(seed, method, max_page):
    # 鲁棒性
    if max_page<=0:
        print('max_page error!')
        return 

    tocrawl = [seed]
    crawled = set()
    graph = {}
    count = 0

    while tocrawl:
        page = tocrawl.pop()
        #if page not in crawled:
        if page not in crawled:
            #print(page)
            content = get_page(page)
            if content==None:continue # 对于打不开的网页忽略
            # add_page_to_folder(page, content)
            outlinks = get_all_links(content, page)

            # 记录图的结构
            if outlinks!=[]:graph[page]=outlinks
            globals()['union_%s' % method](tocrawl, outlinks)
            crawled.add(page)
            max_page-=1
            if max_page==0:break # 达到最大深度
    return graph, crawled

def crawl_hashtable(seed, method, max_page):
    # 鲁棒性
    if max_page<=0:
        print('max_page error!')
        return 

    tocrawl = [seed]
    # crawled = []
    crawled=HashTable()
    graph = {}
    count = 0

    while tocrawl:
        page = tocrawl.pop()
        #if page not in crawled:
        if not crawled.hashtable_lookup(page):
            # print(page)
            content = get_page(page)
            if content==None:continue # 对于打不开的网页忽略
            # add_page_to_folder(page, content)
            outlinks = get_all_links(content, page)

            # 记录图的结构
            if outlinks==[]:continue
            else:graph[page]=outlinks


            globals()['union_%s' % method](tocrawl, outlinks)
            crawled.hashtable_add(page)
            max_page-=1
            if max_page==0:break # 达到最大深度
    # crawled.check()
    return graph, crawled

false_cnt=0
def crawl_BloomFliters(seed,method,max_page):
    global false_cnt
    tocrawl = [seed]
    crawled = Bitarray.BloomFilters(size=20*max_page)# 令k=10
    # _______________________________________
    # 记录实际存储的crawled page 用以统计错误率
    #real_crawled=HashTable()
    # _______________________________________
    graph = {}
    while tocrawl:
        page = tocrawl.pop()
        if not crawled.lookup(page):

            content = get_page(page)
            if content==None:continue # 对于打不开的网页忽略
            outlinks = get_all_links(content,page)

            if outlinks!=[]:graph[page]=outlinks

            globals()['union_%s' % method](tocrawl, outlinks)
            crawled.add(page)
            # 加入real_crawled
            #real_crawled.hashtable_add(page)

            max_page-=1
            if max_page==0:break # 达到最大深度

        #elif not real_crawled.hashtable_lookup(page):false_cnt+=1 # 统计bloomfliter发生hash冲突的网页个数
    return graph, crawled


if __name__ == '__main__':
    """
    seed = sys.argv[1]
    method = sys.argv[2]
    max_page = sys.argv[3]
    """
    start = time.time()
    seed,method,max_page='http://www.sjtu.edu.cn','bfs',500
    """
    graph, crawled = crawl_hashtable(seed, method, max_page)
    end1 = time.time()
    print("hashtable: %fs"%(end1 - start))
    """
    
    graph,crawled=crawl_set(seed,method,max_page)
    
    end2=time.time()
    print("list: %fs"%(end2-start))
    """
    graph,crawled=crawl_BloomFliters(seed,method,max_page)
    end3=time.time()
    print("BloomFliters: %fs"%(end3-start))
    print("错误个数：%f"%(false_cnt))
    """