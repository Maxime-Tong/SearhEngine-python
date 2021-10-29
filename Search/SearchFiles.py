# SJTU EE208
INDEX_DIR = "IndexFiles.index"
import sys, os, lucene
from java.io import File
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.analysis.core import WhitespaceAnalyzer
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.util import Version
from org.apache.lucene.search import BooleanQuery
from org.apache.lucene.search import BooleanClause

import jieba
import jieba.analyse

def JiebaAnalyzer(contents):
    seglist=jieba.cut_for_search(contents)
    return ' '.join(seglist)

def parseCommand(command):
    allowed_opt = ['title', 'site']
    command_dict = {}
    opt = 'contents'
    for i in command.split(' '):
        if ':' in i:
            opt, value = i.split(':')[:2]
            opt = opt.lower()
            if opt in allowed_opt and value != '':
                if opt == 'title' : 
                    opt='titleIndex'
                    value=JiebaAnalyzer(value)
                command_dict[opt] = command_dict.get(opt, '') + ' ' + value
        else:
            i=JiebaAnalyzer(i)
            command_dict[opt] = command_dict.get(opt, '') + ' ' + i

    return command_dict

def extract_tags(path):
    with open(path,'r',encoding='utf-8') as f:
        text=f.read()
        return jieba.analyse.textrank(text, topK=8, allowPOS=('ns', 'n', 'PER', 'LOC', 'ORG'))

def filter(resData):
    newData=list()
    s=set()
    for res in resData:
        if res['title'] not in s:
            s.add(res['title'])
            newData.append(res)
    return newData


def run(searcher, analyzer, keyword):

    result=list()
    command = keyword
    # command = unicode(command, 'GBK')
    if command == '':
        return result
    command_dict = parseCommand(command)
    # print(command_dict)
    # print()
    querys = BooleanQuery.Builder()
    for k,v in command_dict.items():
        query = QueryParser(k, analyzer).parse(v)
        querys.add(query, BooleanClause.Occur.MUST)
    scoreDocs = searcher.search(querys.build(), 50).scoreDocs
    # print("%s total matching documents." % len(scoreDocs))

    tags=['url','title']
    for i, scoreDoc in enumerate(scoreDocs):
        doc = searcher.doc(scoreDoc.doc)
        msg=dict()
        for tag in tags:
            msg[tag]=doc.get(tag)
        path='Search/'+doc.get('path')
        msg['tags']=' '.join(extract_tags(path))
        result.append(msg)
    
    # 过滤掉title相同的文本
    result=filter(result)
    return result

class Search:

    def __init__(self):
        self.vm_env=lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        STORE_DIR = "Search/index"
        print ('lucene', lucene.VERSION)
        self.directory = SimpleFSDirectory(File(STORE_DIR).toPath())
        self.searcher = IndexSearcher(DirectoryReader.open(self.directory))
        self.analyzer = WhitespaceAnalyzer()#Version.LUCENE_CURRENT)
        
    def __del__(self):
        del self.searcher

    def commitSearch(self,keyword):
        self.vm_env.attachCurrentThread()
        return run(self.searcher,self.analyzer,keyword)
