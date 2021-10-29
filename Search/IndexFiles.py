# SJTU EE208


INDEX_DIR = "IndexFiles.index"

import sys, os, lucene, threading, time
from datetime import datetime
from urllib.parse import urlparse

# from java.io import File
import re
from java.nio.file import Paths
from org.apache.lucene.analysis.miscellaneous import LimitTokenCountAnalyzer
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.analysis.core import WhitespaceAnalyzer
from org.apache.lucene.document import Document, Field, FieldType, StringField, TextField
from org.apache.lucene.index import FieldInfo, IndexWriter, IndexWriterConfig, IndexOptions
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.util import Version


import jieba

def JiebaAnalyzer(contents):
    seglist=jieba.cut_for_search(contents)
    return ' '.join(seglist)
"""
This class is loosely based on the Lucene (java implementation) demo class 
org.apache.lucene.demo.IndexFiles.  It will take a directory as an argument
and will index all of the files in that directory and downward recursively.
It will index on the file path, the file name and the file contents.  The
resulting Lucene index will be placed in the current directory and called
'index'.
"""

class Ticker(object):

    def __init__(self):
        self.tick = True

    def run(self):
        while self.tick:
            sys.stdout.write('.')
            sys.stdout.flush()
            time.sleep(1.0)


class IndexFiles(object):
    """Usage: python IndexFiles <doc_directory>"""

    def __init__(self, root, storeDir,indexFile):

        if not os.path.exists(storeDir):
            os.mkdir(storeDir)
        
        with open(indexFile,'r') as f:
            self.contents=f.read()

        store = SimpleFSDirectory(Paths.get(storeDir))
        analyzer = WhitespaceAnalyzer()
        analyzer = LimitTokenCountAnalyzer(analyzer, 1048576)
        config = IndexWriterConfig(analyzer)
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        writer = IndexWriter(store, config)

        self.indexDocs(root, writer)
        ticker = Ticker()
        print('commit index')
        threading.Thread(target=ticker.run).start()
        writer.commit()
        writer.close()
        ticker.tick = False
        print('done')
    
    def findUrlANDTitle(self,filename):
        pattern="(?<=%s)\\t(.*?)\\t(.*?)(?=\\n)"%(filename)
        searchObj=re.search(pattern,self.contents)
        return (searchObj.group(1),searchObj.group(2))

    def getDomain(self,url):
        return urlparse(url).netloc

    def indexDocs(self, root, writer):
        t2 = FieldType()
        t2.setStored(False)
        t2.setTokenized(True)
        t2.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)  # Indexes documents, frequencies and positions.     

        t3 = FieldType()
        t3.setStored(True)
        t3.setTokenized(True)
        t3.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

        for root, dirnames, filenames in os.walk(root):
            # root: crawlerPage
            # dirnames: []
            # filenames: [xxxx.txt ...xxxx.txt]
           
            for filename in filenames:
                if not filename.endswith('.txt'):
                    continue
                print("adding", filename)
                try:
                    path = os.path.join(root, filename)
                    file = open(path, encoding='utf-8',errors='ignore')
                    # 根据实验，发现采取 utf-8格式更加符合本次实验的需求
                    contents = file.read()
                    file.close()
                    contents=JiebaAnalyzer(contents)
                    doc = Document()
                    url,title=self.findUrlANDTitle(filename)
                    domain=self.getDomain(url)
                    
                    doc.add(StringField("name", filename, Field.Store.YES))
                    doc.add(StringField("path", path, Field.Store.YES))
                    doc.add(TextField("url",url,Field.Store.YES))
                    doc.add(TextField("site",domain,Field.Store.YES))
                    doc.add(TextField("title",title,Field.Store.YES))
                    
                    if len(contents) > 0 and len(title) > 0:
                        titleIndex=JiebaAnalyzer(title)
                        doc.add(Field("titleIndex",titleIndex, t3))
                        doc.add(Field("contents", contents, t2))
                    else:
                        print("warning: no content in %s" % filename)
                    writer.addDocument(doc)
                except Exception as e:
                    print("Failed in indexDocs:", e)

def main():
    lucene.initVM()#vmargs=['-Djava.awt.headless=true'])
    print('lucene', lucene.VERSION)
    # import ipdb; ipdb.set_trace()
    start = datetime.now()
    try:
        IndexFiles('crawlerPage', "index","index.txt")
        end = datetime.now()
        print(end - start)
    except Exception as e:
        print("Failed: ", e)
        raise e

if __name__ == '__main__':
    main()