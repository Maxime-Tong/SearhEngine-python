# SJTU EE208

import sys, os, lucene, time, re
from urllib.parse import urlparse
from java.io import File
from org.apache.lucene.analysis.miscellaneous import LimitTokenCountAnalyzer
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.analysis.core import WhitespaceAnalyzer
from org.apache.lucene.document import Document, Field, FieldType, StringField, TextField
from org.apache.lucene.index import FieldInfo, IndexWriter, IndexReader ,IndexWriterConfig, Term, DirectoryReader,IndexOptions
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.search import IndexSearcher, TermQuery
from org.apache.lucene.util import Version

from Analyzer import JiebaAnalyzer

class IndexUpdate(object):
    def __init__(self, storeDir):
        lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        print('lucene', lucene.VERSION)
        self.dir = SimpleFSDirectory(File(storeDir).toPath())
        with open('index.txt','r') as f:
            self.UrlANDTitle=f.read()


    def findUrlANDTitle(self,filename):
        pattern="(?<=%s)\\t(.*?)\\t(.*?)(?=\\n)"%(filename)
        searchObj=re.search(pattern,self.UrlANDTitle)
        return (searchObj.group(1),searchObj.group(2))

    def getDomain(self,url):
        return urlparse(url).netloc

    def AddDocs(self,filepath):
        config = IndexWriterConfig(self.getAnalyzer())
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
        writer = IndexWriter(self.dir, config)
        doc = Document()
        
        t2 = FieldType()
        t2.setStored(False)
        t2.setTokenized(True)
        t2.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)  # Indexes documents, frequencies and positions.     
        
        t3 = FieldType()
        t3.setStored(True)
        t3.setTokenized(True)
        t3.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

        try:
            file = open(filepath, encoding='utf-8',errors='ignore')
            # 根据实验，发现采取 utf-8格式更加符合本次实验的需求
            contents = file.read()
            file.close()
            contents=JiebaAnalyzer(contents)
            doc = Document()
            url,title=self.findUrlANDTitle(os.path.basename(filepath))
            domain=self.getDomain(url)

            doc.add(StringField("name", os.path.basename(filepath), Field.Store.YES))
            doc.add(StringField("path", filepath, Field.Store.YES))

            doc.add(TextField("url",url,Field.Store.YES))
            doc.add(TextField("title",title,Field.Store.YES))
            doc.add(TextField("site",domain,Field.Store.YES))


            if len(contents) > 0 and len(title) > 0:
                titleIndex=JiebaAnalyzer(title)
                doc.add(Field("titleIndex",titleIndex, t3))
                doc.add(Field("contents", contents, t2))
            else:
                print("warning: no content in %s" % filename)
            writer.addDocument(doc)
        except Exception as e:
            print("Failed in indexDocs:", e)
        
        writer.addDocument(doc)
        writer.close()
        
    def DeleteDocs(self, fieldName, searchString):
        config = IndexWriterConfig(self.getAnalyzer())
        config.setOpenMode(IndexWriterConfig.OpenMode.APPEND)
        writer = IndexWriter(self.dir, config)
        writer.deleteDocuments(Term(fieldName, searchString))
        writer.close()

    def getHitCount(self, fieldName, searchString):
        reader = DirectoryReader.open(self.dir) #readOnly = True
        print('%s total docs in index' % reader.numDocs())
        
        searcher = IndexSearcher(reader) #readOnly = True
        t = Term(fieldName, searchString)
        query = TermQuery(t)
        hitCount = len(searcher.search(query, 50).scoreDocs)

        reader.close()
        print("%s total matching documents for %s\n---------------" \
              % (hitCount, searchString))
        return hitCount


    def getAnalyzer(self):
        # return StandardAnalyzer()
        return WhitespaceAnalyzer()

if __name__ == '__main__':
    try:
        fn = 'httpnews.sjtu.edu.cnjdyw20211001159213.html.txt'
        index = IndexUpdate('index')
        print(index.getHitCount('name', fn))
        print('delete %s' % fn)
        index.DeleteDocs('name', fn)
        index.getHitCount('name', fn)
        print('add %s' % fn)
        index.AddDocs('crawlerPage/%s' % fn)
        index.getHitCount('name', fn)
    except Exception as e:
        print("Failed: ", e)

