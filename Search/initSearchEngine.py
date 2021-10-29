import sys
sys.path.append("./crawler") 
import crawler_threading
import IndexFiles
import SearchFiles

if __name__=='__main__':

    crawler_threading.main('http://baike.baidu.com/','bfs',5000)
    IndexFiles.main()
    SearchFiles.main()

