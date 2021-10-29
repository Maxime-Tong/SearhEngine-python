# SJTU EE208

from flask import Flask, redirect, render_template, request, url_for
from Search import SearchFiles

app = Flask(__name__)

# 预加载，初始化lucene vm
@app.before_first_request
def load_index():
    global Searcher
    Searcher=SearchFiles.Search()


@app.route('/', methods=['POST', 'GET'])
def index():
    if request.method == "POST":
        keyword = request.form['keyword']
        return redirect(url_for('show_res', keyword=keyword))
    return render_template("index.html")


@app.route('/show_res',methods=['GET'])
def showbio():
    global Searcher
    keyword = request.args.get('keyword')
    resData=Searcher.commitSearch(keyword)
    return render_template("show_res.html",keyword=keyword,resData=resData)


if __name__ == '__main__':
    app.run(debug=True, port=8080)

