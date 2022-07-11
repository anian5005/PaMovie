from flask import Flask, render_template, redirect, url_for, request, flash, send_from_directory, jsonify, make_response
from package.db import sql

app = Flask(__name__)

@app.route('/')
def movie_home():
    return render_template("home.html")

@app.route('/test')
def test():
    return render_template('index.html')

@app.route('/api/movielist')
def movie_list():
    all_movie_list = sql.all_zh_data_from_db()
    movie_list_json = {'data': all_movie_list}
    return movie_list_json


@app.route('/api/movielist/<int:year_min>/<int:year_max>')
def movie_list_by_year(year_min, year_max):
    movie_list_year = sql.filter_zh_data_from_db(year_min, year_max)

    # print(movie_list)
    movie_list_json = {'data': movie_list_year}
    return movie_list_json

@app.route('/api/info/<string:merge_id>')
def movie_info(merge_id):
    data = sql.zh_data_from_db(merge_id)
    cast = sql.zh_cast_from_db(merge_id)
    rating = sql.rating_from_db(merge_id)
    data.update(cast)
    data.update(rating)
    print('data', data)
    return data

@app.route('/title/<string:merge_id>')
# tt1745960
def movie_page(merge_id):
    data = sql.zh_data_from_db(merge_id)
    # print('data', data)

    cast = sql.zh_cast_from_db(merge_id)
    # print('cast', cast)

    rating = sql.rating_from_db(merge_id)
    # print('rating', rating)

    first = data

    movie = first['en_title']
    zh_title = first['zh_title']
    release_date = first['release_date']
    image = first['image']
    runtime = first['runtime']


    imdb_score =  rating['imdb_score']
    imdb_count =rating['imdb_count']
    tomato_meter = rating['tomato_meter']
    tomato_review_count = rating['tomato_review_count']
    tomato_audience = rating['tomato_audience']
    tomato_audience_count = rating['tomato_audience_count']
    yahoo_score= rating['yahoo_score']
    yahoo_count= rating['yahoo_count']

    # print('movie', first)

    if cast.get('dir', None) != None:
        dirs = cast['dir'][0]
    else:
        dirs = 'no result'

    return render_template('movie_page.html',
                           movie=movie,
                           zh_title=zh_title,
                           release_date=release_date,
                           runtime=runtime,
                           rating=rating,
                           image=image,
                           dir=dirs,
                           imdb_score=imdb_score,
                           imdb_count=imdb_count,
                           tomato_meter=tomato_meter,
                           tomato_audience=tomato_audience,
                           tomato_review_count=tomato_review_count,
                           tomato_audience_count=tomato_audience_count,
                           yahoo_score=yahoo_score,
                           yahoo_count=yahoo_count
                           )

# app.run(debug=True, port=8000, host="0.0.0.0", ssl_context=('ssl/certificate.crt', 'ssl/private.key'))
app.run(debug=True, port=8000, host="0.0.0.0")
