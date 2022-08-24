from flask import Flask, render_template
from package.db.sql_temp import all_zh_data_from_db, filter_zh_data_from_db, zh_data_from_db,movie_page_data,zh_cast_from_db

app = Flask(__name__)

@app.route('/')
def movie_home():
    try:
        return render_template("home.html")
    except:
        return render_template('404.html'), 404

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

@app.route('/api/movielist')
def movie_list():
    all_movie_list = all_zh_data_from_db()
    movie_list_json = {'data': all_movie_list}
    return movie_list_json


@app.route('/api/movielist/<int:year_min>/<int:year_max>')
def movie_list_by_year(year_min, year_max):
    movie_list_year = filter_zh_data_from_db(year_min, year_max)

    # print(movie_list)
    movie_list_json = {'data': movie_list_year}
    return movie_list_json

@app.route('/api/info/<string:merge_id>')
def movie_info(merge_id):

    data = zh_data_from_db(merge_id)
    cast = zh_cast_from_db(merge_id)
    # rating = sql.rating_from_db(merge_id)
    data.update(cast)
    # data.update(rating)
    return data





@app.route('/title/<string:merge_id>')
def movie_page(merge_id):
    try:
        general, cast_dict = movie_page_data(merge_id)

        movie = general['en_title']
        zh_title = general['zh_title']
        release_date = general['release_date']
        image = general['image']
        runtime = general['runtime']
        imdb_score = general['imdb_score']
        imdb_count = general['imdb_count']
        tomato_meter = general['tomato_meter']
        tomato_review_count = general['tomato_review_count']
        tomato_audience = general['tomato_audience']
        tomato_audience_count = general['tomato_audience_count']
        yahoo_score = general['yahoo_score']
        yahoo_count = general['yahoo_count']

        if cast_dict.get('dir', None) != None:
            dirs = cast_dict['dir'][0]
        else:
            dirs = 'no result'

        return render_template('movie_page.html',
                               movie=movie,
                               zh_title=zh_title,
                               release_date=release_date,
                               runtime=runtime,
                               rating=general,
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
    except:
        return render_template('404.html'), 404

app.run(debug=True, port=8000, host="0.0.0.0")
