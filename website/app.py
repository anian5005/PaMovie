import re
import json

from flask import request
from flask import Flask, render_template, redirect, url_for, send_from_directory

from local_package.db.mysql import fetch_movie_detail_dict, get_dashboard_data, MySQL


app = Flask(__name__)

@app.route('/')
def home():
    try:
        return render_template("home.html")
    except:
        return render_template('404.html'), 404


@app.route('/login', methods=['POST'])
def user_loader():
    sql = MySQL()
    engine, sql_conn = sql.get_connection()

    check_int_only = re.match(r'^([\s\d]+)$', request.form['pwd'])
    if check_int_only:
        condition = 'WHERE user ="guest" AND pwd="{}"'.format(request.form['pwd'])
        result = sql.fetch_data(sql_conn, 'dashboard_user', ['user'], condition, 'list')
        if len(result) != 0:
            return redirect(url_for('dashboard'))
        else:
            return render_template('error_login.html')
    else:
        return render_template('error_login.html')


@app.errorhandler(404)
def page_not_found():
    return render_template('404.html'), 404


@app.route('/title/<string:movie_id>')
def get_movie_detail_page(movie_id):
    basic_info = fetch_movie_detail_dict(movie_id)
    if basic_info:
        return render_template('movie_page.html', data=basic_info)
    else:
        return render_template('404.html'), 404


@app.route('/api/movie/filter', methods=['POST'])
def movie_filter():

    sort_col_mapping = {'imdb': 'imdb_rating', 'douban': 'douban_rating', 'tomato': 'tomatoes_rating', 'popular': 'imdb_votes'}
    request_value = json.loads(request.data)
    print('request_value', request_value)
    # {'start': '2020', 'end': '2022', 'genres': '["9","8","6","1","21","26","7","15","3","10","18","11","30"]'}

    start_year = request_value['start']
    end_year = request_value['end']
    display_movie_num = request_value['num']
    sort_value = request_value['sortOrder']
    sort_col_name = sort_col_mapping[sort_value]

    if request_value['genres'] == '[]':
        genres = tuple(range(1, 30))  # all genres
    else:
        genres = tuple(request_value['genres'])  # selected genre type

    sql = MySQL()
    #    WHERE movie_rating.imdb_rating IS NOT NULL
    engine, sql_conn = sql.get_connection()
    table = """(SELECT distinct start_year, imdb_id, primary_title, tw_name, img  FROM
    (SELECT start_year, T2.imdb_id, T2.primary_title, T2.tw_name, T2.img, T2.genre_id FROM
    (SELECT T1.*, movie_genre.genre_id FROM (SELECT start_year, imdb_id, primary_title, tw_name, img FROM movie.movie_info
    WHERE start_year BETWEEN {} AND {} AND img is not null) AS T1
    INNER JOIN movie.movie_genre
    ON movie.movie_genre.imdb_id = T1.imdb_id) AS T2
    INNER JOIN movie.genre_type
    ON T2.genre_id = movie.genre_type.genre_id
    WHERE genre_type.genre_id IN {}
    ) as T3) as T4
    INNER JOIN movie.movie_rating
    ON T4.imdb_id = movie_rating.imdb_id
    ORDER BY {} DESC
    """.format(start_year, end_year, genres, sort_col_name)
    # [{'imdb_id': 'tt0013274', 'primary_title': 'Istoriya grazhdanskoy voyny', 'tw_name': '俄國內戰史', 'img': '5078665_main.webp', 'genre_id': 8, 'zh_genre': '紀錄片'}..]

    columns = ['T4.*', 'start_year', 'movie_rating.imdb_rating', 'movie_rating.tomatoes_rating', 'movie_rating.douban_rating', 'imdb_votes', 'douban_votes']
    result_dict_list = sql.fetch_data(sql_conn, table, columns, '')
    print('result_dict_list', result_dict_list)
    json_obj_list = []
    for item in result_dict_list[: display_movie_num]:
        # serialize SqlAlchemy result to JSON
        movie_dict = dict(item._mapping)
        if movie_dict['tw_name'] is None:
            movie_dict['tw_name'] = movie_dict['primary_title']
        # convert decimal to string
        movie_dict['imdb_rating'] = str(movie_dict['imdb_rating'])
        movie_dict['douban_rating'] = str(movie_dict['douban_rating'])
        movie_dict.pop('primary_title')
        json_obj_list.append(json.dumps(movie_dict))
    data_dict = {'data': json_obj_list, 'total_num': len(result_dict_list)}

    sql_conn.close()
    engine.dispose()
    print('data_dict', data_dict)

    return data_dict


@app.route('/api/realtime_data', methods=['POST'])
def realtime_data():
    dashboard_data = get_dashboard_data()
    return dashboard_data


@app.route('/dashboard')
def dashboard():

    return render_template('dashboard.html')


@app.route('/robots.txt')
def static_from_root():
    return send_from_directory(app.static_folder, request.path[1:])


@app.route('/sitemap_index.xml')
def sitemap_from_root():
    return send_from_directory(app.static_folder, request.path[1:])


app.run(debug=True, port=8000, host="0.0.0.0")
