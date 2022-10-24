import re
import json

from flask import request
from flask import Flask, render_template, redirect, url_for, send_from_directory

from local_package.db.mysql import fetch_movie_detail_dict, get_dashboard_data, MySQL, home_page_filter


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
    # try:
    request_value = json.loads(request.data)
    data_dict = home_page_filter(request_value)
    return data_dict
    # except:
    #     return render_template('404.html'), 404


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
