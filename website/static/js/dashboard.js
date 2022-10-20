function sendDashboardValue() {
    // send Ajax post to movie/filter api
    let letter = {};
    let xhr = new XMLHttpRequest(); // new HttpRequest instance
    xhr.open("POST", "/api/realtime_data");
    xhr.setRequestHeader("Content-Type", "application/json");
    let data = JSON.stringify(letter)
    xhr.send(data);

    // get server response
    xhr.onload = function() {
        if (xhr.status == 200) {
            let data = JSON.parse(this.responseText);
            chartConversionRate(data);
            imdbNewMovieCount(data);
            doubanNewMovieCount(data);
            tomatoNewMovieCount(data);
            totalHeader(data);
            imdbRatingCount(data);
            tomatoRatingCount(data);
            doubanRatingCount(data);
            ratingSpeed(data);
            idFunnelChart(data);
            dataSpeed(data);
            createTableResult(data);
        }
    }
}



function createTableResult(data) {
    let data_date_list = data['date_list'];
    let dataResultList = data['data_table_result'];
    let ratingResultList = data['rating_table_result'];

    for (const row of dataResultList) {
        let tr = document.createElement('tr');


        for (const value of row) {
            let th = document.createElement('th');
            th.innerHTML = value;
            tr.appendChild(th);
        }
        document.getElementById('movie_data_table').appendChild(tr);
    };

    for (const row of ratingResultList) {
        let tr = document.createElement('tr');


        for (const value of row) {
            let th = document.createElement('th');
            th.innerHTML = value;
            tr.appendChild(th);
        }
        document.getElementById('movie_rating_table').appendChild(tr);
    };

}

function idFunnelChart(data) {
    let id_current_count_list = data['id_current_count'];
    let gd = document.getElementById('myDiv');
    let data_list = [{
        type: 'funnel',
        y: ["IMDb", "豆瓣電影", "爛番茄"],
        x: id_current_count_list,
        hoverinfo: 'x+percent previous+percent initial',
        marker: {
            color: ["f5c518", "00B51D", "fa320a"]
        }
    }];


    let layout = {
        margin: {
            l: 150
        },
        width: 600,
        height: 500,
        title: '當前電影 ID 數量比較'
    };

    Plotly.newPlot('idFunnelChart', data_list, layout);

}

function totalHeader(data) {
    date_dict = data['total_header'];
    document.getElementById('total_header_data').innerHTML = date_dict['data_count'];

    document.getElementById('total_header_img').innerHTML = date_dict['movie_poster'];

    document.getElementById('total_header_rating').innerHTML = date_dict['movie_rating_count'];

    document.getElementById('total_header_data_speed').innerHTML = date_dict['data_time'] + '秒/ 筆';

    document.getElementById('total_header_rating_speed').innerHTML = date_dict['rating_time'] + '秒/ 筆';
}

function chartConversionRate(data) {
    date_list = data['date_list']
    imdb_to_douban_list = data['conversion_rate']['imdb_douban']
    douban_to_tomato_list = data['conversion_rate']['douban_tomato']

    let trace1 = {
        x: date_list,
        y: imdb_to_douban_list,
        mode: 'lines',
        name: 'IMDb — 豆瓣之 ID 轉換率',
        line: {
            color: '#fa320a',
            dash: 'solid',
            width: 4
        }
    };

    let trace2 = {
        x: date_list,
        y: douban_to_tomato_list,
        mode: 'lines',
        name: '豆瓣 — 爛番茄之 ID 轉換率',
        line: {
            color: '#00B51D',
            dash: 'solid',
            width: 4
        }
    };


    let data_list = [trace1, trace2];

    let layout = {
        title: '電影 ID mapping 轉換率',
        xaxis: {
            range: date_list,
            autorange: false
        },
        yaxis: {
            tickformat: ',.2%',
            range: [0, 0.4],
            autorange: false
        },
        legend: {
            y: 0.5,
            traceorder: 'reversed',
            font: {
                size: 16
            }
        }
    };

    Plotly.newPlot('ConversionRate', data_list, layout);
}

function imdbNewMovieCount(data) {
    date_list = data['date_list']
    imdb_new_add_count_list = data['new_add_count']['imdb_new_list']

    let trace1 = {
        type: 'bar',
        x: date_list,
        y: imdb_new_add_count_list,
        marker: {
            color: 'f5c518',
            line: {
                width: 2
            }
        }
    };

    let data_list = [trace1];

    let layout = {
        title: 'IMDb 之電影新增數量',
        width: 500,
        font: {
            size: 14
        },
        xaxis: {
            range: date_list
        }
    };

    let config = {
        responsive: true
    }

    Plotly.newPlot('imdbNewMovie', data_list, layout, config);
}

function doubanNewMovieCount(data) {
    date_list = data['date_list']
    douban_new_add_count_list = data['new_add_count']['douban_new_list']

    let trace1 = {
        type: 'bar',
        x: date_list,
        y: douban_new_add_count_list,
        marker: {
            color: '#00B51D',
            line: {
                width: 2
            }
        }
    };

    let data_list = [trace1];

    let layout = {
        title: '豆瓣之電影新增數量',
        width: 500,
        font: {
            size: 14
        },
        xaxis: {
            range: date_list
        }
    };

    let config = {
        responsive: true
    }

    Plotly.newPlot('doubanNewMovie', data_list, layout, config);
}

function tomatoNewMovieCount(data) {
    date_list = data['date_list'];
    tomato_new_list = data['new_add_count']['tomato_new_list'];

    let trace1 = {
        type: 'bar',
        x: date_list,
        y: tomato_new_list,
        marker: {
            color: '#fa320a',
            line: {
                width: 2
            }
        }
    };

    let data_list = [trace1];

    let layout = {
        title: '爛番茄之電影新增數量',
        width: 500,
        font: {
            size: 14
        },
        xaxis: {
            range: date_list
        }
    };

    let config = {
        responsive: true
    }

    Plotly.newPlot('tomatoNewMovie', data_list, layout, config);
}

function tomatoRatingCount(data) {

    date_list = data['rating_date'];
    tomato_rating_count = data['tomato_rating_count'];
    tomato_omdb_count_list = tomato_rating_count['tomato_omdb_count_list']
    tomato_rating_successfully_count = tomato_rating_count['tomato_rating_successfully_count_list'],
        tomato_no_rating_count = tomato_rating_count['tomato_no_rating_count_list']
    rotten_tomatoes_error_count = tomato_rating_count['rotten_tomatoes_error_count_list']

    let trace1 = {
        x: date_list,
        y: tomato_omdb_count_list,
        name: 'OMDb API - 成功更新',
        type: 'bar'
    };

    let trace2 = {
        x: date_list,
        y: tomato_rating_successfully_count,
        name: '爬蟲 - 成功更新 - 有評分',
        type: 'bar'
    };

    let trace3 = {
        x: date_list,
        y: tomato_no_rating_count,
        name: '爬蟲 - 成功更新 - 網頁無評分',
        type: 'bar'
    };

    let trace4 = {
        x: date_list,
        y: rotten_tomatoes_error_count,
        name: '爬蟲 - 錯誤',
        type: 'bar'
    };

    let bar_data_list = [trace1, trace2, trace3, trace4];

    let layout = {
        title: '爛番茄之電影評分更新數量',
        barmode: 'stack'
    };

    Plotly.newPlot('tomatoRatingCount', bar_data_list, layout);
}

function imdbRatingCount(data) {

    date_list = data['rating_date'];
    imdb_rating_count = data['imdb_rating_count'];
    imdb_updated_successfully_count = imdb_rating_count['imdb_updated_count'];
    imdb_no_rating_count = imdb_rating_count['imdb_no_rating_count'];

    let trace1 = {
        x: date_list,
        y: imdb_updated_successfully_count,
        name: '評分成功更新-有評分',
        type: 'bar'
    };

    let trace2 = {
        x: date_list,
        y: imdb_no_rating_count,
        name: '評分成功更新-網頁無評分',
        type: 'bar'
    };

    let bar_data_list = [trace1, trace2];

    let layout = {
        title: 'IMDb 之電影評分更新數量',
        barmode: 'stack'
    };

    Plotly.newPlot('imdbRatingCount', bar_data_list, layout);
}


function doubanRatingCount(data) {

    date_list = data['rating_date'];
    douban_rating_count = data['douban_rating_count'];
    douban_updated_successfully_count = douban_rating_count['douban_rating_successfully_count'];
    douban_no_rating_count = douban_rating_count['douban_no_rating_count'];
    douban_error_count = douban_rating_count['douban_error_count'];
    douban_block_count = douban_rating_count['douban_block_count']

    let trace1 = {
        x: date_list,
        y: douban_updated_successfully_count,
        name: '爬蟲成功 - 有評分',
        type: 'bar'
    };

    let trace2 = {
        x: date_list,
        y: douban_no_rating_count,
        name: '爬蟲成功 - 網頁無評分',
        type: 'bar'
    };

    let trace3 = {
        x: date_list,
        y: douban_error_count,
        name: '爬蟲 - 發生錯誤',
        type: 'bar'
    };

    let trace4 = {
        x: date_list,
        y: douban_block_count,
        name: '爬蟲 - 發生錯誤 - 反爬蟲',
        type: 'bar'
    };

    let bar_data_list = [trace1, trace2, trace3, trace4];

    let layout = {
        title: '豆瓣之電影評分更新數量',
        barmode: 'stack',
        xaxis: {
            range: date_list
        }
    };

    Plotly.newPlot('doubanRatingCount', bar_data_list, layout);
}


function ratingSpeed(data) {
    let date_list = data['rating_date'];
    let douban_rating_speed = data['douan_rating_speed'];
    let tomato_rating_speed = data['tomato_rating_speed'];

    let trace1 = {
        type: "scatter",
        mode: "lines",
        x: date_list,
        y: douban_rating_speed,
        line: {
            color: '#00B51D'
        },
        mode: 'lines',
        name: '豆瓣評分爬取時間（秒/筆）'
    }

    let trace2 = {
        type: "scatter",
        mode: "lines",
        x: date_list,
        y: tomato_rating_speed,
        line: {
            color: '#fa320a'
        },
        name: '爛番茄評分爬取時間（秒/筆）'
    }

    let data_list = [trace1, trace2];

    let layout = {
        title: '電影評分爬取時間比較',
        xaxis: {
            range: date_list,
            type: 'date'
        },
        yaxis: {
            autorange: true,
            type: 'linear'
        }
    };

    Plotly.newPlot('RatingSpeed', data_list, layout);
}

function dataSpeed(data) {
    let date_list = data['date_list'];
    let data_dict = data['movie_data_speed_dict'];
    let imdb_data_update_speed = data_dict['imdb_data_update_speed'];
    let douban_data_update_speed = data_dict['douban_data_update_speed'];
    let tomato_id_update_time_speed = data_dict['tomato_id_update_time_speed'];

    let trace1 = {
        type: "scatter",
        mode: "lines",
        x: date_list,
        y: douban_data_update_speed,
        line: {
            color: '#00B51D'
        },
        mode: 'lines',
        name: '豆瓣資料爬取時間（秒/筆）'
    }

    let trace2 = {
        type: "scatter",
        mode: "lines",
        x: date_list,
        y: tomato_id_update_time_speed,
        line: {
            color: '#fa320a'
        },
        name: '爛番茄電影 ID 爬取時間（秒/筆）'
    }

    let trace3 = {
        type: "scatter",
        mode: "lines",
        x: date_list,
        y: imdb_data_update_speed,
        line: {
            color: '#f5c518'
        },
        name: 'IMDb 資料爬取時間（秒/筆）'
    }

    let data_list = [trace1, trace2, trace3];

    let layout = {
        title: '電影資料爬取時間比較',
        xaxis: {
            range: date_list,
            type: 'date'
        },
        yaxis: {
            type: 'linear'
        }
    };

    Plotly.newPlot('movieDataSpeed', data_list, layout);
}
sendDashboardValue()
