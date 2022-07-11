
// make movie rating info
function create_imdb_rating(class_name, rating_name, value, count) {

    var outer_class_name = class_name + '_rating'

    //outer_div
    var div_el= document.createElement('div');
    div_el.classList.add(outer_class_name);

    //img
    var img_type = {'imdb':'imdb', 'tomato':'tomato', 'tomato_audience':'tomato', 'yahoo':'yahoo'};
    var img_src = '/static/img/' + img_type[class_name] + '_icon.png'
    var img= document.createElement('img');
    img.setAttribute('src', img_src);
    img.style.width = "100px";
    img.style.display = "inline";
    //add img
    div_el.appendChild(img);

    //rating name
    var p_rating_name= document.createElement('p');
    p_rating_name.classList.add('rating_name');
    var rating_name_text = document.createTextNode(rating_name);
    p_rating_name.appendChild(rating_name_text);
    // add rating name
    div_el.appendChild(p_rating_name);

    // value
    var base = {'imdb': '10', 'tomato':'100', 'tomato_audience':'100', 'yahoo':'5'};

    var p_value= document.createElement('p');
    p_value.classList.add('value');
    div_el.appendChild(p_value);
    var text = document.createTextNode(value + '/' + base[class_name]);
    // add value
    p_value.appendChild(text);
    div_el.appendChild(p_value);

    // count
    var p_count= document.createElement('p');

    // end is zero , then add '+'
    var count_str = count.toString();
    if (count_str.substr(-1) === '0' && count_str.length >1) {
        var count_text = document.createTextNode( count + '+' + ' Reviews');
    } else {
        var count_text = document.createTextNode( count + ' Reviews');
    }

    p_count.appendChild(count_text);
    // add count
    div_el.appendChild(p_count);

    // loading bar div
    var bar_type_dict = {'imdb':'energy_imdb', 'tomato':'energy_tomato', 'tomato_audience':'energy_tomato', 'yahoo':'energy_yahoo'}
    var bar_div = document.createElement('div');
    var bar_value = ((parseFloat(value)/parseFloat(base[class_name]))*100 ).toFixed(2);
    console.log('bar_value', bar_value);

    bar_div.classList.add('ldBar', 'label-center');
    bar_div.style.width = "300px";
    bar_div.style.height = "60px";
    bar_div.setAttribute('data-preset', bar_type_dict[class_name]);
    bar_div.setAttribute('data-value', bar_value);
    // add bar
    div_el.appendChild(bar_div);






    var position = document.getElementsByClassName('rating_container')[0];
    position.appendChild(div_el);



}

function movie_info(){
    merge_id = window.location.href.split('title/')[1]

    var dataUrl= "/api/info/" + merge_id;
    var xhr = new XMLHttpRequest();
    xhr.open('GET',dataUrl, true);
    xhr.send();


    xhr.onload = function(){
        if(xhr.status == 200){
            console.log('xhr.status', xhr.status);
            var data = JSON.parse(this.responseText);
            console.log('111', data);
//
//            var imdb_score = data['imdb_score']
//            var imdb_count = data['imdb_count']
//            var tomato_meter = data['tomato_meter']
//            var tomato_audience = data['tomato_audience']
//            var yahoo_score = data['yahoo_score']

        }
    }
}










