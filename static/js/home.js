

function movie_list(){
    var dataUrl= "/api/movielist"
    var xhr = new XMLHttpRequest()
    xhr.open('GET',dataUrl, true)
    xhr.send()
    
    
    xhr.onload = function(){
        if(xhr.status == 200){
            console.log('xhr.status', xhr.status);
            var data = JSON.parse(this.responseText);
            console.log(data);
            create_box(data);

        }
    }
}

movie_list()


function create_box(data){

//    a_outer > div_el > img

    var data_list = data['data'];
//    console.log('data_list', data_list)
    for (var i = 0; i < data_list.length; i++) {
        // data
        var movie = data_list[i];
//         console.log(movie);

        // a_outer
        var merge_id = movie['merge_id'];
        var a_outer = document.createElement('a');
        var page_url = "/title/" + merge_id
         a_outer.setAttribute('href', page_url);
         a_outer.setAttribute('target', "_blank");


        // div_el
        var div_el= document.createElement('div');
        div_el.classList.add('class', 'box');

        // img_el
        var image = movie['image'];
        var img_el = document.createElement('img');
        img_el.setAttribute('src', image);

        // assemble
        div_el.appendChild(img_el);
        a_outer.appendChild(div_el);

        document.getElementById('container').appendChild(a_outer);
    }
}

function filter_movie_list(year_min, year_max){
    var dataUrl= "/api/movielist/" + year_min + '/' +year_max
    var xhr = new XMLHttpRequest()
    xhr.open('GET',dataUrl, true)
    xhr.send()


    xhr.onload = function(){
        if(xhr.status == 200){
            console.log('xhr.status', xhr.status);
            var data = JSON.parse(this.responseText);
            console.log(data);
            create_box(data);

        }
    }
}




