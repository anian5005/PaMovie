console.log('home js')
function clear_box() {
        var node = document.getElementById('container');
        node.remove();

        var div_el= document.createElement('div');
        div_el.classList.add('class', 'container');
        div_el.setAttribute("id", 'container');

        var parent = document.getElementsByClassName('article')[0];
        parent.appendChild(div_el);
}


//home page default movie
function movie_list(){
    var dataUrl= "/api/movielist"
    var xhr = new XMLHttpRequest()
    xhr.open('GET',dataUrl, true)
    xhr.send()

    xhr.onload = function(){
        if(xhr.status == 200){
            console.log('xhr.status', xhr.status);
            var data = JSON.parse(this.responseText);
//            console.log(data);
            create_box(data);

        }
    }
}

movie_list()


function create_box(data){

//    a_outer > div_el > img
//        console.log('data', data);
        var data_list = data['data'];
//        console.log('data_list', data_list);
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

function create_no_result(){
    var result_msg = document.createElement('h3');
    var text = document.createTextNode("Sorry, no results found for this range");
    result_msg.appendChild(text);
    document.getElementById('container').appendChild(result_msg);
}

function update_data_by_slider(year_min, year_max){
    var dataUrl= "/api/movielist/" + year_min + '/' +year_max
    var xhr = new XMLHttpRequest()
    xhr.open('GET',dataUrl, true)
    xhr.send()


    xhr.onload = function(){
        if(xhr.status == 200){
            console.log('xhr.status', xhr.status);
            var data = JSON.parse(this.responseText);
            clear_box();
            console.log('create box >> data', data);
            console.log(typeof data['data'])

            if (data['data'] !== null) {
                create_box(data);
            } else {
                create_no_result()
                console.log('no data');
            }
        }
    }
}




