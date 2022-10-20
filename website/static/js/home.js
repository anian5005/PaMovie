function tagFilterAllTypeButton(displayMovieCount = null) {
    // new filter result
    if (displayMovieCount === null) {
        displayMovieCount = 10;
    }
    let genreList = new Array();
    let beginYear = document.getElementById('range1').textContent;
    let endYear = document.getElementById('range2').textContent;
    let sortOrder = document.querySelector('input[name="rating"]:checked').value;
    // ajax request movie
    sendFilteredValue(beginYear, endYear, [], sortOrder, displayMovieCount);
};


function tagFilterMutualExclusivity() {
    let allTypeElem = document.getElementsByName('genre all')[0];
    let checkboxes = document.getElementsByName('genre name');

    checkboxes.forEach(function(checkbox) {
        checkbox.addEventListener('change', function() {
            document.getElementsByName('genre all')[0].checked = false;

        })
    });

    allTypeElem.addEventListener('change', function() {
        checkboxes.forEach(function(checkbox) {
            checkbox.checked = false;
        })
    });
};


function updateMovieByFilter(displayMovieCount = null) {
    // new filter result
    if (displayMovieCount === null) {
        displayMovieCount = 10;
    }

    let markedCheckbox = document.getElementsByName('genre name');
    let genreList = new Array();
    for (let checkbox of markedCheckbox) {
        if (checkbox.checked) {
            let checkboxValue = checkbox.value;
            genreList.push(checkboxValue);
        };
    };
    // year range
    let beginYear = document.getElementById('range1').textContent;
    let endYear = document.getElementById('range2').textContent;
    // sorted by
    let sortOrder = document.querySelector('input[name="rating"]:checked').value;
    // ajax request movie
    sendFilteredValue(beginYear, endYear, genreList, sortOrder, displayMovieCount);
}


function sendFilteredValue(yearMin, yearMax, genreList, sortOrder, displayMovieCount) {
    // send Ajax post to movie/filter api
    let letter = {
        'start': yearMin,
        'end': yearMax,
        'genres': JSON.stringify(genreList),
        'num': displayMovieCount,
        'sortOrder': sortOrder
    };
    let xhr = new XMLHttpRequest(); // new HttpRequest instance
    xhr.open("POST", "/api/movie/filter");
    xhr.setRequestHeader("Content-Type", "application/json");
    let data = JSON.stringify(letter)
    xhr.send(data);

    // get server response
    xhr.onload = function() {
        if (xhr.status == 200) {
            let data = JSON.parse(this.responseText);
            clear_box();
            if (data['data'] !== null) {
                let totalMovieResult = data['total_num'];
                create_box(data);
                displayPage(displayMovieCount, totalMovieResult);
            } else {
                create_no_result();
            }
        }
    }
}


function clear_box() {
    let node = document.getElementById('container');
    node.remove();
    let div_el = document.createElement('div');
    div_el.classList.add('box_container');
    div_el.setAttribute("id", 'container');

    let parent = document.getElementsByClassName('article')[0];
    parent.appendChild(div_el);
}


function create_box(data) {
    let totalMovieNum = data['total_num'];
    const cardTotalElem = document.getElementById("card-total");
    cardTotalElem.innerHTML = totalMovieNum;

    //    card > a_outer ,  div_el (class="box") ,  img , div_title (class=""), info
    let data_list = data['data'];
    for (let i = 0; i < data_list.length; i++) {
        let movie = JSON.parse(data_list[i]);
        // {"imdb_id": "tt10633456", "primary_title": "Minari", "tw_name": "\u5922\u60f3\u4e4b\u5730", "img": "34463995_main.webp", 'douban_rating':6.1, 'imdb_rating': 6.6, 'tomatoes_rating': 62}


        // div (class="card")
        let card = document.createElement('div');
        card.classList.add('card');

        // a_outer
        let merge_id = movie['imdb_id'];
        let a_outer = document.createElement('a');
        a_outer.classList.add('link');
        let page_url = "/title/" + merge_id;
        a_outer.setAttribute('href', page_url);
        a_outer.setAttribute('target', "_blank");

        // div_el
        let div_el = document.createElement('div');
        div_el.classList.add('box');

        // img_el
        if (movie['img'] == '0' || movie['img'] == null) {
            movie['img'] = "movie_default_large.png";
        }
        let image = 'https://s3-summer.s3.ap-southeast-1.amazonaws.com/movie/poster/' + movie['img'];
        let img_el = document.createElement('img');
        img_el.classList.add('poster');
        img_el.setAttribute('src', image);

        //card_info
        imdbRating = movie['imdb_rating'];
        doubanRating = movie['douban_rating'];
        tomatoRating = movie['tomatoes_rating'];
        let cardInfo = document.createElement('div');
        cardInfo.classList.add('info');

        let detail = document.createElement('div');
        detail.classList.add('detail');


        //imdb
        if (movie['imdb_rating'] != null) {
            let imdb = document.createElement('div');
            imdb.classList.add('rating_row');

            let imdbText = document.createElement('h5');
            imdbText.innerHTML = imdbRating + "  / 10";

            let imdbImage = document.createElement('img');
            let imdbImageUrl = "/static/img/imdb_small_icon.jpg";
            imdbImage.classList.add('rating_icon');
            imdbImage.setAttribute('src', imdbImageUrl);

            imdb.appendChild(imdbImage);
            imdb.appendChild(imdbText);
            detail.appendChild(imdb);
        }

        //douban
        if (movie['douban_rating'] != null) {
            let douban = document.createElement('div');
            douban.classList.add('rating_row');

            let doubanText = document.createElement('h5');
            doubanText.innerHTML = doubanRating + "  / 10";

            let doubanImage = document.createElement('img');
            let doubanImageUrl = "/static/img/douban_icon.jpg";
            doubanImage.classList.add('rating_icon');
            doubanImage.setAttribute('src', doubanImageUrl);

            douban.appendChild(doubanImage);
            douban.appendChild(doubanText);
            detail.appendChild(douban);
        }

        //tomato
        if (movie['tomatoes_rating'] != null) {
            let tomato = document.createElement('div');
            tomato.classList.add('rating_row');

            let tomatoText = document.createElement('h5');
            tomatoText.innerHTML = tomatoRating + "  / 100";

            let tomatoImage = document.createElement('img');
            let tomatoImageUrl = "/static/img/tomato_small_icon.jpg";
            tomatoImage.classList.add('rating_icon');
            tomatoImage.setAttribute('src', tomatoImageUrl);

            tomato.appendChild(tomatoImage);
            tomato.appendChild(tomatoText);
            detail.appendChild(tomato);
        }

        if (movie['imdb_rating'] === null && movie['douban_rating'] === null && movie['tomatoes_rating'] === null) {
            let h5 = document.createElement('h5');
            h5.innerHTML = '&nbsp; &nbsp;No Movie Rating';
            detail.appendChild(h5);
        };

        // title:  div (class="title") > h5 > a > title text
        let titleText = movie['tw_name']
        if (movie['tw_name'] == null) {
            titleText = movie['primary_title']
        }
        let divTitle = document.createElement('div');
        divTitle.classList.add('box_title');
        let h5 = document.createElement('h5');
        h5.classList.add('text-overflow');
        let aTitle = document.createElement('a');
        aTitle.innerHTML = titleText;
        aTitle.setAttribute('href', page_url);
        h5.appendChild(aTitle);
        divTitle.appendChild(h5);

        let title_el = document.createElement('p');

        // assemble
        card.appendChild(a_outer);
        card.appendChild(img_el);
        cardInfo.appendChild(detail);
        card.appendChild(cardInfo);

        div_el.appendChild(card);
        div_el.appendChild(divTitle);

        document.getElementById('container').appendChild(div_el);
    }

}


function create_no_result() {
    let result_msg = document.createElement('h3');
    let text = document.createTextNode("Sorry, no results found for this range");
    result_msg.appendChild(text);
    document.getElementById('container').appendChild(result_msg);
}


function displayPage(displayCount, totalCount) {
    const surplus = totalCount - displayCount;
    if (surplus >= 0) {
        document.getElementById("card-total").textContent = totalCount;
        document.getElementById("card-count").textContent = displayCount;
    } else {
        document.getElementById("card-total").textContent = totalCount;
        document.getElementById("card-count").textContent = totalCount;
    };

};


function loadMoreButton() {
    const cardIncrease = 10;
    const cardCountElem = document.getElementById("card-count");
    const currenCount = Number(cardCountElem.textContent);
    const loadMoreButton = document.getElementById("load-more");
    const cardTotal = Number(document.getElementById("card-total").textContent);

    const currenPage = currenCount / cardIncrease;
    const totalPage = Math.ceil(cardTotal / cardIncrease);

    const handleButtonStatus = () => {
        if (currenPage >= totalPage) {
            loadMoreButton.classList.add("disabled");
            loadMoreButton.setAttribute("disabled", true);
        }
    };

    const surplus = cardTotal - currenCount;

    if (surplus === 0) {
        handleButtonStatus();
    } else if (surplus > 0 && surplus < 10) {
        updateMovieByFilter(displayMovieCount = cardTotal);
        document.getElementById("card-count").textContent = cardTotal;
    } else {
        updateMovieByFilter(displayMovieCount = currenCount + cardIncrease);
        document.getElementById("card-count").textContent = currenCount + cardIncrease;
    };
};

//// When the user clicks on the button, scroll to the top of the document
function goToTop() {
    document.body.scrollTop = 0; // For Safari
    document.documentElement.scrollTop = 0; // For Chrome, Firefox, IE and Opera
}
