function update_data_by_filter() {
    let markedCheckbox = document.getElementsByName('genre');
    let genreList = new Array();
    for (let checkbox of markedCheckbox) {
        if (checkbox.checked) {
            let checkboxValue = checkbox.value;
            genreList.push(checkboxValue);
        }
    }

    let beginYear = document.getElementById('range1').textContent;
    let endYear = document.getElementById('range2').textContent;
    // ajax request movie
    sendFilteredValue(beginYear, endYear, genreList);
}


function sendFilteredValue(yearMin, yearMax, genreList) {
    // send Ajax post to movie/filter api
    let letter = {
        'start': yearMin,
        'end': yearMax,
        'genres': JSON.stringify(genreList)
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
                create_box(data);
            } else {
                create_no_result();
            }
        }
    }
}
