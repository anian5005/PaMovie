const range = () => {
    var inputLeft = document.getElementById("input-left");
    var inputRight = document.getElementById("input-right");

    var thumbLeft = document.querySelector(".slider.year > .thumb.left");
    var thumbRight = document.querySelector(".slider.year > .thumb.right");
    var range = document.querySelector(".slider.year > .range");
    var showeL = document.querySelector(".slider.year > .left-s");
    var showeR = document.querySelector(".slider.year > .right-s");

    function setLeftValue() {
      var _this = inputLeft,
        min = parseInt(_this.min),

        max = parseInt(_this.max);
//       console.log( _this.value);
//          console.log( _this.value);

      _this.value = Math.min(parseInt(_this.value), parseInt(inputRight.value) - 1);

      var percent = ((_this.value - min) / (max - min)) * 100;

      thumbLeft.style.left = percent + "%";
      range.style.left = percent + "%";

      showeL.style.left = percent + "%";
      showeL.innerHTML = _this.value;
    }
    setLeftValue();


    function setRightValue() {
      var _this = inputRight,
        min = parseInt(_this.min),
        max = parseInt(_this.max);

      _this.value = Math.max(parseInt(_this.value), parseInt(inputLeft.value) + 1);

      var percent = ((_this.value - min) / (max - min)) * 100;
      thumbRight.style.right = (100 - percent) + "%";
      range.style.right = (100 - percent) + "%";

      showeR.style.right = (100 - percent) + "%";
      showeR.innerHTML = _this.value;
    }
    setRightValue();

    inputLeft.addEventListener("input", setLeftValue);
    inputRight.addEventListener("input", setRightValue);

    inputLeft.addEventListener("mouseover", function() {
      thumbLeft.classList.add("hover");
    });
    inputLeft.addEventListener("mouseout", function() {
      thumbLeft.classList.remove("hover");
    });
    inputLeft.addEventListener("mousedown", function() {
      thumbLeft.classList.add("active");

    });
    inputLeft.addEventListener("mouseup", function() {
      thumbLeft.classList.remove("active");
      get_year_filter()
    });

    inputRight.addEventListener("mouseover", function() {
      thumbRight.classList.add("hover");
    });
    inputRight.addEventListener("mouseout", function() {
      thumbRight.classList.remove("hover");
    });
    inputRight.addEventListener("mousedown", function() {
      thumbRight.classList.add("active");

    });
    inputRight.addEventListener("mouseup", function() {
      thumbRight.classList.remove("active");
      get_year_filter()
    });
};

range();


const range_IMDB = () => {
    var inputLeft = document.getElementById("input-left_IMDB");
    var inputRight = document.getElementById("input-right_IMDB");

    var thumbLeft = document.querySelector(".multi-range-slider.IMDB > .slider.IMDB > .thumb.left");
    var thumbRight = document.querySelector(".multi-range-slider.IMDB > .slider.IMDB > .thumb.right");
    var range = document.querySelector(".multi-range-slider.IMDB > .slider.IMDB > .range");
    var showeL = document.querySelector(".multi-range-slider.IMDB > .slider.IMDB > .left-s");
    var showeR = document.querySelector(".multi-range-slider.IMDB > .slider.IMDB > .right-s");

    function setLeftValue() {
      var _this = inputLeft,
        min = parseInt(_this.min),

        max = parseInt(_this.max);
//       console.log( _this.value);
//          console.log( _this.value);

      _this.value = Math.min(parseInt(_this.value), parseInt(inputRight.value) - 1);

      var percent = ((_this.value - min) / (max - min)) * 100;

      thumbLeft.style.left = percent + "%";
      range.style.left = percent + "%";

      showeL.style.left = percent + "%";
      showeL.innerHTML = _this.value;
    }
    setLeftValue();

    function setRightValue() {
      var _this = inputRight,
        min = parseInt(_this.min),
        max = parseInt(_this.max);

      _this.value = Math.max(parseInt(_this.value), parseInt(inputLeft.value) + 1);

      var percent = ((_this.value - min) / (max - min)) * 100;
      thumbRight.style.right = (100 - percent) + "%";
      range.style.right = (100 - percent) + "%";

      showeR.style.right = (100 - percent) + "%";
      showeR.innerHTML = _this.value;
    }
    setRightValue();

    inputLeft.addEventListener("input", setLeftValue);
    inputRight.addEventListener("input", setRightValue);

    inputLeft.addEventListener("mouseover", function() {
      thumbLeft.classList.add("hover");
    });
    inputLeft.addEventListener("mouseout", function() {
      thumbLeft.classList.remove("hover");
    });
    inputLeft.addEventListener("mousedown", function() {
      thumbLeft.classList.add("active");
    });
    inputLeft.addEventListener("mouseup", function() {
      thumbLeft.classList.remove("active");
    });

    inputRight.addEventListener("mouseover", function() {
      thumbRight.classList.add("hover");
    });
    inputRight.addEventListener("mouseout", function() {
      thumbRight.classList.remove("hover");
    });
    inputRight.addEventListener("mousedown", function() {
      thumbRight.classList.add("active");
    });
    inputRight.addEventListener("mouseup", function() {
      thumbRight.classList.remove("active");
    });
};

range_IMDB();
