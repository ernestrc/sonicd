'use strict';

var graph = (function() {

  var TAIL = {
    "path" : "/var/log/opentok",
    "class" : "JsonStreamSource",
    "tail" : true
  }

  var SYNTHETIC = {
    "class" : "SyntheticSource",
    "size" : 1000
  }

  function getQuery() {
    //TODO
    return {};
  }

  function tail() {

    var q = getQuery();

    var query = {
      query: JSON.stringify(q),
      config: TAIL
      //config: SYNTHETIC
    }

    var client = new sonicd.Client('ws://localhost:9111');

    function done(err) {
      if (err) {
        stop(err);
        return;
      }

      stop();
    }

    function output(elems) {
      var cols = elems.map(function(el) {
        if (typeof el === 'object') {
          return "<th>" + JSON.stringify(el) + "</th>";
        } else {
          return "<th>" + el + "</th>";
        }
      });
      $('#data').append("<tr>" + cols + "</tr>");
    }

    function progress(prog) {
      console.log(prog);
    }

    function metadata(meta) {
      $('#data').empty();
      var metaCol = meta.map(function(el) {
        return "<th>" + el[0] + "</th>";
      });
      $('#data').append("<tr>" + metaCol + "</tr>");
      console.log(meta);
    }

    client.stream(query, done, output, progress, metadata);

  }

  function log(m) {
    $('#logs').append(m);
  }

  function stop(err) {
    if (err) {
      console.error(err);
      log(err);
      $('#content').removeClass('panel-default');
      $('#content').addClass('panel-danger');
    }
    $("#cat").removeClass("disabled");
    $("#tail").removeClass("disabled");
    $("#stop").addClass("disabled");
  }

  function start(fn) {
    $('#content').removeClass('');
    $("#cat").addClass("disabled");
    $("#tail").addClass("disabled");
    $("#stop").removeClass("disabled");
    $('#content').removeClass('panel-danger');
    $('#content').addClass('panel-default');
    fn();
  }

  function init() {
    console.log('initialising timeline...');
    var data = [
      {id: 1, content: 'item 1', start: '2013-04-20'},
      {id: 2, content: 'item 2', start: '2013-04-14'},
      {id: 3, content: 'item 3', start: '2013-04-18'},
      {id: 4, content: 'item 4', start: '2013-04-16', end: '2013-04-19'},
      {id: 5, content: 'item 5', start: '2013-04-25'},
      {id: 6, content: 'item 6', start: '2013-04-27'}
    ];

    // DOM element where the Timeline will be attached
    var container = document.getElementById('visualization');

    // Create a DataSet (allows two way data-binding)
    var items = new vis.DataSet(data);

    // Configuration for the Timeline
    var options = {};

    // Create a Timeline
    var timeline = new vis.Timeline(container, items, options);

    //attach flush and re-init

    //attach click listeners on menu
    $("#cat").click(function() {
      start(cat);
    });
    $("#tail").click(function() {
      start(tail);
    });
    $("#stop").click(function() {
      stop();
    });
  };

  return {
    init: init,
    tail: tail,
    stop: stop,
    cat: cat
  }
})();
