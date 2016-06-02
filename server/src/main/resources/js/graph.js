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

  var currentId = 0;

  function buildItem(attempt, success, idxMdc, idxTs, idxType) {
    currentId += 1;
    console.log(attempt);
    return {
      id: currentId,
      content: attempt[idxMdc].callType,
      className: attempt[idxType],
      group: attempt[idxMdc].traceId,
      type: 'range',
      title: attempt[idxMdc].callType,
      start: moment(attempt[idxTs]),
      end: moment(success[idxTs]),
    }
  }

  function tail(timeline, dataSet, groups) {

    var q = getQuery();
    var meta = [];
    var data = {};
    var ids = [];

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

    function output(row) {
      var cols = row.map(function(el) {
        if (typeof el === 'object') {
          return "<th>" + JSON.stringify(el) + "</th>";
        } else {
          return "<th>" + el + "</th>";
        }
      });
      $('#data').append("<tr>" + cols + "</tr>");

      //if variation is success, find attempt in data
      //else append to data
      var idxMdc = meta.indexOf('mdc');
      var d = row[idxMdc];
      if (d && d.traceId) {
        if (ids.indexOf(d.traceId) < 0) {
          ids.push(d.traceId);
          groups.add({id: d.traceId, content: d.traceId});
        }
        if (d.variation == 'Success') {
          var ev = data[d.traceId];
          if (!ev)  {
            console.error('received success before any other event: ' + JSON.stringify(row));
          } else {
            //find one with same call_type and variation == attempt
            var filtered = ev.filter(function(l) {
              return d.callType == l[idxMdc].callType && l[idxMdc].variation == 'Attempt';
            });

            if (filtered.length == 0) {
              console.error('could not map success to attempt' + JSON.stringify(row));
            } else {
              var attempt = filtered[0];
              var success = row;
              var item = buildItem(attempt, success, idxMdc, meta.indexOf('@timestamp'), meta.indexOf('logType'));
              dataSet.add(item);
              var range = timeline.getItemRange();
              timeline.setWindow(range.min, range.max);
            }
          }
        } else {
          if (!data[d.traceId]) {
            data[d.traceId] = [row];
          } else {
            data[d.traceId].push(row);
          }
        }
      } else {
        console.log('traceId undefined:')
        console.log(d);
      }
    }

    function progress(prog) {
      console.log(prog);
    }

    function metadata(metaRow) {
      $('#data').empty();
      var metaCol = metaRow.map(function(el) {
        meta.push(el[0]);
        return "<th>" + el[0] + "</th>";
      });
      $('#data').append("<tr>" + metaCol + "</tr>");
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
    $("#clear").removeClass("disabled");
    $("#cat").removeClass("disabled");
    $("#tail").removeClass("disabled");
    $("#stop").addClass("disabled");
  }

  function start(fn, timeline, dataSet,groups) {
    $('#content').removeClass('');
    $("#cat").addClass("disabled");
    $("#tail").addClass("disabled");
    $("#stop").removeClass("disabled");
    $("#clear").addClass("disabled");
    $('#content').removeClass('panel-danger');
    $('#content').addClass('panel-default');
    fn(timeline, dataSet, groups);
  }

  function init() {
    console.log('initialising timeline...');

    // DOM element where the Timeline will be attached
    var container = document.getElementById('visualization');

    // Create a DataSet (allows two way data-binding)
    var dataSet = new vis.DataSet();
    var groups = new vis.DataSet();

    // Configuration for the Timeline
    var options = {
      showCurrentTime: false 
    };

    // Create a Timeline
    var timeline = new vis.Timeline(container, dataSet, groups, options);

    $("#clear").click(function(e) {
      e.preventDefault();
      dataSet.clear();
      groups.clear();
    });
    //attach click listeners on menu
    $("#tail").click(function(e) {
      e.preventDefault();
      start(tail, timeline, dataSet, groups);
    });
    $("#stop").click(function(e) {
      e.preventDefault();
      stop();
    });
  };

  return {
    init: init,
    tail: tail,
    stop: stop
  }
})();
