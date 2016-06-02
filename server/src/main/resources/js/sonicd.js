var sonicd = (function() {

  function Client(sonicdAddress) {
    this.address = sonicdAddress;
  }

  function run(address, query, done, outputCb, progressCb, metadataCb) {
    var output = outputCb || (function() {});
    var progress = progressCb || (function() {});
    var metadata = metadataCb || (function() {});
    var isDone = false;
    var uri = address + '/v1/query';
    var ws = new WebSocket(uri);

    ws.onclose = function() {
      console.log('peer closed connection!');
      if (!isDone) {
        done(new Error('connection closed unexpectedly'));
      }
    };

    ws.onmessage = function(message) {
      console.log('recv msg: ');
      console.log(message);
      var msg = JSON.parse(message.data);

      switch (msg.event_type) {

        case 'P':
          progress(msg.payload);
          break;

        case 'D':
          isDone = true;
          if (msg.variation === 'success') {
            done();
          } else {
            done(new Error(JSON.stringify(msg.payload)));
          }
          break;

        case 'T':
          metadata(msg.payload.map(function(elem) {
            return [elem[0], typeof elem[1]];
          }));
          break;

        case 'O':
          output(msg.payload);
          break;
      }
    };

    ws.onopen = function() {
      var raw = JSON.stringify(query);
      ws.send(raw);
      console.log('sent query to server: ');
      console.log(query);
    };

    return ws;
  }

  Client.prototype.stream = function(query, done, output, progress, metadata) {

    this.client = run(this.address, query, done, output, progress, metadata);

  };

  Client.prototype.close = function() {

    this.client.close();

  };

  return {
    Client: Client
  }
})();
