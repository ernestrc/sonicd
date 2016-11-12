#!/usr/bin/env node

const docopt = require('docopt').docopt;
const log = require('winston');
const kafka = require('kafka-node');
const microtime = require('microtime');
const assert = require('assert');
const sonic = require('sonic-js');
const median = require('median');

const Producer = kafka.Producer;
const Consumer = kafka.Consumer;
// const KeyedMessage = kafka.KeyedMessage;

const version = require('./package.json').version;

const DOC = `
Utility to measure the latency between a Kafka Sonicd client and a direct Kafka consumer

Usage:
    kafka-sonic latency <rate> [options]
    kafka-sonic -h | --help | --version

Arguments:
    rate                    Producer rate (Max is 1000)

Options:
    -k, --kafka=<addr>      Kafka address [default: localhost:9092]
    -z, --zookeeper=<addr>  Zookeeper address [default: localhost:2181]
    -s, --sonicd=<addr>     Sonicd ws address [default: localhost:9111]
    -p, --message=<file>    JSON file to push as message
    --no-parser              Disable Sonicd JSON parsing
    --verbose               Turn on debug log level
`;

const options = docopt(DOC, { version: version });

log.level = options['--verbose'] ? 'debug' : 'info';
log.cli();

log.debug('log level set to %s', log.level);
log.debug('parsed options', options);

const kurl = options['--kafka'];
const zurl = options['--zookeeper'];
const surl = options['--sonicd'];
const disableParsing = options['--no-parser'];

// const GROUP_ID = `sonicd_latency_group_${microtime.now()}`;
const GROUP_ID = 'sonicd_latency_group';
const TOPIC = 'sonicd_latency';
const SONICD_CONFIG = {
  class: 'KafkaSource',
  'key-deserializer': 'StringDeserializer',
  'value-deserializer': 'StringDeserializer',
  'ignore-parsing-errors': 100,
  settings: {
    group: {
      id: GROUP_ID
    },
    bootstrap: {
      servers: kurl
    }
  },
};

if (!disableParsing) {
  log.info('enabled KafkaSource$JSONParser in Sonicd');
  SONICD_CONFIG['value-json-format'] = 'build.unstable.sonicd.source.KafkaSource$JSONParser';
} else {
  log.info('disabled KafkaSource$JSONParser in Sonicd');
}

const SONICD_QUERY = {
  query: JSON.stringify({
    topic: TOPIC,
    partition: 0
  }),
  config: {
    config: {
      config: {
        config: SONICD_CONFIG,
        class: 'SonicSource',
        host: '0.0.0.0',
        port: 10001
      },
      class: 'SonicSource',
      host: '0.0.0.0',
      port: 10001
    },
    class: 'SonicSource',
    host: '0.0.0.0',
    port: 10001
  }
};

const rate = parseInt(options['<rate>'], 10);
assert(rate <= 1000);
const delay = 1000 / rate;

const consumeropt = {
  // Consumer group id, default `kafka-node-group`
  groupId: GROUP_ID,
  // Optional consumer id, defaults to groupId + uuid
  id: 'sonicd_latency_test',
  // Auto commit config
  autoCommit: true,
  autoCommitIntervalMs: 500,
  // The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued, default 100ms
  fetchMaxWaitMs: 100,
  // This is the minimum number of bytes of messages that must be available to give a response, default 1 byte
  fetchMinBytes: 1,
  // The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
  fetchMaxBytes: 1024 * 1024,
  // If set true, consumer will fetch message from the given offset in the payloads
  fromOffset: false,
  // If set to 'buffer', values will be returned as raw buffer objects.
  encoding: 'utf8'
};

const template = options['--message'] ? require(options['--message']) : { id: null, ts: null };

const producerpay = [{
  topic: TOPIC,
  messages: template,
  key: 'k', // only needed when using keyed partitioner
  partition: 0, // default 0
  attributes: 2 // default: 0
}];
const consumerpay = [{ topic: TOPIC, partition: 0 }];

const kclient = new kafka.Client(zurl);
const sclient = new sonic.Client(`ws://${surl}/v1/query`);

const producer = new Producer(kclient);

let id = 0;
let meta = {};

// { <id>: { 'p1': <tsproducer1>, 'p2': <tsproducer2>, 'c': <tsconsumer>, 's': <tssonicd> }};
const results = {};

function startProducing() {
  log.info('producer will send messages at %s/s (delay is %s)', rate, delay);

  setInterval(() => {
    let ts = microtime.now();
    template.id = id;
    template.ts = ts;
    results[id] = { p1: ts, c: null, s: null };
    producerpay[0].messages = JSON.stringify(template);
    producer.send(producerpay, (err, data) => {
      if (err) {
        log.error('error producing message %s', err);
        process.exit(1);
        return;
      }
      ts = microtime.now();
      log.debug('successfully produced message with id %s: %j', id, template);
    });
    id += 1;
  }, delay);
}

function startMeasuring() {
  // TODO drain results
  setInterval(() => {
    const sums = Object.keys(results).reduce((acc, i) => {
      const result = results[i];
      if (result.c && result.s) {
        const klatency = result.c - result.p1;
        const slatency = result.s - result.p1;
        return acc.concat([slatency - klatency]);
      }

      return acc;
    }, []);
    const m = median(sums);
    log.debug('%j', sums);
    log.info('Sonicd client median extra latency: %s microseconds', m);
  }, 1000);
}

function startConsuming() {
  const consumer = new Consumer(kclient, consumerpay, consumeropt);
  const stream = sclient.stream(SONICD_QUERY);

  stream.on('data', (data) => {
    let ts;
    let d;
    try {
      if (!disableParsing) {
        // construct object from data & meta arrays
        d = {};
        data.forEach((val, idx) => {
          d[meta[idx][0]] = val;
        });
      } else {
        d = JSON.parse(data[0]);
      }
      ts = microtime.now();
      results[d.id].s = ts;
    } catch (e) {
      log.warn('unexpected message %s: %s', data, e);
    }
    // log.debug('sonicd client received %j in %s microseconds', data, ts - data[1]);
  });

  stream.on('metadata', m => {
    meta = m;
    log.debug('sonicd client received metadata: %j', meta);
  });

  stream.on('error', (err) => {
    log.error('sonicd stream error: %s', err);
    process.exit(1);
  });

  consumer.on('message', (message) => {
    try {
      const parsed = JSON.parse(message.value);
      const i = parsed.id;
      const ts = microtime.now();
      results[i].c = ts;
      // log.debug('consumer received %j in %s microseconds', message, ts - parsed.ts);
    } catch (e) {
      log.warn('unexpected message %s: %s', message, e);
    }
  });
}

producer.on('ready', () => {
  producer.createTopics([TOPIC], false, (err, data) => {
    if (err) {
      log.error('error creating test topic: %s', err);
      process.exit(1);
      return;
    }

    log.info('topic "%s" ready: %j', TOPIC, data);

    startConsuming();
    startProducing();
    startMeasuring();
  });
});

producer.on('error', (err) => {
  log.error('producer error: %s', err);
  process.exit(1);
});
