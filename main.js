const fs = require('fs');
const ansiEscapes = require('ansi-escapes');
const pm = require('pretty-ms');
const { nanoid } = require('nanoid');
const { Kafka } = require('kafkajs');
// require('colors');

const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');
const argv = yargs(hideBin(process.argv))
  .option('config', {
    alias: 'c',
    type: 'string',
    description: 'Config file',
    default: 'config.json',
  })
  .option('output', {
    alias: 'o',
    type: 'string',
    description: 'Output file',
    config: 'state.<<random>>.json',
  }).argv;

const configFile = argv.config || 'config.json';
const output = argv.output || `state.${nanoid}.json`;

const config = require(configFile);
const clusters = config.clusters.map((conf) => getKafka(conf));

function getKafkaOld(clusterName) {
  return new Kafka({
    clientId: 'mm-test-app',
    brokers: [`davide-${clusterName}-dev-sandbox.aivencloud.com:12693`],
    ssl: {
      rejectUnauthorized: false,
      ca: [fs.readFileSync('./ca.pem', 'utf-8')],
      key: fs.readFileSync(`./service.${clusterName}.key`, 'utf-8'),
      cert: fs.readFileSync(`./service.${clusterName}.cert`, 'utf-8'),
    },
  });
}

function getKafka({ brokerUrl, caFile, serviceKeyFile, serviceCertFile }) {
  return new Kafka({
    clientId: 'latenct-test-app',
    brokers: [brokerUrl],
    ssl: {
      rejectUnauthorized: false,
      ca: [fs.readFileSync(caFile, 'utf-8')],
      key: fs.readFileSync(serviceKeyFile, 'utf-8'),
      cert: fs.readFileSync(serviceCertFile, 'utf-8'),
    },
  });
}

const k1 = getKafkaOld('k1');
const k2 = getKafkaOld('k2');

async function runProducer(kafka, topic, id, delay = 250) {
  const producer = kafka.producer();

  await producer.connect();

  if (delay > 0) {
    const send = async () =>
      producer.send({
        topic,
        messages: [{ value: JSON.stringify({ id, ts: Date.now() }) }],
      });

    await send();

    setInterval(send, delay);
  } else {
    while (true) {
      const messages = [];
      for (let i = 0; i < 50; i++) {
        messages.push({ value: JSON.stringify({ id, ts: Date.now() }) });
      }
      await producer.send({ topic, messages });
    }
  }
}

let deleteLineCount = 0;
const STATE = {};

function report() {
  const keys = Object.keys(STATE).sort();
  let content = '';
  keys.forEach((key) => {
    const { cg, topic, diff, min, max, avg, count, startTime } = STATE[key];
    const rate = (count * 1000.0) / (Date.now() - startTime);

    content +=
      `[${cg}]`.bold.bgRed.white +
      `[${topic}]`.bold.bgBlue.white +
      `\t${pm(diff)}\t(${diff.toLocaleString()}ms)`.green +
      `\t{min: ${pm(min)}, max: ${pm(max)}, avg: ${pm(
        avg,
      )}, count: ${count.toLocaleString()}, rate: ${rate.toLocaleString()}m/s }\n`
        .green;
  });
  if (deleteLineCount > 0) {
    process.stdout.write(ansiEscapes.eraseLines(deleteLineCount + 1) + content);
  } else {
    process.stdout.write(content);
  }
  deleteLineCount = keys.length;
}

async function runConsumer(kafka, topic, id) {
  const consumer = kafka.consumer({ groupId: `cg-${id}` });

  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: false });

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const payload = JSON.parse(message.value.toString());
      const diff = Date.now() - payload.ts;
      const key = `${id}:${topic}`;
      let {
        min = Number.MAX_VALUE,
        max = -1,
        count = 0,
        avg = 0,
        startTime = Date.now(),
      } = STATE[key] || {};
      min = Math.min(min, diff);
      max = Math.max(max, diff);
      avg = (count * avg + diff) / (count + 1);
      count++;
      STATE[key] = {
        cg: id,
        topic,
        diff,
        min,
        max,
        count,
        avg,
        startTime,
      };
      // report();
      //   console.log(`${topic}: ${pm(diff)}  (${diff.toLocaleString()}ms)`);
      //   console.log(
      //     `[${topic}]`.bold.bgBlue.white +
      //       `  ${pm(diff)} (${diff.toLocaleString()}ms)`.green,
      //   );
    },
  });
}

async function run(options) {
  const { producers = true, consumers = true } = options;
  const topic = 'repl-latency-test';
  const topicPattern = new RegExp(`^.*${topic}`);
  const testId = nanoid();
  const producerDelay = -1;
  if (producers) {
    console.log('Starting 2 producers');
    runProducer(k1, topic, testId, producerDelay);
    runProducer(k2, topic, testId, producerDelay);
    // await Promise.all([
    //   runProducer(k1, topic, testId, producerDelay),
    //   runProducer(k2, topic, testId, producerDelay),
    // ]);
  }
  if (consumers) {
    const cgId = nanoid(6);
    setTimeout(async () => {
      console.log('Starting 2 consumers');
      runConsumer(k1, topicPattern, `k1-${cgId}`);
      runConsumer(k2, topicPattern, `k2-${cgId}`);
    }, 5000);

    setInterval(report, 500);
    setInterval(
      () => fs.writeFileSync(`state.AWS.${cgId}.json`, JSON.stringify(STATE)),
      5000,
    );
  }
}

run({
  consumers: process.env.CONSUMERS === 'true',
  producers: process.env.PRODUCERS !== 'false',
});
