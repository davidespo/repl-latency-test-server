const fs = require('fs');
const { Kafka } = require('kafkajs');
const { observe } = require('./stats.js');

function getKafka({ brokerUrl, caFile, serviceKeyFile, serviceCertFile }) {
  return new Kafka({
    clientId: 'repl-latenct-test-app',
    brokers: [brokerUrl],
    ssl: {
      rejectUnauthorized: false,
      ca: [fs.readFileSync(caFile, 'utf-8')],
      key: fs.readFileSync(serviceKeyFile, 'utf-8'),
      cert: fs.readFileSync(serviceCertFile, 'utf-8'),
    },
  });
}

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

async function runConsumer(kafka, topic, id, cluster) {
  const consumer = kafka.consumer({ groupId: `cg-${id}` });
  //   const consumer = kafka.consumer();

  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: false });

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const payload = JSON.parse(message.value.toString());
      const diff = Date.now() - payload.ts;
      const key = `${id}:${topic}`;
      observe(cluster, topic, key, diff);
      //   let {
      //     min = Number.MAX_VALUE,
      //     max = -1,
      //     count = 0,
      //     avg = 0,
      //     startTime = Date.now(),
      //     gk = new sperc.GK(0.01),
      //   } = STATE[key] || {};
      //   gk.insert(diff);
      //   min = Math.min(min, diff);
      //   max = Math.max(max, diff);
      //   avg = (count * avg + diff) / (count + 1);
      //   count++;
      //   progressCb(key, {
      //     cluster,
      //     cg: id,
      //     topic,
      //     diff,
      //     gk,
      //     min,
      //     max,
      //     count,
      //     avg,
      //     startTime,
      //   });
    },
  });
}

module.exports = {
  getKafka,
  runConsumer,
  runProducer,
};
