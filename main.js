const fs = require('fs');
const { nanoid } = require('nanoid');
const pm = require('pretty-ms');
const { clusters, output, topics } = require('./src/config.js');
const { runConsumer, runProducer, getKafka } = require('./src/kafka.js');
const { report, getSummary, reportProgress } = require('./src/stats.js');

setInterval(report, 2500);

if (!!output) {
  setInterval(() => {
    fs.writeFileSync(output, JSON.stringify(getSummary(), null, 2));
  }, 5000);
}

async function run(cluster, kafka, topics) {
  const topic = cluster.topic;
  const topicPattern = new RegExp(topics.map((t) => `^.*${topic}`).join('|'));
  const testId = nanoid();
  const { producerDelay = -1, producerCount = 1 } = cluster;
  console.log(
    `Starting ${producerCount} producers with delay=${
      producerDelay === -1 ? 'NONE' : pm(producerDelay)
    }`,
  );
  for (let i = 0; i < producerCount; i++) {
    runProducer(kafka, topic, testId, producerDelay);
  }
  const cgId = nanoid(6);
  runConsumer(kafka, topicPattern, `k1-${cgId}`, cluster, reportProgress);
}

clusters.forEach((cluster) => {
  const kafka = getKafka(cluster);
  run(cluster, kafka, topics);
});
