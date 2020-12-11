const sperc = require('streaming-percentiles');
const ansiEscapes = require('ansi-escapes');
const pm = require('pretty-ms');

const { pretty, stats } = require('./config.js');

const getSPerc = () => new sperc.GK(stats.epsilon || 0.01);

const STATE = {};

const observe = (cluster, topic, key, diff) => {
  let {
    min = Number.MAX_VALUE,
    max = -1,
    count = 0,
    avg = 0,
    startTime = Date.now(),
    gk = new sperc.GK(0.01),
  } = STATE[key] || {};
  gk.insert(diff);
  min = Math.min(min, diff);
  max = Math.max(max, diff);
  avg = (count * avg + diff) / (count + 1);
  count++;
  STATE[key] = {
    cluster,
    cg: key,
    topic,
    diff,
    gk,
    min,
    max,
    count,
    avg,
    startTime,
  };
};

const getSummary = () => {
  const keys = Object.keys(STATE).sort();
  const summary = {};
  keys.forEach((key) => {
    const { cluster, cg, topic, gk, min, max, avg, count, startTime } = STATE[
      key
    ];
    const rate = (count * 1000.0) / (Date.now() - startTime);
    const perc = (q) => pm(Math.trunc(gk.quantile(q)));
    const percentiles = {};
    stats.quantiles.forEach((p) => (percentiles[`${p * 100}%`] = perc(p)));
    summary[key] = {
      cluster,
      cg,
      topic,
      gk,
      min,
      max,
      avg,
      count,
      startTime,
      rate,
      percentiles,
    };
  });
  return summary;
};

const reportLine = (summaryLine) => {
  const {
    cg,
    topic,
    gk,
    min,
    max,
    avg,
    count,
    rate,
    percentiles,
  } = summaryLine;
  const percContent = Object.keys(percentiles)
    .map((pk) => `${pk}:${percentiles[pk]}`)
    .join(', ');
  const consumerGroupS = `[${cg}]`;
  const topicS = `[${topic}]`;
  const statsS =
    `{min/avg/max: ${pm(min)}<${pm(avg)}<${pm(max)} ${percContent}, ` +
    `count: ${count.toLocaleString()}, ` +
    `rate: ${Math.trunc(rate).toLocaleString()}m/s }`;
  return [consumerGroupS, topicS, statsS];
};

const prettyReport = () => {
  const summary = getSummary();
  const keys = Object.keys(summary).sort();
  let content = '';
  keys.forEach((key) => {
    const [consumerGroupS, topicS, statsS] = reportLine(summary[key]);
    content +=
      consumerGroupS.bold.bgRed.white + topicS.bold.bgBlue.white + statsS.green;
  });
  if (deleteLineCount > 0) {
    process.stdout.write(ansiEscapes.eraseLines(deleteLineCount + 1) + content);
  } else {
    process.stdout.write(content);
  }
  deleteLineCount = keys.length;
};

const logReport = () => {
  const keys = Object.keys(STATE).sort();
  const summary = getSummary();
  keys.forEach((key) => {
    const [consumerGroupS, topicS, statsS] = reportLine(summary[key]);
    console.log(`${consumerGroupS}${topicS} ${statsS}`);
  });
  console.log('');
};

const report = pretty ? prettyReport : logReport;

module.exports = {
  getSPerc,
  report,
  getSummary,
  observe,
};
