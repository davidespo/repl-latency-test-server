const fs = require('fs');
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
  })
  .option('pretty', {
    alias: 'p',
    type: 'boolean',
    description: 'Pretty print the report to console',
  }).argv;

const configFile = argv.config || 'config.json';
const output = argv.output;
const pretty = argv.pretty;

const config = JSON.parse(fs.readFileSync(configFile));
const { stats, clusters } = config;
const topics = clusters.map(({ topic }) => topic);

module.exports = {
  output,
  pretty,
  topics,
  stats,
  clusters,
};
