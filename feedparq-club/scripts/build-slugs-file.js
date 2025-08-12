#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const feedsPath = path.join(__dirname, '..', 'config/feeds');
const fileNames = fs.readdirSync(feedsPath);
const slugsMap = fileNames.reduce((acc, fileName) => {
  const slug = `/${fileName}`;
  acc[slug] = {};
  return acc;
}, {});

const functionsPath = path.join(__dirname, '..', 'functions');
fs.writeFileSync(path.join(functionsPath, 'slugs.json'), JSON.stringify(slugsMap, null, 2));
