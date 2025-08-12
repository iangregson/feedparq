import localforage from 'https://cdn.jsdelivr.net/npm/localforage@1.10.0/+esm';

const APP_NAME = 'feedparq';
const PIN_MOJIS = ['📍', '📰', '🧐', '👉', '🧑‍💻', '🧵', '🎩', '🦧', '🪐', '🌈', '🥖', '🥨', '🫙', '🥌', '🎲', '🎯', '🚧', '🏗️', '⏱️', '⚖️', '🧱', '💡', '🔦', '🏺', '🎁', '💌', '📜', '📓', '🔗', '🔖', '📚', '📌', '🖍️', '✏️', '🆒', '🟣', '🔸', '🔹', '🟧', '🟦', '📣', '🚩'];
const NAMED_MOJI = { 'check': '✓', 'timer': '⏳' };
const LINE_BREAK = '\n';
const LINE_SEPARATOR = '-----------------------';
const TAG_ALLOW_LIST = ['audio','video','img','picture','iframe'];
const constants = {
  APP_NAME,
  PIN_MOJIS,
  NAMED_MOJI,
  LINE_BREAK,
  LINE_SEPARATOR,
  TAG_ALLOW_LIST
};
window.feedparq = new Map([['constants', constants]]);

class Fs {
  static Error = class FsError extends Error { };
  static error(message) {
    return new Fs.Error('fs : ' + message);
  }

  static _store = localforage.createInstance({
    driver: localforage.INDEXEDDB,
    name: 'fs',
    storeName: `${APP_NAME}.fs`,
    version: 1.0,
  });
  static protocol = 'file:';
  static get name() {
    return Fs._store.config().name;
  }
  static get storeName() {
    return Fs._store.config().storeName;
  }
  static filePathToUrl(path) {
    return new URL(`${Fs.protocol}//${Fs.storeName}/${path}`);
  }
  static async ls() {
    return Fs._store
      .keys()
      .then(keys => keys.map(key => new URL(key)));
  }
  static async size() {
    return Fs._store.length();
  }
  static async read(url) {
    Fs.assertSupported(url);
    return Fs._store.getItem(url.href);
  }
  static async write(url, file) {
    Fs.assertFile(file);
    Fs.assertSupported(url);
    return Fs._store.setItem(url.href, file);
  }
  static async exists(url) {
    Fs.assertSupported(url);
    let keys = await Fs._store.keys();
    keys = new Set(keys);
    return keys.has(url.href);
  }
  static assertSupported(url) {
    if (url.protocol !== Fs.protocol) {
      throw Fs.error('protocol not supported');
    }
    if (url.host !== Fs.storeName) {
      throw Fs.error('host not supported');
    }
  }
  static assertFile(file) {
    if (file instanceof File) return;
    throw Fs.error('can only store files');
  }
  static async clear() {
    return Fs._store.clear();
  }
}

class Ui {
  static body = document.querySelector('body');
  static main = document.querySelector('main');
  static header = document.querySelector('body > header');
  static deleteDataBtn = document.querySelector('button#delete-data');

  static registerHandlers() {
    const { NAMED_MOJI } = window.feedparq.get('constants');
    Ui.deleteDataBtn.onclick = async () => {
      const p = Ui.deleteDataBtn.parentElement;
      const status = document.createElement('span');
      status.innerHTML = NAMED_MOJI.timer;
      p.appendChild(status);
      await Fs.clear();
      status.innerHTML = NAMED_MOJI.check;
    };
  }
}

class App {
  static async new() {
    const app = new App();
    return app;
  }

  constructor() {}

  async main() {}
}

async function main() {
  try {
    feedparq.set('Fs', Fs);
    feedparq.set('Ui', Ui);
    
    const app = await App.new();
    feedparq.set('App', app);
    window.app = app;

    Ui.registerHandlers();
  } catch (error) {
    console.error(error);
  }
}

document.addEventListener('DOMContentLoaded', () => {
  main();
});

