import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.1-dev106.0/+esm';
import localforage from 'https://cdn.jsdelivr.net/npm/localforage@1.10.0/+esm';
import { marked } from 'https://cdn.jsdelivr.net/npm/marked@13.0.3/+esm';
import DOMPurify from 'https://cdn.jsdelivr.net/npm/dompurify@3.1.6/+esm';

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

class Meta {
  static get channel() {
    const name = document.querySelector('link[data-channel]').getAttribute('title');
    const url = new URL(document.querySelector('link[data-channel]').href);
    return { name, url };
  }
  static get settings() {
    const name = document.querySelector('link[data-settings]').getAttribute('title');
    const url = new URL(document.querySelector('link[data-settings]').href);
    return { name, url };
  }
}

class Modal {
  #element;
  #form;
  #modelInput;
  #keyInput;

  constructor(element) {
    this.#element = element;
    this.#form = element.querySelector('form');
    this.#modelInput = element.querySelector('form > select#model');
    this.#keyInput = element.querySelector('form > input#key');

    const closeBtn = element.querySelector('button#cancel');
    closeBtn.onclick = () => {
      this.close();
    };
  }

  async show(llmcreds = {}) {
    return new Promise((resolve) => {
      this.#element.showModal();
      this.key = llmcreds.key;
      this.model = llmcreds.model;
      this.#form.addEventListener('submit', (e) => {
        e.preventDefault();
  
        if (this.model && this.key) {
          this.close();
          resolve({ 
            key: this.key,
            model: this.model
          });
        } else {
          resolve(null);
        }
      }, { once: true });
    });
  }

  close() {
    this.#element.classList.add('closing');
    this.#element.addEventListener('animationend', () => {
      this.#element.close();
      this.#element.classList.remove('closing');
    }, { once: true });
  }

  set model(model) {
    return this.#modelInput.value = model ?? null;
  }
  get model() {
    return this.#modelInput.value ?? null;
  }

  set key(key) {
    return this.#keyInput.value = key ?? null;
  }
  get key() {
    return this.#keyInput.value ?? null;
  }
}

class Ui {
  static body = document.querySelector('body');
  static main = document.querySelector('main');
  static header = document.querySelector('body > header');
  static subheader = document.querySelector('header > div#subheader');
  static optionButtons = document.querySelectorAll('header > div#subheader > nav > button');
  static modal = new Modal(document.querySelector('dialog'));

  static checkCuratedSponsoredLinks() {
    const sponsored = Ui.subheader.querySelector('a#sponsored');
    if (!sponsored) {
      Ui.subheader.querySelector('div#sponsored').remove();
    }
    const curated = Ui.subheader.querySelector('a#curated');
    if (!curated) {
      Ui.subheader.querySelector('div#curated').remove();
    }
  }

  static applyCheck(node) {
    const parent = node.parentElement;
    const existingCheck = parent.querySelector('span#check');
    existingCheck && existingCheck.remove();
    const { NAMED_MOJI } = feedparq.get('constants');
    const check = document.createElement('span');
    check.id = 'check';
    check.innerHTML = NAMED_MOJI.check;
    node.insertAdjacentElement('afterend', check);
  }

  static registerHandlers(app) {
    // option buttons
    Ui.optionButtons.forEach(button => {
      switch (button.id) {
        case 'ai':
          button.onclick = async () => {
            app.settings.viewMode = 'ai';
            
            if (app.settings.hasLlmcreds) {
              Ui.applyCheck(button);
              Ui.render(app);
              return;
            }

            const llmcreds = await Ui.modal
              .show(app.settings.llmcreds);
            if (!llmcreds) return;
            
            app.settings.llmcreds = llmcreds;
            Ui.applyCheck(button);
            Ui.render(app);
          };
          break;
        case 'links':
          button.onclick = () => {
            app.settings.viewMode = 'links';
            Ui.applyCheck(button);
            Ui.render(app);
          };
          break;
        case 'more':
          button.onclick = () => {
            app.settings.viewMode = 'more';
            Ui.applyCheck(button);
            Ui.render(app);
          };
          break;
      }

      if (button.id === app.settings.viewMode) {
        Ui.applyCheck(button);
      }
    });
  }

  static attachMedia(app) {
    const rowsById = app.rows.reduce((acc, row) => {
      acc[row.id] = row;
      return acc;
    }, {});

    const articles = Ui.main.querySelectorAll('article');

    articles.forEach(article => {
      const row = rowsById[article.id];
      const p = document.createElement('p');
      p.innerHTML = row.summary || row.content || row.media_descriptions;
      article.appendChild(p);

      const mType = mediaType(row);
      if (mType) {
        switch (mType) {
          case 'youtube':
            embedYoutube(article, row);
            break;
          case 'audio': 
            embedAudio(article, row);
            break;
        
          default:
            break;
        }
      }
    });

    function mediaType(row) {
      const audioExts = new Set([
        'mp3',
        'wav',
        'ogg',
        'aac',
        'm4a',
      ]);
      const imageExtensions = new Set([
        'jpg',
        'png',
        'gif',
        'svg',
        'webp',
      ]);
      if (!row.media_url) {
        return null;
      }
      const mediaUrl = new URL(row.media_url);
      const pathParts = mediaUrl.pathname.split('/');
      const lastPart = pathParts.slice(-1)[0];
      const fileExtension = lastPart.split('.').slice(-1)[0];
      if (mediaUrl.host.includes('youtube.com')) {
        return 'youtube'
      } else if (fileExtension && audioExts.has(fileExtension)) {
        return 'audio'
      } else if (fileExtension && imageExtensions.has(fileExtension)) {
        return 'image'
      } else {
        return null;
      }
    }

    function embedYoutube(article, row) {
      const iframe = document.createElement('iframe');
      const videoUrl = new URL(row.link);
      const videoId = videoUrl.searchParams.get('v');
      iframe.src = `https://www.youtube.com/embed/${videoId}`;
      iframe.title = 'YouTube video player';
      iframe.allowFullscreen = true;
      iframe.allow = 'accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture';
      iframe.setAttribute('frameborder', '0');
      article.appendChild(iframe);
    }

    function embedAudio(article, row) {
      const audio = document.createElement('audio');
      audio.controls = true;
      
      audio.innerHTML = `
        <source src="${row.media_url}" type="audio/mpeg">
        Your browser does not support the audio element.
      `;

      article.appendChild(audio);
    }
  }

  static async aiRender(app) {
    await app.llm.generate(app.rows, Ui.main, app.settings.llmcreds);
  }

  static clear() {
    Ui.main.innerHTML = '';
  }

  static channelFeedNotFound() {
    Ui.clear();
    Ui.cancelLoader();
    const notFound = document.createElement('div');
    notFound.id = 'error';
    notFound.innerHTML = `
      ⚠
      <p>not found</p>
    `;
    Ui.body.insertBefore(notFound, Ui.main);
  }

  static cancelLoader() {
    const loader = Ui.body.querySelector('div#loader');
    loader && loader.remove();
  }

  static renderLinks(app) {
    const { PIN_MOJIS } = feedparq.get('constants');
    for (const row of app.rows) {
      const article = document.createElement('article');
      article.id = row.id;
      article.style.opacity = '0';
      article.style.animationDelay = '10ms';
      const n = Math.ceil(Math.random() * 1e6);
      const pinMoji = constants.PIN_MOJIS[n % PIN_MOJIS.length];
      const link = new URL(row.link);
      article.innerHTML = `
        <header>
          <small class="pin">${pinMoji}</small>
          
          <a id="${row.id}" href="${row.link}">${row.title}</a>
          
          <small class="link-host">
            <a href="${link.origin}">(${link.host})</a>
          </small>
        </header>
      `;

      Ui.main.appendChild(article);
      
      // Force a reflow before starting the animation
      void article.offsetWidth;
      article.style.removeProperty('opacity');
      article.style.removeProperty('animation-delay');
    }
  }

  static async render(app) {
    Ui.clear();
    switch (app.settings.viewMode) {
      case 'ai':
        await Ui.aiRender(app);
        break;
      case 'links':
        Ui.renderLinks(app);
        break;
      case 'more':
        Ui.renderLinks(app);
        Ui.attachMedia(app);
        break;
      default:
        Ui.renderLinks(app);
    }
    Ui.cancelLoader();
  }
}

class Channel {
  static async topK(db, k) {
    const conn = await db.connect();
    const table = await conn.query(`SELECT * FROM channel ORDER BY published_ms DESC LIMIT ${k}`);
    await conn.close();
    const rows = table.toArray().map(r => r.toJSON());
    return rows;
  }
}

class Db {
  static Error = class DbError extends Error { };
  static error(message, cause) {
    return new Db.Error('Db : ' + message, { cause });
  }

  static async loadChannel(db) {
    const { channel } = feedparq.get('Meta');
    const { name, url } = channel;
    
    const test = await fetch(url);
    if (!test.ok) {
      Ui.channelFeedNotFound();
      throw Db.error('channel feed not found');
    }

    await db.registerFileURL(name, url.href, duckdb.DuckDBDataProtocol.HTTP, false);
    const conn = await db.connect();
    // await conn.query(`CREATE TABLE channel AS SELECT * FROM ${Meta.channel.name};`);
    // await conn.close();
    await conn.query(
      `CREATE VIEW IF NOT EXISTS 'channel' AS SELECT * FROM parquet_scan('${name}')`,
    );
    await conn.close();
  }

  static async loadSettings(db) {
    const { settings } = feedparq.get('Meta');
    if (await Fs.exists(settings.url)) {
      const file = await Fs.read(settings.url);
      const arrayBuffer = await file.arrayBuffer();
      await db.registerFileBuffer(settings.name, new Uint8Array(arrayBuffer));
      const conn = await db.connect();
      await conn.query(`
        CREATE TABLE settings (
          key TEXT UNIQUE NOT NULL,
          value TEXT
        );`
      );
      await conn.query(`INSERT INTO settings SELECT * FROM ${settings.name};`);
      await conn.close();
    } else {
      await db.registerEmptyFileBuffer(settings.name);
      const conn = await db.connect();
      await conn.query(`
        CREATE TABLE settings (
          key TEXT UNIQUE NOT NULL,
          value TEXT
        );`
      );
      await conn.close();
      await Db.saveSettings(db);
    }
  }

  static async saveSettings(db) {
    const { settings } = feedparq.get('Meta');
    const conn = await db.connect();
    await conn.send(`COPY (SELECT * FROM settings) TO '${settings.name}' (FORMAT 'parquet');`);
    await conn.close();
    const parquetBuffer = await db.copyFileToBuffer(settings.name);
    const blob = new Blob([parquetBuffer], { type: 'application/octet-stream' });
    const file = new File([blob], settings.name);
    await Fs.write(settings.url, file);
  }

  static async new() {
    const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

    // Select a bundle based on browser checks
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

    const worker_url = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
    );

    // Instantiate the asynchronus version of DuckDB-Wasm
    const worker = new Worker(worker_url);
    const logger = new duckdb.ConsoleLogger();
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(worker_url);

    const conn = await db.connect();
    await conn.query(`
      INSTALL parquet; LOAD parquet;
    `);
    await conn.close();

    await Db.loadChannel(db)
    await Db.loadSettings(db)

    return db;
  }
}

class Settings {
  static Error = class SettingsError extends Error { }
  static error(message) {
    return new Settings.Error('Settings : ' + message);
  }

  static URN_PREFIX = `urn:${APP_NAME}:settings`;
  static getUrn(key, type = 'string') {
    return `${this.URN_PREFIX}:${type}:${key}`;
  }

  #defaults = {
    [Settings.getUrn('viewMode', 'string')]: 'links',
    [Settings.getUrn('topK', 'number')]: '50',
  };
  #store = new Map([...Object.entries(this.#defaults)]);

  constructor(db) {
    this.db = db;
  }

  async read() {
    const conn = await this.db.connect();
    const table = await conn.query(`SELECT * FROM settings;`);
    await conn.close();
    const rows = table.toArray().map(r => r.toJSON());
    return rows.reduce((acc, next) => {
      this.#store.set(next.key, next.value);
      acc[next.key] = next.value;
      return acc;
    }, {});
  }

  async write(keyValues = {}) {
    const conn = await this.db.connect();
    Object.keys(keyValues).forEach(async (key) => {
      Settings.assertUrn(key);
      const value = keyValues[key];
      await conn.query(`INSERT INTO settings (key, value)
        VALUES ('${key}', '${value}')
        ON CONFLICT (key)
        DO UPDATE SET value = '${value}';
      `);
    });
    await conn.close();
  }

  async save() {
    await this.write(Object.fromEntries(this.#store));
    await Db.saveSettings(this.db);
  }

  static assertUrn(key) {
    let hasError;
    if (typeof key !== 'string') {
      hasError = true;
    }
    const parts = key.split(':');
    if (parts.length < 2) {
      hasError = true;
    }
    if (hasError) {
      throw Settings.error('settings keys should be urn format');
    }
  }

  static assertString(str) {
    if (typeof str === 'string') return;
    throw Settings.error(`typeof str is ${typeof str} but should be string`);
  }

  get viewMode() {
    const urn = Settings.getUrn('viewMode', 'string');
    return this.#store.get(urn);
  }
  set viewMode(viewMode) {
    Settings.assertString(viewMode);
    const urn = Settings.getUrn('viewMode', 'string');
    this.#store.set(urn, viewMode);
    this.save()
      .catch(err => {
        throw new Settings.Error('could not save settings', { cause: err });
      });
  }

  get llmcreds() {
    const modelUrn = Settings.getUrn('llmcreds:model', 'string'); 
    const keyUrn = Settings.getUrn('llmcreds:key', 'string'); 
    const model = this.#store.get(modelUrn) ?? null;
    const key = this.#store.get(keyUrn) ?? null;
    return { key, model };
  }
  set llmcreds(creds = {}) {
    Settings.assertString(creds.key);
    Settings.assertString(creds.model);
    const modelUrn = Settings.getUrn('llmcreds:model', 'string'); 
    const keyUrn = Settings.getUrn('llmcreds:key', 'string'); 
    this.#store.set(keyUrn, creds.key);
    this.#store.set(modelUrn, creds.model);
    this.save()
      .catch(err => {
        throw new Settings.Error('could not save settings', { cause: err });
      });
  }
  get hasLlmcreds() {
    const { key, model } = this.llmcreds;
    try {
      Settings.assertString(key);
      Settings.assertString(model);
    } catch (_) {
      return false;
    }
    return true;
  }
}

class OpenAi {
  #endpoint = new URL('https://api.openai.com/v1/chat/completions');
  #system_prompt = `
    You're an experienced and creative web developer.

    The user will give you a list of messages and each message will
    contain an entry from a content feed. It could be a blog post,
    a podcast episode, a video clip or audio recording. Let's call 
    these content feed entries "posts".

    You should respond with a valid html page that will render in 
    the user's browser. 

    You should not include all the posts in your response. Instead,
    you should synthesise and summarise where possible and select
    5 distinct posts that you find most appealing. 

    All of the posts will contain an 'id' and a 'link'. If you
    reference a post, you must cite it with that link and make sure
    that the link's id attribute contains the post's id.

    You should use semantic html. But avoid using inline styles or 
    css classes. 

    Additional helpful rules are:
    - If the post's 'media_url' is audio, use the appropriate html
      element to embed the media
    - If the media_url is a YouTube video, use the appropriate
      YouTube embed code instead
    - Feel free to use emoji, icons, svgs and images as you see fit
    - Have fun!
  `;

  #requestBody = {
    model: 'gpt-4o-mini',
    messages: [
      { role: 'system', content: this.#system_prompt }
    ]
  };

  async generate(rows, outputElement, { model, key }) {
    const { LINE_BREAK, LINE_SEPARATOR } = feedparq.get('constants');
    const documents = rows.map(row => {
      const doc = [
        `id: ${row.id}`,
        `title: ${row.title}`,
        `link: ${row.link}`,
        `media_url: ${row.media_url}`,
        LINE_SEPARATOR,
        (row.summary || row.content || row.media_descriptions),
        LINE_BREAK,
      ];
      return doc.join(LINE_BREAK);
    });
    const requestBody = {
      ...this.#requestBody,
      model,
      messages: [
        ...this.#requestBody.messages,
        ...documents.map(content => ({ role: 'user', content }))
      ],
      stream: true,
    };
    const request = new Request(this.#endpoint, {
      method: 'POST',
      headers: new Headers({
        'Accept': 'application/json',
        'Access-Control-Allow-Credentials': 'true',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + key,
      }),
      body: JSON.stringify(requestBody),
    });
    const response = await fetch(request);
    
    const stream = new OpenAi.Stream(response);
    let outputString = '';
    for await (const data of stream) {
      outputString += data.choices[0]?.delta?.content ?? '';
      outputElement.innerHTML = marked.parse(outputString);
    }
    const code = outputElement.querySelector('code').innerText;
   
    const { TAG_ALLOW_LIST } = feedparq.get('constants');
    outputElement.innerHTML = DOMPurify.sanitize(code, { ADD_TAGS: TAG_ALLOW_LIST });
  }

  static Stream = class Stream {
    #readable;

    constructor(response) {
      if (!response.ok) {
        throw OpenAi.error('error from OpenAI api');
      }
  
      const stream = response.body;
      if (!stream) {
        throw OpenAi.error('no stream in OpenAI response');
      }

      this.#readable = stream;
    }

    static async * splitStream(stream) {
      const reader = stream.getReader();
      let lastFragment = "";
      try {
        while (true) {
          const { value, done } = await reader.read();
          if (done) {
            // Flush the last fragment now that we're done
            if (lastFragment !== "") {
              yield lastFragment;
            }
            break;
          }
          const data = new TextDecoder().decode(value);
          lastFragment += data;
          const parts = lastFragment.split("\n\n");
          // Yield all except for the last part
          for (let i = 0; i < parts.length - 1; i += 1) {
            yield parts[i];
          }
          // Save the last part as the new last fragment
          lastFragment = parts[parts.length - 1];
        }
      } finally {
        reader.releaseLock();
      }
    }

    [Symbol.asyncIterator] = async function* () {
      for await (const data of OpenAi.Stream.splitStream(this.#readable)) {
        // Handling the OpenAI HTTP streaming protocol
        if (data.startsWith("data:")) {
          const json = data.substring("data:".length).trimStart();
          if (json.startsWith("[DONE]")) {
            return;
          }
          yield JSON.parse(json);
        }
      }
    }
  }
  static Error = class OpenAiError extends Error { };
  static error(message, cause) {
    return new OpenAi.Error('OpenAi : ' + message, { cause });
  }
}

class App {
  static async new() {
    const db = await Db.new();
    const app = new App(db);
    await app.settings.read();
    return app;
  }

  rows = [];

  constructor(db) {
    this.db = db;
    this.settings = new Settings(db);
    this.llm = new OpenAi();
  }

  async main() {
    const rows = await Channel.topK(this.db, 50);
    this.rows = rows;
    await Ui.render(this);
  }
}

async function main() {
  try {
    feedparq.set('Db', Db);
    feedparq.set('Fs', Fs);
    feedparq.set('Meta', Meta);
    feedparq.set('Ui', Ui);

    Ui.checkCuratedSponsoredLinks();
    
    const app = await App.new();
    feedparq.set('App', app);
    window.app = app;

    Ui.registerHandlers(app);

    await app.main();
  } catch (error) {
    console.error(error);
  }
}

document.addEventListener('DOMContentLoaded', () => {
  main();
});

