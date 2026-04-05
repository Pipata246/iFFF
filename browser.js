/**
 * Запуск Chromium (Playwright): прокси, stealth, locale, viewport, timezone.
 *
 * Переменные окружения (PowerShell):
 *   $env:AVITO_NO_PROXY=1   — без прокси (проверка, что падает именно прокси).
 *   $env:AVITO_MANUAL_PROXY=1 — см. main.js: сначала ручной вход на сайте провайдера, затем Enter.
 *   $env:AVITO_PROXY_ON_LAUNCH=1 — прокси на chromium.launch вместо newContext (если ERR_CONNECTION_CLOSED через контекст).
 *   $env:WB_SKIP_HOME_WARMUP=1 — Wildberries: не заходить сначала на главную (wb_runner.js), только пауза и сразу URL поиска.
 *   $env:PARSER_WB_STORAGE_STATE=wb_storage.json — файл сессии после входа на wildberries.ru (цены с WB Кошельком).
 *     Создать: PLAYWRIGHT_HEADLESS=0 npm run wb:save-session
 *   $env:WB_SESSION_CHANNEL=chrome|msedge|chromium — для save_wb_storage_state.js (см. файл).
 */

const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');
const {
  pickUserAgent,
  pickViewport,
  pickTimezone,
  parseProxyUrl,
} = require('./utils');

/**
 * Мобильный прокси (HTTP). Ротация ~2 мин — при длительных сессиях возможны обрывы.
 * Логин: буква O в начале (OU9…), не ноль (0U9…).
 */
const PROXY_URL = 'http://OU9tLKqk63:t8cfLDzi55@91.221.70.204:10237';

/**
 * Скрипт инициализации страницы: снижение признаков автоматизации.
 */
const STEALTH_INIT = () => {
  // navigator.webdriver
  Object.defineProperty(navigator, 'webdriver', {
    get: () => undefined,
    configurable: true,
  });
  // Chrome object (часто проверяют скрипты)
  // @ts-ignore
  window.chrome = window.chrome || { runtime: {} };
  // permissions query
  const origQuery = window.navigator.permissions?.query;
  if (origQuery) {
    // @ts-ignore
    window.navigator.permissions.query = (parameters) =>
      parameters && parameters.name === 'notifications'
        ? Promise.resolve({ state: Notification.permission })
        : origQuery(parameters);
  }
  // plugins length
  Object.defineProperty(navigator, 'plugins', {
    get: () => [1, 2, 3, 4, 5],
  });
  Object.defineProperty(navigator, 'languages', {
    get: () => ['ru-RU', 'ru', 'en-US', 'en'],
  });
};

/** Проверка без прокси: в PowerShell ` $env:AVITO_NO_PROXY=1; node main.js ` */
function isProxyDisabled() {
  return process.env.AVITO_NO_PROXY === '1' || process.env.AVITO_NO_PROXY === 'true';
}

function useProxyOnLaunch() {
  return process.env.AVITO_PROXY_ON_LAUNCH === '1' || process.env.AVITO_PROXY_ON_LAUNCH === 'true';
}

/**
 * Прокси для контекста: server + username/password (надёжнее для HTTPS через HTTP-прокси).
 * @returns {{ proxy?: { server: string, username?: string, password?: string } }}
 */
function getProxyOptions() {
  if (isProxyDisabled()) {
    return {};
  }
  const { server, username, password } = parseProxyUrl(PROXY_URL);
  const proxy = { server };
  if (username) proxy.username = username;
  if (password) proxy.password = password;
  return { proxy };
}

/**
 * Запуск Chromium без прокси на процессе — прокси в newContext (см. newStealthContext).
 * @param {import('playwright').LaunchOptions} [overrides] — например { channel: 'chrome' } для save_wb_storage_state.js
 * @returns {Promise<import('playwright').Browser>}
 */
async function launchBrowser(overrides = {}) {
  /** @type {import('playwright').LaunchOptions} */
  const launchOpts = {
    // VPS запуск: браузер не должен отображаться (headless по умолчанию).
    // Чтобы открыть браузер с UI локально: PLAYWRIGHT_HEADLESS=0 node main.js
    headless: process.env.PLAYWRIGHT_HEADLESS === '0' ? false : true,
    args: [
      '--disable-blink-features=AutomationControlled',
      '--disable-dev-shm-usage',
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-infobars',
      '--window-position=0,0',
      '--ignore-certificate-errors',
      '--ignore-certificate-errors-spki-list',
    ],
    ignoreDefaultArgs: ['--enable-automation'],
    ...overrides,
  };

  const p = getProxyOptions().proxy;
  if (p && useProxyOnLaunch()) {
    launchOpts.proxy = p;
  }

  return chromium.launch(launchOpts);
}

/**
 * Путь к JSON сессии WB (cookies + localStorage) для Playwright storageState.
 * @param {string} raw из PARSER_WB_STORAGE_STATE
 * @returns {string|null} абсолютный путь или null
 */
function resolveWbStorageStatePath(raw) {
  const s = String(raw || '').trim();
  if (!s) return null;
  const abs = path.isAbsolute(s) ? s : path.join(process.cwd(), s);
  return fs.existsSync(abs) ? abs : null;
}

/**
 * Новый контекст: прокси (если включён), viewport, UA, locale, timezone + stealth.
 * @param {import('playwright').Browser} browser
 * @param {{ wbUseSavedSession?: boolean }} [opts] — при wbUseSavedSession читать PARSER_WB_STORAGE_STATE (только wb_runner)
 */
async function newStealthContext(browser, opts = {}) {
  const viewport = pickViewport();
  const userAgent = pickUserAgent();
  const timezoneId = pickTimezone();

  const proxyOpts = getProxyOptions();

  /** @type {import('playwright').BrowserContextOptions} */
  const contextOptions = {
    viewport,
    userAgent,
    locale: 'ru-RU',
    timezoneId,
    javaScriptEnabled: true,
    ignoreHTTPSErrors: true,
    extraHTTPHeaders: {
      'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    },
  };

  if (proxyOpts.proxy && !useProxyOnLaunch()) {
    contextOptions.proxy = proxyOpts.proxy;
  }

  if (opts && opts.wbUseSavedSession) {
    const p = resolveWbStorageStatePath(process.env.PARSER_WB_STORAGE_STATE);
    if (p) contextOptions.storageState = p;
  }

  const context = await browser.newContext(contextOptions);

  await context.addInitScript(STEALTH_INIT);
  return context;
}

module.exports = {
  PROXY_URL,
  isProxyDisabled,
  launchBrowser,
  newStealthContext,
  resolveWbStorageStatePath,
};
