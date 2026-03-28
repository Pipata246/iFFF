/**
 * Утилиты: логирование, случайные значения, прокси, город, цена, дедупликация.
 */

const fs = require('fs');
const path = require('path');

/** Путь к xlsx на текущий запуск node main.js (после initSessionResultsOutput). */
let sessionResultsPath = null;

/**
 * Метка даты-времени для имени файла: часовой пояс как у Самары и Саратова (Europe/Samara, UTC+4).
 * @param {Date} [now]
 * @returns {string} например 2026-03-28_19-30-45
 */
function formatSamaraResultsBasename(now = new Date()) {
  const s = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Europe/Samara',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  }).format(now);
  return s.replace(' ', '_').replace(/:/g, '-');
}

/**
 * Задать путь к Excel на этот запуск: results_ГГГГ-ММ-ДД_ЧЧ-мм-сс.xlsx (время по Самаре).
 * Если файл с таким именем уже есть — добавляется _1, _2, …
 */
function initSessionResultsOutput() {
  const base = formatSamaraResultsBasename();
  const dir = __dirname;
  let candidate = path.join(dir, `results_${base}.xlsx`);
  let n = 0;
  while (fs.existsSync(candidate)) {
    n += 1;
    candidate = path.join(dir, `results_${base}_${n}.xlsx`);
  }
  sessionResultsPath = candidate;
}

function ts() {
  return new Date().toISOString();
}

/**
 * Произвольная строка с меткой времени (на русском с вызывающей стороны).
 * @param {string} message
 */
function log(message) {
  console.log(`[${ts()}] ${message}`);
}

/**
 * Пошаговый лог: Шаг N: заголовок — детали.
 * @param {number} step
 * @param {string} title
 * @param {string} [detail]
 */
function logStep(step, title, detail) {
  const tail = detail != null && detail !== '' ? ` — ${detail}` : '';
  console.log(`[${ts()}] Шаг ${step}: ${title}${tail}`);
}

/**
 * @param {string} detail
 */
function logRetry(detail) {
  console.log(`[${ts()}] ПОВТОР ПОПЫТКИ — ${detail}`);
}

/**
 * @param {string} detail
 */
function logBlock(detail) {
  console.log(`[${ts()}] ОБНАРУЖЕН БЛОК / КАПЧА — ${detail}`);
}

/**
 * @param {string} detail
 */
function logSuccess(detail) {
  console.log(`[${ts()}] УСПЕХ — ${detail}`);
}

/**
 * @param {number} min
 * @param {number} max
 */
function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

/**
 * @param {number} minMs
 * @param {number} maxMs
 * @returns {Promise<void>}
 */
function randomDelay(minMs, maxMs) {
  const ms = randomInt(minMs, maxMs);
  return new Promise((r) => setTimeout(r, ms));
}

const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
];

const VIEWPORTS = [
  { width: 1920, height: 1080 },
  { width: 1680, height: 1050 },
  { width: 1536, height: 864 },
  { width: 1440, height: 900 },
  { width: 1366, height: 768 },
];

const TIMEZONES = ['Europe/Moscow', 'Europe/Kaliningrad', 'Europe/Samara'];

/**
 * Случайный user-agent.
 * @returns {string}
 */
function pickUserAgent() {
  return USER_AGENTS[randomInt(0, USER_AGENTS.length - 1)];
}

/**
 * Случайный viewport.
 * @returns {{ width: number, height: number }}
 */
function pickViewport() {
  return VIEWPORTS[randomInt(0, VIEWPORTS.length - 1)];
}

/**
 * Europe/Moscow или случайный из списка.
 * @returns {string}
 */
function pickTimezone() {
  if (Math.random() < 0.6) return 'Europe/Moscow';
  return TIMEZONES[randomInt(0, TIMEZONES.length - 1)];
}

/**
 * Разбор прокси вида http://login:password@host:port
 * @param {string} proxyUrl
 * @returns {{ server: string, username: string, password: string }}
 */
function parseProxyUrl(proxyUrl) {
  const u = new URL(proxyUrl.trim());
  const port = u.port || (u.protocol === 'https:' ? '443' : '80');
  const server = `${u.protocol}//${u.hostname}:${port}`;
  const username = decodeURIComponent(u.username || '');
  const password = decodeURIComponent(u.password || '');
  return { server, username, password };
}

/** Частые города → slug для пути avito.ru/{slug} */
const CITY_SLUGS = {
  москва: 'moskva',
  'санкт-петербург': 'sankt-peterburg',
  спб: 'sankt-peterburg',
  питер: 'sankt-peterburg',
  екатеринбург: 'ekaterinburg',
  новосибирск: 'novosibirsk',
  казань: 'kazan',
  'нижний новгород': 'nizhniy_novgorod',
  челябинск: 'chelyabinsk',
  самара: 'samara',
  омск: 'omsk',
  'ростов-на-дону': 'rostov-na-donu',
  уфа: 'ufa',
  красноярск: 'krasnoyarsk',
  воронеж: 'voronezh',
  перм: 'perm',
  волгоград: 'volgograd',
  краснодар: 'krasnodar',
  саратов: 'saratov',
  тюмень: 'tyumen',
  тольятти: 'tolyatti',
  ижевск: 'izhevsk',
  барнаул: 'barnaul',
  'вся россия': 'all',
  россия: 'all',
  все: 'all',
};

/**
 * Нормализация названия города в slug для URL.
 * @param {string} city
 * @returns {string}
 */
function cityToSlug(city) {
  const key = city.trim().toLowerCase();
  if (CITY_SLUGS[key]) return CITY_SLUGS[key];
  // если пользователь уже ввёл латиницу (moskva)
  if (/^[a-z0-9_-]+$/i.test(city.trim())) return city.trim().toLowerCase();
  return 'all';
}

/**
 * Извлечь число цены из строки вида "125 000 ₽".
 * @param {string} text
 * @returns {number|null}
 */
function parsePriceNumber(text) {
  if (!text) return null;
  const digits = text.replace(/\D/g, '');
  if (!digits) return null;
  const n = parseInt(digits, 10);
  return Number.isFinite(n) ? n : null;
}

/**
 * Убрать дубликаты объявлений по нормализованной ссылке.
 * @param {Array<{ href: string }>} items
 * @returns {Array}
 */
function dedupeByHref(items) {
  const seen = new Set();
  const out = [];
  for (const it of items) {
    let key = (it.href || '').split('?')[0].replace(/#.*$/, '');
    try {
      const u = new URL(it.href);
      key = `${u.origin}${u.pathname}`;
    } catch (_) {
      /* оставляем key как есть */
    }
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(it);
  }
  return out;
}

/**
 * Путь к файлу результатов: после initSessionResultsOutput — уникальное имя с датой/временем (Самара).
 * @returns {string}
 */
function resultsPath() {
  if (sessionResultsPath) return sessionResultsPath;
  return path.join(__dirname, 'results.xlsx');
}

module.exports = {
  log,
  logStep,
  logRetry,
  logBlock,
  logSuccess,
  randomInt,
  randomDelay,
  pickUserAgent,
  pickViewport,
  pickTimezone,
  parseProxyUrl,
  cityToSlug,
  parsePriceNumber,
  dedupeByHref,
  resultsPath,
  initSessionResultsOutput,
};
