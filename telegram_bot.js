const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

// Minimal .env loader (so VPS can keep secrets in /opt/ifind/.env)
function loadDotEnvIfPresent() {
  try {
    const envPath = path.join(process.cwd(), '.env');
    if (!fs.existsSync(envPath)) return;
    const raw = fs.readFileSync(envPath, 'utf8');
    for (const line of raw.split(/\r?\n/)) {
      const t = line.trim();
      if (!t || t.startsWith('#')) continue;
      const eq = t.indexOf('=');
      if (eq <= 0) continue;
      const key = t.slice(0, eq).trim();
      let val = t.slice(eq + 1).trim();
      if (
        (val.startsWith('"') && val.endsWith('"')) ||
        (val.startsWith("'") && val.endsWith("'"))
      ) {
        val = val.slice(1, -1);
      }
      if (process.env[key] == null || process.env[key] === '') process.env[key] = val;
    }
  } catch (_) {
    // ignore
  }
}

loadDotEnvIfPresent();

const token = process.env.TELEGRAM_BOT_TOKEN || process.env.BOT_TOKEN;
if (!token) {
  console.error('Ошибка: не задан TELEGRAM_BOT_TOKEN (или BOT_TOKEN).');
  process.exit(1);
}

const TG_API = `https://api.telegram.org/bot${token}`;
const SUPABASE_URL = (process.env.SUPABASE_URL || '').trim().replace(/\/+$/, '');
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || '';

const PROJECT_DIR = __dirname; // telegram_bot.js лежит в корне проекта
const XLSX_GLOB_PREFIX = 'results_';
const XLSX_GLOB_SUFFIX = '.xlsx';

let offset = Number(process.env.TELEGRAM_BOT_OFFSET || 0);
let running = true;

const MENU = {
  manualRun: '🚀 Ручной запуск',
  autoSettings: '⚙️ Настройки автопарсинга',
  guide: '📘 Инструкция',
  excels: '📁 Эксель файлы',
  backToMenu: '⬅️ Назад в меню',
};

const STOP_PARSING_TEXT = '⛔ Остановить парсинг';

function buildStopKeyboard() {
  return {
    keyboard: [[{ text: STOP_PARSING_TEXT }]],
    resize_keyboard: true,
    one_time_keyboard: false,
  };
}

const mainKeyboard = {
  keyboard: [
    [{ text: MENU.manualRun }, { text: MENU.autoSettings }],
    [{ text: MENU.guide }, { text: MENU.excels }],
  ],
  resize_keyboard: true,
  one_time_keyboard: false,
};

const startText =
  '🤖 Бот помогает находить выгодные iPhone на Авито и Wildberries.\n\n' +
  '📌 Нажми кнопку `Ручной запуск` и выбери площадку.\n\n' +
  '⚙️ В `Настройки автопарсинга` задай фильтры кнопками.\n\n' +
  '📁 В `Эксель файлы` получишь список файлов на сервере.';

const yesNoKeyboard = (yesText, noText) => ({
  keyboard: [[{ text: yesText }, { text: noText }]],
  resize_keyboard: true,
  one_time_keyboard: false,
});

const WIZARD_YES = '✅ Да';
const WIZARD_NO = '❌ Нет';
const WIZARD_SKIP = '⏭️ Пропустить';
const WIZARD_START = '🚀 Запустить парсинг';
const SETTINGS_EDIT = '✏️ Изменить настройки';
const SETTINGS_AVITO = '🛒 Настроить Авито';
const SETTINGS_WB = '🛍️ Настроить ВБ';
const SETTINGS_SAVE = '💾 Сохранить настройки';
const RUN_CONTINUE_AUTO = '▶️ Продолжить с настройками автопарсинга';
const RUN_SET_MANUAL = '✍️ Задать вручную';

function wizardYesNoKeyboard() {
  return yesNoKeyboard(WIZARD_YES, WIZARD_NO);
}

function confirmKeyboard(configureOnly) {
  return {
    keyboard: [[{ text: configureOnly ? SETTINGS_SAVE : WIZARD_START }, { text: MENU.backToMenu }]],
    resize_keyboard: true,
    one_time_keyboard: false,
  };
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function isRetryableNetErr(err) {
  const s = String(err?.message || err || '').toUpperCase();
  return (
    s.includes('ECONNRESET') ||
    s.includes('ETIMEDOUT') ||
    s.includes('EAI_AGAIN') ||
    s.includes('ENOTFOUND') ||
    s.includes('FETCH FAILED') ||
    s.includes('NETWORK')
  );
}

async function tgApi(method, payload) {
  let lastErr = null;
  for (let attempt = 1; attempt <= 5; attempt += 1) {
    try {
      const isFormData = typeof FormData !== 'undefined' && payload instanceof FormData;
      const resp = await fetch(`${TG_API}/${method}`, {
        method: 'POST',
        headers: isFormData ? undefined : { 'content-type': 'application/json' },
        body: isFormData ? payload : JSON.stringify(payload || {}),
      });
      const data = await resp.json();
      if (!resp.ok || !data.ok) {
        throw new Error(data.description || `HTTP ${resp.status}`);
      }
      return data.result;
    } catch (err) {
      lastErr = err;
      const msg = String(err?.message || err || '');
      console.error(`Telegram API ошибка ${method} (${attempt}/5):`, msg);
      if (!isRetryableNetErr(err)) break;
      await sleep(700 * attempt);
    }
  }
  throw lastErr;
}

async function sendMessage(chatId, text, replyMarkup, parseMode = null) {
  const payload = {
    chat_id: chatId,
    text,
    reply_markup: replyMarkup || mainKeyboard,
  };
  if (parseMode) payload.parse_mode = parseMode;
  return tgApi('sendMessage', payload);
}

async function sendDocument(chatId, filePath, caption) {
  // Optional feature: if later you want to send actual xlsx files.
  // Telegram file upload: multipart/form-data.
  const file = fs.readFileSync(filePath);
  const form = new FormData();
  form.append('chat_id', String(chatId));
  form.append('caption', caption || '');
  form.append('document', new Blob([file]), path.basename(filePath));
  return tgApi('sendDocument', form);
}

// In-memory user state (for VPS single instance; if restart needed -> later can persist).
// Minimal: marketplace + a few boolean/static parameters.
const userStateByChatId = new Map();

function getUserState(chatId) {
  if (!userStateByChatId.has(chatId)) {
    userStateByChatId.set(chatId, {
      settings: {
        query: 'iPhone',
        extraKeywords: '',
        city: 'moskva',
        marketplaceDefault: 'avito',
        onlyToday: false,
        priceFilterEnabled: false,
        minPrice: 0,
        maxPrice: 0,
        memoryFilterEnabled: false,
        memoryGb: '',
        colorFilterEnabled: false,
        color: '',
        sellerType: 'any',
        minRating: 0,
      },
      stage: 'main', // main | choosing_market | settings_only_today | settings_price | settings_memory | settings_color
      runDraft: null,
      configureOnly: false,
      selectedMarketplaceForRun: null,
      marketSettings: {
        avito: null,
        wb: null,
      },
    });
  }
  return userStateByChatId.get(chatId);
}

function buildMarketplaceKeyboard() {
  // EXACTLY 2 buttons as requested
  return {
    keyboard: [[{ text: '🛒 Авито' }, { text: '🛍️ ВБ' }]],
    resize_keyboard: true,
    one_time_keyboard: false,
  };
}

const COLOR_BUTTONS = [
  'Бежевый',
  'Белый',
  'Голубой',
  'Желтый',
  'Зеленый',
  'Коричневый',
  'Красный',
  'Оранжевый',
  'Розовый',
  'Серый',
  'Синий',
  'Фиолетовый',
  'Черный',
];

function buildColorKeyboard() {
  // 2 columns grid
  const rows = [];
  for (let i = 0; i < COLOR_BUTTONS.length; i += 2) {
    const left = COLOR_BUTTONS[i];
    const right = COLOR_BUTTONS[i + 1];
    if (right) rows.push([{ text: left }, { text: right }]);
    else rows.push([{ text: left }]);
  }
  // add "без фильтра" at the end
  rows.push([{ text: 'Без фильтра' }]);
  return { keyboard: rows, resize_keyboard: true, one_time_keyboard: false };
}

function buildMemoryKeyboard() {
  return {
    keyboard: [
      [{ text: '128' }, { text: '256' }],
      [{ text: '512' }, { text: '1ТБ' }],
      [{ text: 'Любая' }],
    ],
    resize_keyboard: true,
    one_time_keyboard: false,
  };
}

function buildPriceKeyboard() {
  // Static range buttons to keep it simple on weak VPS
  return {
    keyboard: [
      [{ text: 'Цена: БЕЗ фильтра' }, { text: 'Цена: 43000-120000' }],
    ],
    resize_keyboard: true,
    one_time_keyboard: false,
  };
}

function buildOnlyTodayKeyboard() {
  return yesNoKeyboard('Только за сегодня: ДА', 'Только за сегодня: НЕТ');
}

function buildSettingsKeyboard(settings, opts = {}) {
  const launchMode = Boolean(opts.launchMode);
  const lastRow = launchMode ? [{ text: 'Запустить парсинг' }, { text: MENU.backToMenu }] : [{ text: MENU.backToMenu }];

  const onlyTodayYes = settings.onlyToday ? 'Только за сегодня: ДА' : 'Только за сегодня: НЕТ';
  const onlyTodayNo = settings.onlyToday ? 'Только за сегодня: НЕТ' : 'Только за сегодня: ДА';
  const priceYes = settings.priceFilterEnabled ? 'Цена: 43000-120000' : 'Цена: БЕЗ фильтра';
  const priceNo = settings.priceFilterEnabled ? 'Цена: БЕЗ фильтра' : 'Цена: 43000-120000';

  const memYes = settings.memoryFilterEnabled ? 'Память: ДА' : 'Память: НЕТ';
  const memNo = settings.memoryFilterEnabled ? 'Память: НЕТ' : 'Память: ДА';

  const colorYes = settings.colorFilterEnabled ? 'Цвет WB: ДА' : 'Цвет WB: НЕТ';
  const colorNo = settings.colorFilterEnabled ? 'Цвет WB: НЕТ' : 'Цвет WB: ДА';

  return {
    keyboard: [
      [{ text: onlyTodayYes }, { text: onlyTodayNo }],
      [{ text: priceYes }, { text: priceNo }],
      [{ text: memYes }, { text: memNo }],
      [{ text: colorYes }, { text: colorNo }],
      lastRow,
    ],
    resize_keyboard: true,
    one_time_keyboard: false,
  };
}

function listExcelFilesOnServer() {
  let files = [];
  try {
    const all = fs.readdirSync(PROJECT_DIR);
    files = all
      .filter((fn) => fn.startsWith(XLSX_GLOB_PREFIX) && fn.endsWith(XLSX_GLOB_SUFFIX))
      .map((fn) => {
        const full = path.join(PROJECT_DIR, fn);
        const st = fs.statSync(full);
        return {
          fileName: fn,
          filePath: full,
          sizeBytes: st.size,
          mtimeMs: st.mtimeMs,
        };
      });
  } catch (_) {
    // ignore
  }
  files.sort((a, b) => b.mtimeMs - a.mtimeMs);
  return files;
}

function formatBytes(n) {
  if (!Number.isFinite(n)) return '-';
  const units = ['B', 'KB', 'MB', 'GB'];
  let idx = 0;
  let val = n;
  while (val >= 1024 && idx < units.length - 1) {
    val /= 1024;
    idx += 1;
  }
  return `${val.toFixed(idx === 0 ? 0 : 1)} ${units[idx]}`;
}

async function supabaseUpsertUser(tgUserId, from) {
  const hasSupabaseCfg = Boolean(SUPABASE_URL && SUPABASE_ANON_KEY);
  if (!hasSupabaseCfg || !tgUserId) return;

  const payload = [
    {
      telegram_user_id: String(tgUserId),
      telegram_username: from.username || null,
      first_name: from.first_name || null,
      last_name: from.last_name || null,
    },
  ];

  const resp = await fetch(`${SUPABASE_URL}/rest/v1/users`, {
    method: 'POST',
    headers: {
      apikey: SUPABASE_ANON_KEY,
      'content-type': 'application/json',
      Prefer: 'resolution=merge-duplicates,return=minimal',
    },
    body: JSON.stringify(payload),
  });
  if (!resp.ok) {
    const txt = await resp.text().catch(() => '');
    throw new Error(`Supabase users upsert failed: HTTP ${resp.status}: ${txt}`);
  }
}

async function supabaseFetchUserSettings(tgUserId) {
  const hasSupabaseCfg = Boolean(SUPABASE_URL && SUPABASE_ANON_KEY);
  if (!hasSupabaseCfg || !tgUserId) return null;

  const qs = new URLSearchParams({
    telegram_user_id: `eq.${String(tgUserId)}`,
    select:
      'telegram_user_id,marketplace_default,query,extra_keywords,city,min_price,max_price,memory,color,only_today,seller_type,min_rating',
    limit: '1',
  });
  const resp = await fetch(`${SUPABASE_URL}/rest/v1/user_settings?${qs.toString()}`, {
    headers: {
      apikey: SUPABASE_ANON_KEY,
      'content-type': 'application/json',
    },
  });
  if (!resp.ok) {
    const txt = await resp.text().catch(() => '');
    throw new Error(`Supabase user_settings select failed: HTTP ${resp.status}: ${txt}`);
  }
  const rows = await resp.json();
  return Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
}

async function supabaseUpsertUserSettings(tgUserId, s) {
  const hasSupabaseCfg = Boolean(SUPABASE_URL && SUPABASE_ANON_KEY);
  if (!hasSupabaseCfg || !tgUserId) return;

  const payload = [
    {
      telegram_user_id: String(tgUserId),
      marketplace_default: s.marketplaceDefault || 'avito',
      query: s.query || 'iPhone',
      extra_keywords: s.extraKeywords || '',
      city: s.city || 'moskva',
      min_price: Number.isFinite(Number(s.minPrice)) ? Number(s.minPrice) : 0,
      max_price: Number.isFinite(Number(s.maxPrice)) ? Number(s.maxPrice) : 0,
      memory: s.memoryGb || '',
      color: s.color || '',
      only_today: Boolean(s.onlyToday),
      seller_type: s.sellerType || 'any',
      min_rating: Number.isFinite(Number(s.minRating)) ? Number(s.minRating) : 0,
    },
  ];

  const resp = await fetch(`${SUPABASE_URL}/rest/v1/user_settings`, {
    method: 'POST',
    headers: {
      apikey: SUPABASE_ANON_KEY,
      'content-type': 'application/json',
      Prefer: 'resolution=merge-duplicates,return=minimal',
    },
    body: JSON.stringify(payload),
  });
  if (!resp.ok) {
    const txt = await resp.text().catch(() => '');
    throw new Error(`Supabase user_settings upsert failed: HTTP ${resp.status}: ${txt}`);
  }
}

async function supabaseFetchMarketSettingsAll(tgUserId) {
  const hasSupabaseCfg = Boolean(SUPABASE_URL && SUPABASE_ANON_KEY);
  if (!hasSupabaseCfg || !tgUserId) return [];
  const qs = new URLSearchParams({
    telegram_user_id: `eq.${String(tgUserId)}`,
    select: 'telegram_user_id,marketplace,query,extra_keywords,city,min_price,max_price,memory,color,only_today',
  });
  const resp = await fetch(`${SUPABASE_URL}/rest/v1/user_market_settings?${qs.toString()}`, {
    headers: {
      apikey: SUPABASE_ANON_KEY,
      'content-type': 'application/json',
    },
  });
  if (!resp.ok) {
    const txt = await resp.text().catch(() => '');
    throw new Error(`Supabase user_market_settings select failed: HTTP ${resp.status}: ${txt}`);
  }
  const rows = await resp.json();
  return Array.isArray(rows) ? rows : [];
}

async function supabaseUpsertMarketSettings(tgUserId, marketplace, s) {
  const hasSupabaseCfg = Boolean(SUPABASE_URL && SUPABASE_ANON_KEY);
  if (!hasSupabaseCfg || !tgUserId) return;
  const payload = [
    {
      telegram_user_id: String(tgUserId),
      marketplace,
      query: s.query || 'iPhone',
      extra_keywords: s.extraKeywords || '',
      city: s.city || 'moskva',
      min_price: Number.isFinite(Number(s.minPrice)) ? Number(s.minPrice) : 0,
      max_price: Number.isFinite(Number(s.maxPrice)) ? Number(s.maxPrice) : 0,
      memory: s.memory || '',
      color: s.color || '',
      only_today: Boolean(s.onlyToday),
    },
  ];
  const resp = await fetch(
    `${SUPABASE_URL}/rest/v1/user_market_settings?on_conflict=telegram_user_id,marketplace`,
    {
    method: 'POST',
    headers: {
      apikey: SUPABASE_ANON_KEY,
      'content-type': 'application/json',
      Prefer: 'resolution=merge-duplicates,return=minimal',
    },
    body: JSON.stringify(payload),
    }
  );
  if (!resp.ok) {
    const txt = await resp.text().catch(() => '');
    throw new Error(`Supabase user_market_settings upsert failed: HTTP ${resp.status}: ${txt}`);
  }
}

function mapRowToMarketSettings(row) {
  return {
    query: row.query || 'iPhone',
    extraKeywords: row.extra_keywords || '',
    city: row.city || 'moskva',
    minPrice: Number(row.min_price || 0),
    maxPrice: Number(row.max_price || 0),
    memory: row.memory || '',
    color: row.color || '',
    onlyToday: Boolean(row.only_today),
  };
}

function formatMarketSettingsBlock(title, s) {
  if (!s) {
    return `${title}:\nтут пока что пусто`;
  }
  return (
    `${title}:\n` +
    `1) Запрос: ${s.query || '—'}\n` +
    `2) Модель: ${s.extraKeywords || '—'}\n` +
    `3) Город: ${s.city || '—'}\n` +
    `4) Цена: ${s.minPrice || 0} - ${s.maxPrice || 0}\n` +
    `5) Память: ${s.memory || '—'}\n` +
    `6) Цвет: ${s.color || '—'}\n` +
    `7) Только сегодня: ${s.onlyToday ? 'ДА' : 'НЕТ'}`
  );
}

function formatWbSettingsBlock(title, s) {
  if (!s) {
    return `${title}:\nтут пока что пусто`;
  }
  return (
    `${title}:\n` +
    `1) Запрос: ${s.query || '—'}\n` +
    `2) Модель: ${s.extraKeywords || '—'}\n` +
    `3) Цена: ${s.minPrice || 0} - ${s.maxPrice || 0}\n` +
    `4) Память: ${s.memory || '—'}\n` +
    `5) Цвет: ${s.color || '—'}`
  );
}

async function supabaseUpsertExcelFile({ telegramUserId, fileName, filePath, marketplace }) {
  const hasSupabaseCfg = Boolean(SUPABASE_URL && SUPABASE_ANON_KEY);
  if (!hasSupabaseCfg) return;

  const payload = [
    {
      telegram_user_id: telegramUserId != null ? String(telegramUserId) : null,
      file_name: fileName,
      file_path: filePath,
      marketplace: marketplace || null,
    },
  ];

  const resp = await fetch(`${SUPABASE_URL}/rest/v1/excel_files?on_conflict=file_path`, {
    method: 'POST',
    headers: {
      apikey: SUPABASE_ANON_KEY,
      'content-type': 'application/json',
      Prefer: 'resolution=merge-duplicates,return=minimal',
    },
    body: JSON.stringify(payload),
  });
  if (!resp.ok) {
    const txt = await resp.text().catch(() => '');
    if (resp.status === 409) return;
    throw new Error(`Supabase excel_files upsert failed: HTTP ${resp.status}: ${txt}`);
  }
}

let isParsing = false;
let lastRunMarketplace = null;
const activeChildByChatId = new Map(); // chatId -> child process
const stopRequestedByChatId = new Map(); // chatId -> boolean

function buildParserEnvForRun({ chatId, marketplace, settings, parserOverrides = null }) {
  const o = parserOverrides || {};
  const runMarketplace = o.marketplace || marketplace;
  const isWbOnly = runMarketplace === 'wb';
  // main.js переключаем в env mode (без readline)
  return {
    ...process.env,
    PARSER_USE_ENV: '1',
    PARSER_MARKETPLACE: runMarketplace,
    // Always run unattended (no manual proxy prompts) for VPS bot runs
    AVITO_MANUAL_PROXY: '0',
    AVITO_WAIT_ENTER: '0',
    // Ensure no WB home warmup (avoid going to WB home)
    WB_USE_HOME_WARMUP: '0',
    // Force headless by default on VPS
    PLAYWRIGHT_HEADLESS: '1',
    // Keep memory stable on 1GB RAM VPS
    NODE_OPTIONS: process.env.NODE_OPTIONS || '--max-old-space-size=384',
    PARSER_QUERY: o.query != null ? String(o.query) : process.env.PARSER_QUERY || 'iPhone 15',
    PARSER_EXTRA_KEYWORDS: o.extraKeywords != null ? String(o.extraKeywords) : process.env.PARSER_EXTRA_KEYWORDS || '',
    PARSER_CITY: isWbOnly ? '' : o.city != null ? String(o.city) : process.env.PARSER_CITY || 'moskva',
    PARSER_MIN_PRICE: o.minPrice != null ? String(o.minPrice) : settings.priceFilterEnabled ? String(settings.minPrice) : '0',
    PARSER_MAX_PRICE: o.maxPrice != null ? String(o.maxPrice) : settings.priceFilterEnabled ? String(settings.maxPrice) : '0',
    PARSER_MEMORY: o.memory != null ? String(o.memory) : settings.memoryFilterEnabled ? String(settings.memoryGb) : '',
    PARSER_SELLER_TYPE: settings.sellerType || 'any',
    PARSER_MIN_RATING: String(settings.minRating || 0),
    PARSER_ONLY_TODAY: isWbOnly
      ? '0'
      : o.onlyToday != null
        ? o.onlyToday
          ? '1'
          : '0'
        : settings.onlyToday
          ? '1'
          : '0',
    PARSER_COLOR:
      o.color != null
        ? String(o.color)
        : marketplace === 'wb' || marketplace === 'both'
          ? settings.colorFilterEnabled
            ? settings.color
            : ''
          : '',
  };
}

async function runParserForMarketplace({ chatId, marketplace, parserOverrides = null }) {
  if (isParsing) {
    await sendMessage(chatId, 'Парсинг уже запущен. Подождите, пожалуйста.');
    return;
  }

  isParsing = true;
  lastRunMarketplace = marketplace;

  const state = getUserState(chatId);
  const settings = state.settings;
  state.stage = 'parsing';

  const stopKeyboard = buildStopKeyboard();
  let stage1Sent = false;
  let stage2Sent = false;
  let stage3Emitted = false;

  const beforeFiles = listExcelFilesOnServer();
  const beforePaths = new Set(beforeFiles.map((f) => f.filePath));

  await sendMessage(chatId, `🟢 Парсинг запущен: ${marketplace === 'avito' ? 'Авито' : 'ВБ'}.\nНажми «${STOP_PARSING_TEXT}».`, stopKeyboard);

  // Run main.js directly so we can stop it and stream stdout/stderr.
  const child = spawn('node', ['main.js'], {
    cwd: PROJECT_DIR,
    env: buildParserEnvForRun({ chatId, marketplace, settings, parserOverrides }),
    stdio: ['ignore', 'pipe', 'pipe'],
    detached: false,
  });

  activeChildByChatId.set(chatId, child);

  function extractResultsFile(line) {
    const m = line && line.match(/(results_[^\s]+\.xlsx)/i);
    return m ? m[1] : null;
  }

  function handleParserLine(line) {
    const s = String(line || '').trimEnd();
    if (!s) return;

    // В Telegram отправляем только 3 этапа, подробные строки остаются в terminal/journalctl.
    if (!stage3Emitted && !stage1Sent && (s.includes('Адрес поиска') || s.includes('Переход на') || s.includes('Открываю страницу'))) {
      stage1Sent = true;
      sendMessage(chatId, '🌐 Захожу на страницу', stopKeyboard).catch(() => {});
    }

    if (
      !stage3Emitted &&
      !stage2Sent &&
      (s.includes('Парсинг страницы выдачи') ||
        s.includes('Ожидание карточек') ||
        s.includes('Карточки товаров') ||
        s.includes('Скролл ленты') ||
        s.includes('Парсинг карточек'))
    ) {
      stage2Sent = true;
      sendMessage(chatId, '✅ Страница открыта, собираю данные', stopKeyboard).catch(() => {});
    }
  }

  let stdoutBuffer = '';
  child.stdout.on('data', (data) => {
    try {
      process.stdout.write(data.toString('utf8'));
    } catch (_) {
      /* ignore */
    }
    stdoutBuffer += data.toString('utf8');
    const parts = stdoutBuffer.split(/\r?\n/);
    stdoutBuffer = parts.pop() || '';
    for (const line of parts) handleParserLine(line);
  });
  let stderrBuffer = '';
  child.stderr.on('data', (data) => {
    try {
      process.stderr.write(data.toString('utf8'));
    } catch (_) {
      /* ignore */
    }
    stderrBuffer += data.toString('utf8');
    const parts = stderrBuffer.split(/\r?\n/);
    stderrBuffer = parts.pop() || '';
    for (const line of parts) handleParserLine(line);
  });

  child.on('exit', async (code) => {
    activeChildByChatId.delete(chatId);
    state.stage = 'main';
    const stopped = Boolean(stopRequestedByChatId.get(chatId));
    stopRequestedByChatId.delete(chatId);
    if (stopped) {
      try {
        await sendMessage(chatId, '⛔ Парсинг остановлен.', mainKeyboard);
      } catch (_) {
        /* ignore */
      } finally {
        isParsing = false;
        lastRunMarketplace = null;
      }
      return;
    }
    try {
      const afterFiles = listExcelFilesOnServer();
      const newOnes = afterFiles.filter((f) => !beforePaths.has(f.filePath));
      // store excel metadata to DB (best-effort)
      if (SUPABASE_URL && SUPABASE_ANON_KEY && newOnes.length > 0) {
        for (const f of newOnes) {
          try {
            await supabaseUpsertExcelFile({
              telegramUserId: chatId,
              fileName: f.fileName,
              filePath: f.filePath,
              marketplace,
            });
          } catch (e) {
            console.error('Excel upsert error:', String(e?.message || e || ''));
          }
        }
      }

      stage3Emitted = true;
      const bestFile = newOnes.length > 0 ? newOnes[0].fileName : null;
      await sendMessage(
        chatId,
        bestFile
          ? `✅ Парсинг завершен, ваш файл сохранен: ${bestFile}`
          : '⚠️ Парсинг завершен, но новый файл Excel не найден.',
        mainKeyboard
      );
    } catch (e) {
      console.error('After parse handler error:', String(e?.message || e || ''));
      await sendMessage(chatId, `Ошибка после завершения парсинга: ${String(e?.message || e || '')}`, mainKeyboard);
    } finally {
      isParsing = false;
      lastRunMarketplace = null;
    }
  });
}

async function syncExcelFilesToDb(chatId) {
  const files = listExcelFilesOnServer();
  const telegramUserId = String(chatId);
  if (!files.length) return;

  if (!(SUPABASE_URL && SUPABASE_ANON_KEY)) return;

  for (const f of files) {
    try {
      await supabaseUpsertExcelFile({
        telegramUserId,
        fileName: f.fileName,
        filePath: f.filePath,
        marketplace: null,
      });
    } catch (e) {
      console.error('Excel sync upsert error:', String(e?.message || e || ''));
    }
  }
}

async function handleMessage(msg) {
  if (!msg || !msg.chat || typeof msg.chat.id !== 'number') return;
  const chatId = msg.chat.id;
  const text = String(msg.text || '').trim();
  if (!text) return;

  const state = getUserState(chatId);

  // Stop parsing button (while parser is running)
  if (state.stage === 'parsing' && text === STOP_PARSING_TEXT) {
    const child = activeChildByChatId.get(chatId);
    if (child) {
      stopRequestedByChatId.set(chatId, true);
      try {
        child.kill('SIGINT');
      } catch (_) {
        /* ignore */
      }
      setTimeout(() => {
        try {
          child.kill('SIGKILL');
        } catch (_) {
          /* ignore */
        }
      }, 10_000);
    }
    activeChildByChatId.delete(chatId);
    isParsing = false;
    lastRunMarketplace = null;
    state.stage = 'main';
    await sendMessage(chatId, '⛔ Парсинг остановлен.', mainKeyboard);
    return;
  }

  // Upsert Telegram user in DB on /start
  if (/^\/start(?:@\w+)?(?:\s+.*)?$/i.test(text)) {
    try {
      await supabaseUpsertUser(msg.from?.id, msg.from || {});
      const saved = await supabaseFetchUserSettings(msg.from?.id);
      if (saved) {
        state.settings.query = saved.query || state.settings.query;
        state.settings.extraKeywords = saved.extra_keywords || '';
        state.settings.city = saved.city || state.settings.city;
        state.settings.marketplaceDefault = saved.marketplace_default || 'avito';
        state.settings.minPrice = Number(saved.min_price || 0);
        state.settings.maxPrice = Number(saved.max_price || 0);
        state.settings.priceFilterEnabled = state.settings.minPrice > 0 || state.settings.maxPrice > 0;
        state.settings.memoryGb = saved.memory || '';
        state.settings.memoryFilterEnabled = Boolean(state.settings.memoryGb);
        state.settings.color = saved.color || '';
        state.settings.colorFilterEnabled = Boolean(state.settings.color);
        state.settings.onlyToday = Boolean(saved.only_today);
        state.settings.sellerType = saved.seller_type || 'any';
        state.settings.minRating = Number(saved.min_rating || 0);
      }
    } catch (e) {
      console.error('Supabase user upsert error:', String(e?.message || e || ''));
    }
    await sendMessage(chatId, startText, mainKeyboard);
    state.stage = 'main';
    return;
  }

  // Main menu buttons
  if (text === MENU.manualRun) {
    state.stage = 'choosing_market';
    state.runDraft = null;
    state.selectedMarketplaceForRun = null;
    await sendMessage(chatId, '🚀 Выберите площадку для ручного запуска:', buildMarketplaceKeyboard());
    return;
  }

  if (text === MENU.autoSettings) {
    try {
      const rows = await supabaseFetchMarketSettingsAll(msg.from?.id);
      const avitoRow = rows.find((r) => String(r.marketplace || '').toLowerCase() === 'avito');
      const wbRow = rows.find((r) => String(r.marketplace || '').toLowerCase() === 'wb');
      state.marketSettings.avito = avitoRow ? mapRowToMarketSettings(avitoRow) : null;
      state.marketSettings.wb = wbRow ? mapRowToMarketSettings(wbRow) : null;

      await sendMessage(
        chatId,
        `⚙️ Ваши настройки площадок:\n\n` +
          `${formatMarketSettingsBlock('🛒 Настройки Авито', state.marketSettings.avito)}\n\n` +
          `${formatWbSettingsBlock('🛍️ Настройки ВБ', state.marketSettings.wb)}`,
        {
          keyboard: [[{ text: SETTINGS_AVITO }, { text: SETTINGS_WB }], [{ text: MENU.backToMenu }]],
          resize_keyboard: true,
          one_time_keyboard: false,
        }
      );
    } catch (e) {
      await sendMessage(chatId, `⚠️ Ошибка чтения настроек: ${String(e?.message || e || '')}`, mainKeyboard);
    }
    return;
  }

  if (text === MENU.guide) {
    state.stage = 'main';
    await sendMessage(
      chatId,
      '📘 *Инструкция*\n\n' +
        '1️⃣ Нажми _⚙️ Настройки автопарсинга_ и заполни параметры.\n' +
        '2️⃣ Нажми _🚀 Ручной запуск_ и выбери площадку _🛒 Авито_ или _🛍️ ВБ_.\n' +
        '3️⃣ Дождись этапов парсинга:\n' +
        '   • 🌐 Открываю страницу\n' +
        '   • ✅ Страница открыта, начинаю парсинг\n' +
        '   • 📁 Парсинг завершен, файл сохранен\n' +
        '4️⃣ Нажми _📁 Эксель файлы_, чтобы увидеть список файлов на сервере.\n\n' +
        '🧷 Во время парсинга можно нажать _⛔ Остановить парсинг_.',
      mainKeyboard,
      'Markdown'
    );
    return;
  }

  if (text === MENU.excels) {
    state.stage = 'main';
    const files = listExcelFilesOnServer();
    if (!files.length) {
      await sendMessage(chatId, 'Excel-файлов пока нет. Запусти парсинг и потом нажми `Эксель файлы`.', mainKeyboard);
      return;
    }

    // best-effort sync to DB
    await syncExcelFilesToDb(chatId);

    const listText =
      'На сервере найдено Excel файлов: \n' +
      files
        .slice(0, 30)
        .map((f) => `• ${f.fileName} (${formatBytes(f.sizeBytes)})`)
        .join('\n') +
      (files.length > 30 ? `\n... и ещё ${files.length - 30}` : '');

    await sendMessage(chatId, listText, mainKeyboard);
    try {
      const latest = files[0];
      await sendDocument(chatId, latest.filePath, `📎 Файл: ${latest.fileName}`);
    } catch (e) {
      console.error('Send excel document error:', String(e?.message || e || ''));
      await sendMessage(chatId, '⚠️ Не удалось отправить файл в Telegram, но он сохранен на сервере.', mainKeyboard);
    }
    return;
  }

  if (text === SETTINGS_AVITO || text === SETTINGS_WB || text === SETTINGS_EDIT) {
    state.configureOnly = true;
    const m = text === SETTINGS_WB ? 'wb' : 'avito';
    const current = m === 'wb' ? state.marketSettings.wb : state.marketSettings.avito;
    state.runDraft = {
      marketplace: m,
      query: current?.query || 'iPhone',
      extraKeywords: current?.extraKeywords || '',
      city: current?.city || 'moskva',
      minPrice: Number(current?.minPrice || 0),
      maxPrice: Number(current?.maxPrice || 0),
      memory: current?.memory || '',
      onlyToday: Boolean(current?.onlyToday),
      color: current?.color || '',
    };
    state.stage = 'run_query';
    await sendMessage(chatId, `⚙️ Настройка ${m === 'avito' ? 'Авито' : 'ВБ'}.\n📝 Шаг 1: Введите название поиска (например: iPhone):`, { keyboard: [[{ text: WIZARD_SKIP }]], resize_keyboard: true, one_time_keyboard: false });
    return;
  }

  if (text === MENU.backToMenu) {
    state.stage = 'main';
    state.configureOnly = false;
    state.runDraft = null;
    await sendMessage(chatId, '⬅️ Возврат в меню.', mainKeyboard);
    return;
  }

  // Stage: choosing marketplace
  if (state.stage === 'choosing_market') {
    if (text === '🛒 Авито' || text === 'Авито') {
      state.selectedMarketplaceForRun = 'avito';
      state.stage = 'run_mode_choice';
      await sendMessage(
        chatId,
        '🛒 Авито выбрано. Как продолжить?',
        {
          keyboard: [[{ text: RUN_CONTINUE_AUTO }], [{ text: RUN_SET_MANUAL }], [{ text: MENU.backToMenu }]],
          resize_keyboard: true,
          one_time_keyboard: false,
        }
      );
      return;
    }
    if (text === '🛍️ ВБ' || text === 'ВБ') {
      state.selectedMarketplaceForRun = 'wb';
      state.stage = 'run_mode_choice';
      await sendMessage(
        chatId,
        '🛍️ ВБ выбрано. Как продолжить?',
        {
          keyboard: [[{ text: RUN_CONTINUE_AUTO }], [{ text: RUN_SET_MANUAL }], [{ text: MENU.backToMenu }]],
          resize_keyboard: true,
          one_time_keyboard: false,
        }
      );
      return;
    }
    await sendMessage(chatId, '👇 Выбери площадку кнопкой снизу.', buildMarketplaceKeyboard());
    return;
  }

  if (state.stage === 'run_mode_choice') {
    const m = state.selectedMarketplaceForRun || 'avito';
    if (text === RUN_SET_MANUAL) {
      const current = m === 'wb' ? state.marketSettings.wb : state.marketSettings.avito;
      state.runDraft = {
        marketplace: m,
        query: current?.query || 'iPhone',
        extraKeywords: current?.extraKeywords || '',
        city: current?.city || 'moskva',
        minPrice: Number(current?.minPrice || 0),
        maxPrice: Number(current?.maxPrice || 0),
        memory: current?.memory || '',
        onlyToday: Boolean(current?.onlyToday),
        color: current?.color || '',
      };
      state.stage = 'run_query';
      await sendMessage(
        chatId,
        `📝 Шаг 1/${m === 'avito' ? '8' : '7'}: Введите название поиска (например: iPhone):`,
        { keyboard: [[{ text: WIZARD_SKIP }]], resize_keyboard: true, one_time_keyboard: false }
      );
      return;
    }

    if (text === RUN_CONTINUE_AUTO) {
      // pull latest settings from DB for selected marketplace
      try {
        const rows = await supabaseFetchMarketSettingsAll(msg.from?.id);
        const row = rows.find((r) => String(r.marketplace || '').toLowerCase() === m);
        const cached = m === 'wb' ? state.marketSettings.wb : state.marketSettings.avito;
        const s = row ? mapRowToMarketSettings(row) : cached;
        if (!s) {
          await sendMessage(
            chatId,
            `⚠️ Для площадки ${m === 'avito' ? 'Авито' : 'ВБ'} настройки пока пустые.\nВыберите "✍️ Задать вручную" или откройте "⚙️ Настройки автопарсинга".`,
            {
              keyboard: [[{ text: RUN_SET_MANUAL }], [{ text: MENU.autoSettings }], [{ text: MENU.backToMenu }]],
              resize_keyboard: true,
              one_time_keyboard: false,
            }
          );
          return;
        }

        await runParserForMarketplace({
          chatId,
          marketplace: m,
          parserOverrides: {
            marketplace: m,
            query: s.query || 'iPhone',
            extraKeywords: s.extraKeywords || '',
            city: s.city || 'moskva',
            minPrice: s.minPrice || 0,
            maxPrice: s.maxPrice || 0,
            memory: s.memory || '',
            onlyToday: Boolean(s.onlyToday),
            color: s.color || '',
          },
        });
        return;
      } catch (e) {
        await sendMessage(chatId, `⚠️ Не удалось прочитать настройки: ${String(e?.message || e || '')}`, mainKeyboard);
        return;
      }
    }

    if (text === MENU.backToMenu) {
      state.stage = 'main';
      state.selectedMarketplaceForRun = null;
      await sendMessage(chatId, '⬅️ Возврат в меню.', mainKeyboard);
      return;
    }

    await sendMessage(chatId, '👇 Выберите вариант продолжения кнопкой снизу.', {
      keyboard: [[{ text: RUN_CONTINUE_AUTO }], [{ text: RUN_SET_MANUAL }], [{ text: MENU.backToMenu }]],
      resize_keyboard: true,
      one_time_keyboard: false,
    });
    return;
  }

  if (state.stage === 'run_query') {
    state.runDraft.query = text === WIZARD_SKIP ? 'iPhone' : text;
    state.stage = 'run_model';
    await sendMessage(chatId, '📝 Шаг 2/7: Введите модель (например: 15 Pro Max) или пропустите:', { keyboard: [[{ text: WIZARD_SKIP }]], resize_keyboard: true, one_time_keyboard: false });
    return;
  }

  if (state.stage === 'run_model') {
    state.runDraft.extraKeywords = text === WIZARD_SKIP ? '' : text;
    if (state.runDraft.marketplace === 'avito') {
      state.stage = 'run_city';
      await sendMessage(chatId, '🌆 Шаг 3/8: Введите город Avito (например: Москва) или пропустите:', { keyboard: [[{ text: WIZARD_SKIP }]], resize_keyboard: true, one_time_keyboard: false });
      return;
    }
    state.stage = 'run_min_price';
    await sendMessage(chatId, '💰 Шаг 3/7: Цена ОТ (только число) или 0:', { keyboard: [[{ text: '0' }]], resize_keyboard: true, one_time_keyboard: false });
    return;
  }

  if (state.stage === 'run_city') {
    state.runDraft.city = text === WIZARD_SKIP ? 'moskva' : text;
    state.stage = 'run_min_price';
    await sendMessage(chatId, '💰 Шаг 4/8: Цена ОТ (только число) или 0:', { keyboard: [[{ text: '0' }]], resize_keyboard: true, one_time_keyboard: false });
    return;
  }

  if (state.stage === 'run_min_price') {
    const v = parseInt(String(text).replace(/\D/g, ''), 10);
    state.runDraft.minPrice = Number.isFinite(v) ? v : 0;
    state.stage = 'run_max_price';
    await sendMessage(
      chatId,
      state.runDraft.marketplace === 'avito'
        ? '💰 Шаг 5/8: Цена ДО (только число) или 0:'
        : '💰 Шаг 4/7: Цена ДО (только число) или 0:',
      { keyboard: [[{ text: '0' }]], resize_keyboard: true, one_time_keyboard: false }
    );
    return;
  }

  if (state.stage === 'run_max_price') {
    const v = parseInt(String(text).replace(/\D/g, ''), 10);
    state.runDraft.maxPrice = Number.isFinite(v) ? v : 0;
    state.stage = 'run_memory_enable';
    await sendMessage(
      chatId,
      state.runDraft.marketplace === 'avito'
        ? '💾 Шаг 6/8: Фильтровать по памяти?'
        : '💾 Шаг 5/7: Фильтровать по памяти?',
      wizardYesNoKeyboard()
    );
    return;
  }

  if (state.stage === 'run_memory_enable') {
    if (text === WIZARD_YES) {
      state.stage = 'run_memory_value';
      await sendMessage(chatId, '💾 Выберите память:', buildMemoryKeyboard());
      return;
    }
    state.runDraft.memory = '';
    state.stage = state.runDraft.marketplace === 'avito' ? 'run_only_today' : 'run_color_enable';
    await sendMessage(
      chatId,
      state.runDraft.marketplace === 'avito' ? '📆 Шаг 7/8: Только за сегодня?' : '🎨 Шаг 6/7: Фильтровать по цвету?',
      wizardYesNoKeyboard()
    );
    return;
  }

  if (state.stage === 'run_memory_value') {
    if (text === '128' || text === '256' || text === '512') state.runDraft.memory = text;
    else if (text === '1ТБ') state.runDraft.memory = '1024';
    else state.runDraft.memory = '';
    state.stage = state.runDraft.marketplace === 'avito' ? 'run_only_today' : 'run_color_enable';
    await sendMessage(
      chatId,
      state.runDraft.marketplace === 'avito' ? '📆 Шаг 7/8: Только за сегодня?' : '🎨 Шаг 6/7: Фильтровать по цвету?',
      wizardYesNoKeyboard()
    );
    return;
  }

  if (state.stage === 'run_only_today') {
    state.runDraft.onlyToday = text === WIZARD_YES;
    state.stage = 'run_confirm';
    await sendMessage(
      chatId,
      state.configureOnly
        ? '💾 Шаг 8/8: Сохранить настройки этой площадки?'
        : '✅ Шаг 8/8: Запустить парсинг с этими настройками?',
      confirmKeyboard(state.configureOnly)
    );
    return;
  }

  if (state.stage === 'run_color_enable') {
    if (text === WIZARD_YES) {
      state.stage = 'run_color_value';
      await sendMessage(chatId, '🎨 Выберите цвет:', buildColorKeyboard());
      return;
    }
    state.runDraft.color = '';
    state.stage = 'run_confirm';
    await sendMessage(
      chatId,
      state.configureOnly
        ? '💾 Шаг 7/7: Сохранить настройки этой площадки?'
        : '✅ Шаг 7/7: Запустить парсинг с этими настройками?',
      confirmKeyboard(state.configureOnly)
    );
    return;
  }

  if (state.stage === 'run_color_value') {
    state.runDraft.color = COLOR_BUTTONS.includes(text) ? text : '';
    state.stage = 'run_confirm';
    await sendMessage(
      chatId,
      state.configureOnly
        ? '💾 Шаг 7/7: Сохранить настройки этой площадки?'
        : '✅ Шаг 7/7: Запустить парсинг с этими настройками?',
      confirmKeyboard(state.configureOnly)
    );
    return;
  }

  if (state.stage === 'run_confirm') {
    if (text === WIZARD_START || (state.configureOnly && text === SETTINGS_SAVE)) {
      const d = state.runDraft || {};
      // Persist settings in DB and in-memory profile
      state.settings.marketplaceDefault = d.marketplace || 'avito';
      state.settings.query = d.query || 'iPhone';
      state.settings.extraKeywords = d.extraKeywords || '';
      state.settings.city = d.city || state.settings.city || 'moskva';
      state.settings.minPrice = Number(d.minPrice || 0);
      state.settings.maxPrice = Number(d.maxPrice || 0);
      state.settings.priceFilterEnabled = state.settings.minPrice > 0 || state.settings.maxPrice > 0;
      state.settings.memoryGb = d.memory || '';
      state.settings.memoryFilterEnabled = Boolean(state.settings.memoryGb);
      state.settings.onlyToday = Boolean(d.onlyToday);
      state.settings.color = d.color || '';
      state.settings.colorFilterEnabled = Boolean(state.settings.color);

      try {
        if (d.marketplace === 'avito') state.marketSettings.avito = { ...d };
        if (d.marketplace === 'wb') state.marketSettings.wb = { ...d };
        await supabaseUpsertMarketSettings(msg.from?.id, d.marketplace || 'avito', d);
      } catch (e) {
        console.error('Supabase settings upsert error:', String(e?.message || e || ''));
      }

      state.stage = 'main';
      if (state.configureOnly) {
        state.configureOnly = false;
        await sendMessage(
          chatId,
          `✅ Настройки ${d.marketplace === 'wb' ? 'ВБ' : 'Авито'} сохранены в БД.`,
          {
            keyboard: [[{ text: SETTINGS_AVITO }, { text: SETTINGS_WB }], [{ text: MENU.backToMenu }]],
            resize_keyboard: true,
            one_time_keyboard: false,
          }
        );
        return;
      }

      await runParserForMarketplace({
        chatId,
        marketplace: d.marketplace || 'avito',
        parserOverrides: {
          marketplace: d.marketplace || 'avito',
          query: d.query || 'iPhone',
          extraKeywords: d.extraKeywords || '',
          city: d.city || state.settings.city || 'moskva',
          minPrice: d.minPrice || 0,
          maxPrice: d.maxPrice || 0,
          memory: d.memory || '',
          onlyToday: Boolean(d.onlyToday),
          color: d.color || '',
        },
      });
      return;
    }
    if (text === MENU.backToMenu) {
      state.stage = 'main';
      state.runDraft = null;
      state.configureOnly = false;
      await sendMessage(chatId, '⬅️ Возврат в меню.', mainKeyboard);
      return;
    }
    await sendMessage(
      chatId,
      state.configureOnly
        ? 'Нажми `Сохранить настройки` или `Назад`.'
        : 'Нажми `Запустить парсинг` или `Назад`.',
      confirmKeyboard(state.configureOnly)
    );
    return;
  }

  // Stage: settings wizard
  if (state.stage === 'settings') {
    const s = state.settings;
    if (state.launchMode && text === 'Запустить парсинг') {
      const marketplace = state.selectedMarketplace || 'avito';
      state.stage = 'main';
      state.launchMode = false;
      await runParserForMarketplace({ chatId, marketplace });
      return;
    }

    if (text === 'Только за сегодня: ДА') {
      s.onlyToday = true;
      await sendMessage(chatId, 'Ок: только за сегодня = ДА', buildSettingsKeyboard(s));
      return;
    }
    if (text === 'Только за сегодня: НЕТ') {
      s.onlyToday = false;
      await sendMessage(chatId, 'Ок: только за сегодня = НЕТ', buildSettingsKeyboard(s));
      return;
    }

    if (text === 'Цена: БЕЗ фильтра') {
      s.priceFilterEnabled = false;
      s.minPrice = 0;
      s.maxPrice = 0;
      await sendMessage(chatId, 'Ок: фильтр цены выключен', buildSettingsKeyboard(s));
      return;
    }
    if (text === 'Цена: 43000-120000') {
      s.priceFilterEnabled = true;
      s.minPrice = 43000;
      s.maxPrice = 120000;
      await sendMessage(chatId, 'Ок: фильтр цены включен (43000-120000)', buildSettingsKeyboard(s));
      return;
    }

    if (text === 'Память: ДА') {
      s.memoryFilterEnabled = true;
      state.stage = 'settings_memory';
      await sendMessage(chatId, 'Выбери память для WB/фильтра:', buildMemoryKeyboard());
      return;
    }
    if (text === 'Память: НЕТ') {
      s.memoryFilterEnabled = false;
      s.memoryGb = '';
      await sendMessage(chatId, 'Ок: фильтр памяти выключен', buildSettingsKeyboard(s));
      return;
    }

    if (text === 'Цвет WB: ДА') {
      s.colorFilterEnabled = true;
      state.stage = 'settings_color';
      await sendMessage(chatId, 'Выбери цвет для Wildberries:', buildColorKeyboard());
      return;
    }
    if (text === 'Цвет WB: НЕТ') {
      s.colorFilterEnabled = false;
      s.color = '';
      await sendMessage(chatId, 'Ок: фильтр цвета выключен', buildSettingsKeyboard(s));
      return;
    }

    // back from settings (for launch mode go back to marketplace chooser)
    if (text === MENU.backToMenu) {
      if (state.launchMode) {
        state.stage = 'choosing_market';
        state.selectedMarketplace = null;
        state.launchMode = false;
        await sendMessage(chatId, 'Выбери площадку:', buildMarketplaceKeyboard());
      } else {
        state.stage = 'main';
        await sendMessage(chatId, 'Ок, меню.', mainKeyboard);
      }
      return;
    }

    await sendMessage(chatId, 'Не понял. Используй кнопки настроек.', buildSettingsKeyboard(s));
    return;
  }

  // Stage: choose memory
  if (state.stage === 'settings_memory') {
    const s = state.settings;
    if (text === '128' || text === '256' || text === '512') {
      s.memoryGb = text;
      s.memoryFilterEnabled = true;
      state.stage = 'settings';
      await sendMessage(chatId, `Ок: память = ${text} ГБ`, buildSettingsKeyboard(s));
      return;
    }
    if (text === '1ТБ') {
      s.memoryGb = '1024';
      s.memoryFilterEnabled = true;
      state.stage = 'settings';
      await sendMessage(chatId, 'Ок: память = 1ТБ (1024 ГБ)', buildSettingsKeyboard(s));
      return;
    }
    if (text === 'Любая') {
      s.memoryFilterEnabled = false;
      s.memoryGb = '';
      state.stage = 'settings';
      await sendMessage(chatId, 'Ок: память любая (фильтр выключен)', buildSettingsKeyboard(s));
      return;
    }
    await sendMessage(chatId, 'Выбери память из кнопок.', buildMemoryKeyboard());
    return;
  }

  // Stage: choose color
  if (state.stage === 'settings_color') {
    const s = state.settings;
    if (text === 'Без фильтра') {
      s.colorFilterEnabled = false;
      s.color = '';
      state.stage = 'settings';
      await sendMessage(chatId, 'Ок: цвет фильтровать не будем', buildSettingsKeyboard(s));
      return;
    }
    if (COLOR_BUTTONS.includes(text)) {
      s.colorFilterEnabled = true;
      s.color = text;
      state.stage = 'settings';
      await sendMessage(chatId, `Ок: цвет = ${text}`, buildSettingsKeyboard(s));
      return;
    }
    await sendMessage(chatId, 'Выбери цвет из списка кнопок.', buildColorKeyboard());
    return;
  }

  // fallback
  await sendMessage(chatId, 'Нажми кнопку меню снизу или /start', mainKeyboard);
}

async function pollLoop() {
  console.log('Telegram-бот запущен.');
  let backoffMs = 1200;
  while (running) {
    try {
      const updates = await tgApi('getUpdates', {
        timeout: 25,
        offset,
        allowed_updates: ['message'],
      });

      if (Array.isArray(updates) && updates.length > 0) {
        for (const upd of updates) {
          offset = Math.max(offset, Number(upd.update_id || 0) + 1);
          try {
            await handleMessage(upd.message);
          } catch (err) {
            console.error('Ошибка обработки сообщения:', String(err?.message || err || ''));
          }
        }
      }
      backoffMs = 1200;
    } catch (err) {
      console.error('Ошибка polling:', String(err?.message || err || ''));
      await sleep(backoffMs);
      backoffMs = Math.min(15_000, Math.round(backoffMs * 1.6));
    }
  }
}

process.on('SIGINT', () => {
  running = false;
  console.log('Остановка бота (SIGINT)…');
});
process.on('SIGTERM', () => {
  running = false;
  console.log('Остановка бота (SIGTERM)…');
});

pollLoop().catch((e) => {
  console.error('Критическая ошибка бота:', e);
  process.exit(1);
});

