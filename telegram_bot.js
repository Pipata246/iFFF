const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
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

/** Корень проекта (нужен до warnIfWbStorageFileWithoutEnv — там ранний вызов при загрузке модуля). */
const PROJECT_DIR = __dirname;

/** Файл сессии WB лежит в проекте, а в .env не прописан путь — парсер не подставит его сам. */
function warnIfWbStorageFileWithoutEnv() {
  const wbJson = path.join(PROJECT_DIR, 'wb_storage.json');
  if (String(process.env.PARSER_WB_STORAGE_STATE || '').trim()) return;
  if (!fs.existsSync(wbJson)) return;
  const abs = path.resolve(wbJson);
  console.warn(
    `[ifind] Найден ${abs}, но PARSER_WB_STORAGE_STATE не задан (пустой или закомментирован в .env). ` +
      `Добавьте строку без #: PARSER_WB_STORAGE_STATE=${abs} и systemctl restart ifind-bot`
  );
}
warnIfWbStorageFileWithoutEnv();

const token = process.env.TELEGRAM_BOT_TOKEN || process.env.BOT_TOKEN;
if (!token) {
  console.error('Ошибка: не задан TELEGRAM_BOT_TOKEN (или BOT_TOKEN).');
  process.exit(1);
}

const TG_API = `https://api.telegram.org/bot${token}`;
const SUPABASE_URL = (process.env.SUPABASE_URL || '').trim().replace(/\/+$/, '');
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || '';

const XLSX_GLOB_PREFIX = 'results_';
const XLSX_GLOB_SUFFIX = '.xlsx';

let offset = Number(process.env.TELEGRAM_BOT_OFFSET || 0);
let running = true;

const MENU = {
  manualRun: '🚀 Ручной запуск',
  autoSettings: '⚙️ Настройки автопарсинга',
  guide: '📘 Инструкция',
  excels: '📁 Эксель файлы',
  checkAutoparse: '🔎 Проверить автопарсинг',
  backToMenu: '⬅️ Назад в меню',
};

const AUTO_WB_FILTERS = '🔧 Настроить фильтры';
const AUTO_WB_INTERVAL = '⏱ Настроить периодичность';
const AUTO_EDIT_AVITO = '✏️ Авито — изменить фильтры';
const AUTO_EDIT_WB = '✏️ ВБ — изменить фильтры';
const AUTO_BACK_TO_AUTO_MAIN = '⬅️ К настройкам автопарсинга';
const AUTO_WB_ENABLED = '✅ Автопарсинг ВКЛ';
const AUTO_WB_DISABLED = '⏸ Автопарсинг ВЫКЛ';
const AUTO_WB_EXCEL_ON = '📊 Сохранять Excel: ВКЛ';
const AUTO_WB_EXCEL_OFF = '📊 Сохранять Excel: ВЫКЛ';

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
    [{ text: MENU.checkAutoparse }],
  ],
  resize_keyboard: true,
  one_time_keyboard: false,
};

const startText =
  '🤖 Бот помогает находить выгодные iPhone на Авито и Wildberries.\n\n' +
  '📌 `Ручной запуск` — выбери площадку и параметры.\n\n' +
  '⚙️ `Настройки автопарсинга` — автообход ВБ по расписанию и уведомления о новых объявлениях.\n\n' +
  '🔎 `Проверить автопарсинг` — статус и время следующего запуска ВБ.\n\n' +
  '📁 `Эксель файлы` — список и выгрузка результатов.';

const yesNoKeyboard = (yesText, noText) => ({
  keyboard: [[{ text: yesText }, { text: noText }]],
  resize_keyboard: true,
  one_time_keyboard: false,
});

const WIZARD_YES = '✅ Да';
const WIZARD_NO = '❌ Нет';
const WIZARD_SKIP = '⏭️ Пропустить';
const WIZARD_CANCEL = '❌ Отмена';
const WIZARD_START = '🚀 Запустить парсинг';
const SETTINGS_EDIT = '✏️ Изменить настройки';
const SETTINGS_AVITO = '🛒 Настроить Авито';
const SETTINGS_WB = '🛍️ Настроить ВБ';
const SETTINGS_SAVE = '💾 Сохранить настройки';
const RUN_CONTINUE_AUTO = '▶️ Продолжить с настройками автопарсинга';
const RUN_SET_MANUAL = '✍️ Задать вручную';
const EXCEL_VIEW = '👀 Посмотреть';
const EXCEL_DELETE = '🗑 Удалить';

function wizardYesNoKeyboard() {
  return {
    keyboard: [[{ text: WIZARD_YES }, { text: WIZARD_NO }], [{ text: WIZARD_CANCEL }]],
    resize_keyboard: true,
    one_time_keyboard: false,
  };
}

function confirmKeyboard(configureOnly) {
  return {
    keyboard: [[{ text: configureOnly ? SETTINGS_SAVE : WIZARD_START }], [{ text: WIZARD_CANCEL }]],
    resize_keyboard: true,
    one_time_keyboard: false,
  };
}

function skipKeyboard() {
  return {
    keyboard: [[{ text: WIZARD_SKIP }], [{ text: WIZARD_CANCEL }]],
    resize_keyboard: true,
    one_time_keyboard: false,
  };
}

function numberKeyboard(v = '0') {
  return {
    keyboard: [[{ text: String(v) }], [{ text: WIZARD_CANCEL }]],
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
      excelDeleteCandidates: [],
      marketSettings: {
        avito: null,
        wb: null,
      },
      returnToAutoFiltersHub: false,
      /** Ручной запуск «задать вручную» — не писать настройки в БД автопарсинга. */
      manualRunEphemeral: false,
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
  rows.push([{ text: WIZARD_CANCEL }]);
  return { keyboard: rows, resize_keyboard: true, one_time_keyboard: false };
}

function buildMemoryKeyboard() {
  return {
    keyboard: [
      [{ text: '128' }, { text: '256' }],
      [{ text: '512' }, { text: '1ТБ' }],
      [{ text: 'Любая' }],
      [{ text: WIZARD_CANCEL }],
    ],
    resize_keyboard: true,
    one_time_keyboard: false,
  };
}

function buildExcelMenuKeyboard() {
  return {
    keyboard: [[{ text: EXCEL_VIEW }, { text: EXCEL_DELETE }], [{ text: MENU.backToMenu }]],
    resize_keyboard: true,
    one_time_keyboard: false,
  };
}

function buildExcelDeleteKeyboard(files) {
  const rows = files.map((f) => [{ text: f.fileName }]);
  rows.push([{ text: MENU.backToMenu }]);
  return {
    keyboard: rows,
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
      [{ text: WIZARD_CANCEL }],
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

async function supabaseDeleteExcelFileByPath(filePath) {
  const hasSupabaseCfg = Boolean(SUPABASE_URL && SUPABASE_ANON_KEY);
  if (!hasSupabaseCfg || !filePath) return;
  const qs = new URLSearchParams({
    file_path: `eq.${filePath}`,
  });
  const resp = await fetch(`${SUPABASE_URL}/rest/v1/excel_files?${qs.toString()}`, {
    method: 'DELETE',
    headers: {
      apikey: SUPABASE_ANON_KEY,
      'content-type': 'application/json',
      Prefer: 'return=minimal',
    },
  });
  if (!resp.ok) {
    const txt = await resp.text().catch(() => '');
    throw new Error(`Supabase excel_files delete failed: HTTP ${resp.status}: ${txt}`);
  }
}

/** Отпечаток фильтров ВБ для сброса базы при изменении настроек. */
function wbSettingsFingerprintFromDraft(d) {
  const payload = JSON.stringify({
    q: String(d.query || '').trim(),
    e: String(d.extraKeywords || '').trim(),
    min: Number(d.minPrice || 0),
    max: Number(d.maxPrice || 0),
    mem: String(d.memory || '').trim(),
    col: String(d.color || '').trim(),
  });
  return crypto.createHash('sha256').update(payload).digest('hex').slice(0, 40);
}

async function supabaseFetchWbAutoparseState(tgUserId) {
  const hasSupabaseCfg = Boolean(SUPABASE_URL && SUPABASE_ANON_KEY);
  if (!hasSupabaseCfg || tgUserId == null) return null;
  const qs = new URLSearchParams({
    telegram_user_id: `eq.${String(tgUserId)}`,
    select:
      'telegram_user_id,interval_minutes,enabled,save_excel,is_running,baseline_ready,settings_fingerprint,last_run_started_at,last_run_finished_at,last_run_ok,next_scheduled_at',
    limit: '1',
  });
  const resp = await fetch(`${SUPABASE_URL}/rest/v1/wb_autoparse_state?${qs.toString()}`, {
    headers: { apikey: SUPABASE_ANON_KEY, 'content-type': 'application/json' },
  });
  if (!resp.ok) {
    const txt = await resp.text().catch(() => '');
    throw new Error(`wb_autoparse_state select failed: HTTP ${resp.status}: ${txt}`);
  }
  const rows = await resp.json();
  return Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
}

async function supabaseUpsertWbAutoparseState(tgUserId, patch) {
  const hasSupabaseCfg = Boolean(SUPABASE_URL && SUPABASE_ANON_KEY);
  if (!hasSupabaseCfg || tgUserId == null) return;
  const row = {
    telegram_user_id: String(tgUserId),
    ...patch,
  };
  const resp = await fetch(`${SUPABASE_URL}/rest/v1/wb_autoparse_state?on_conflict=telegram_user_id`, {
    method: 'POST',
    headers: {
      apikey: SUPABASE_ANON_KEY,
      'content-type': 'application/json',
      Prefer: 'resolution=merge-duplicates,return=minimal',
    },
    body: JSON.stringify([row]),
  });
  if (!resp.ok) {
    const txt = await resp.text().catch(() => '');
    throw new Error(`wb_autoparse_state upsert failed: HTTP ${resp.status}: ${txt}`);
  }
}

async function supabaseDeleteWbSeenListings(tgUserId) {
  const hasSupabaseCfg = Boolean(SUPABASE_URL && SUPABASE_ANON_KEY);
  if (!hasSupabaseCfg || tgUserId == null) return;
  const qs = new URLSearchParams({ telegram_user_id: `eq.${String(tgUserId)}` });
  const resp = await fetch(`${SUPABASE_URL}/rest/v1/wb_seen_listings?${qs.toString()}`, {
    method: 'DELETE',
    headers: { apikey: SUPABASE_ANON_KEY, Prefer: 'return=minimal' },
  });
  if (!resp.ok) {
    const txt = await resp.text().catch(() => '');
    throw new Error(`wb_seen_listings delete failed: HTTP ${resp.status}: ${txt}`);
  }
}

async function supabaseFetchWbSeenIdsSet(tgUserId) {
  const hasSupabaseCfg = Boolean(SUPABASE_URL && SUPABASE_ANON_KEY);
  if (!hasSupabaseCfg || tgUserId == null) return new Set();
  const qs = new URLSearchParams({
    telegram_user_id: `eq.${String(tgUserId)}`,
    select: 'listing_id',
  });
  const resp = await fetch(`${SUPABASE_URL}/rest/v1/wb_seen_listings?${qs.toString()}`, {
    headers: { apikey: SUPABASE_ANON_KEY },
  });
  if (!resp.ok) {
    const txt = await resp.text().catch(() => '');
    throw new Error(`wb_seen_listings select failed: HTTP ${resp.status}: ${txt}`);
  }
  const rows = await resp.json();
  const set = new Set();
  if (Array.isArray(rows)) {
    for (const r of rows) {
      if (r && r.listing_id) set.add(String(r.listing_id));
    }
  }
  return set;
}

async function supabaseInsertWbSeenBatch(tgUserId, listingIds) {
  const hasSupabaseCfg = Boolean(SUPABASE_URL && SUPABASE_ANON_KEY);
  if (!hasSupabaseCfg || !listingIds.length) return;
  const chunkSize = 400;
  for (let i = 0; i < listingIds.length; i += chunkSize) {
    const chunk = listingIds.slice(i, i + chunkSize).map((listing_id) => ({
      telegram_user_id: String(tgUserId),
      listing_id: String(listing_id),
    }));
    const resp = await fetch(
      `${SUPABASE_URL}/rest/v1/wb_seen_listings?on_conflict=telegram_user_id,listing_id`,
      {
        method: 'POST',
        headers: {
          apikey: SUPABASE_ANON_KEY,
          'content-type': 'application/json',
          Prefer: 'resolution=merge-duplicates,return=minimal',
        },
        body: JSON.stringify(chunk),
      }
    );
    if (!resp.ok) {
      const txt = await resp.text().catch(() => '');
      throw new Error(`wb_seen_listings insert failed: HTTP ${resp.status}: ${txt}`);
    }
  }
}

async function supabaseFetchDueWbAutoparseUsers() {
  const hasSupabaseCfg = Boolean(SUPABASE_URL && SUPABASE_ANON_KEY);
  if (!hasSupabaseCfg) return [];
  const qs = new URLSearchParams({
    enabled: 'eq.true',
    is_running: 'eq.false',
    select:
      'telegram_user_id,interval_minutes,enabled,baseline_ready,settings_fingerprint,next_scheduled_at',
    order: 'next_scheduled_at.asc.nullsfirst',
    limit: '25',
  });
  const resp = await fetch(`${SUPABASE_URL}/rest/v1/wb_autoparse_state?${qs.toString()}`, {
    headers: { apikey: SUPABASE_ANON_KEY },
  });
  if (!resp.ok) {
    const txt = await resp.text().catch(() => '');
    console.error('wb_autoparse due list failed:', txt);
    return [];
  }
  const rows = await resp.json();
  if (!Array.isArray(rows)) return [];
  const now = Date.now();
  const filtered = rows.filter((r) => {
    if (r.next_scheduled_at == null || r.next_scheduled_at === '') return true;
    const t = new Date(r.next_scheduled_at).getTime();
    return !Number.isNaN(t) && t <= now;
  });
  filtered.sort((a, b) => {
    const ta = a.next_scheduled_at ? new Date(a.next_scheduled_at).getTime() : 0;
    const tb = b.next_scheduled_at ? new Date(b.next_scheduled_at).getTime() : 0;
    const fa = Number.isNaN(ta) ? 0 : ta;
    const fb = Number.isNaN(tb) ? 0 : tb;
    return fa - fb;
  });
  return filtered;
}

async function supabaseResetAllWbAutoparseRunning() {
  const hasSupabaseCfg = Boolean(SUPABASE_URL && SUPABASE_ANON_KEY);
  if (!hasSupabaseCfg) return;
  try {
    const resp = await fetch(`${SUPABASE_URL}/rest/v1/wb_autoparse_state?is_running=eq.true`, {
      method: 'PATCH',
      headers: {
        apikey: SUPABASE_ANON_KEY,
        'content-type': 'application/json',
        Prefer: 'return=minimal',
      },
      body: JSON.stringify({ is_running: false }),
    });
    if (!resp.ok) {
      const txt = await resp.text().catch(() => '');
      console.error('wb_autoparse reset running flags:', txt);
    }
  } catch (e) {
    console.error('wb_autoparse reset running:', String(e?.message || e || ''));
  }
}

function buildAutoSettingsMainKeyboard(st) {
  const saveExcelEnabled = st == null ? true : st.save_excel !== false;
  const excelText = saveExcelEnabled ? AUTO_WB_EXCEL_ON : AUTO_WB_EXCEL_OFF;
  return {
    keyboard: [
      [{ text: AUTO_WB_FILTERS }],
      [{ text: AUTO_WB_INTERVAL }],
      [{ text: excelText }],
      [{ text: MENU.backToMenu }],
    ],
    resize_keyboard: true,
    one_time_keyboard: false,
  };
}

function buildAutoFiltersHubKeyboard() {
  return {
    keyboard: [
      [{ text: AUTO_EDIT_AVITO }],
      [{ text: AUTO_EDIT_WB }],
      [{ text: AUTO_BACK_TO_AUTO_MAIN }],
    ],
    resize_keyboard: true,
    one_time_keyboard: false,
  };
}

/** Экран выбора площадки для фильтров + текущие сохранённые настройки из БД. */
async function sendAutoFiltersHubScreen(chatId, telegramUserId, extraNotice = '') {
  const rows = await supabaseFetchMarketSettingsAll(telegramUserId).catch(() => []);
  const avitoRow = rows.find((r) => String(r.marketplace || '').toLowerCase() === 'avito');
  const wbRow = rows.find((r) => String(r.marketplace || '').toLowerCase() === 'wb');
  const state = getUserState(chatId);
  state.marketSettings.avito = avitoRow ? mapRowToMarketSettings(avitoRow) : null;
  state.marketSettings.wb = wbRow ? mapRowToMarketSettings(wbRow) : null;
  state.stage = 'auto_filters_hub';
  const head = extraNotice ? `${extraNotice}\n\n` : '';
  const text =
    `${head}` +
    `📋 *Текущие сохранённые фильтры*\n\n` +
    `${formatMarketSettingsBlock('🛒 Авито', state.marketSettings.avito)}\n\n` +
    `${formatWbSettingsBlock('🛍️ Wildberries (автопарсинг)', state.marketSettings.wb)}\n\n` +
    `Выберите площадку — откроется мастер изменения и сохранения в БД:`;
  await sendMessage(chatId, text, buildAutoFiltersHubKeyboard(), 'Markdown');
}

function buildWbIntervalKeyboard(st) {
  const enabled = st && st.enabled !== false;
  const rowToggle = enabled
    ? [{ text: AUTO_WB_DISABLED }]
    : [{ text: AUTO_WB_ENABLED }];
  return {
    keyboard: [
      [{ text: '10 мин' }, { text: '15 мин' }, { text: '30 мин' }],
      [{ text: '45 мин' }, { text: '60 мин' }],
      rowToggle,
      [{ text: MENU.backToMenu }],
    ],
    resize_keyboard: true,
    one_time_keyboard: false,
  };
}

let isParsing = false;
let lastRunMarketplace = null;
/** Тихий запуск ВБ из планировщика автопарсинга (для сообщения при ручном запуске). */
let wbAutoparseWbRunning = false;
const activeChildByChatId = new Map(); // chatId -> child process
const stopRequestedByChatId = new Map(); // chatId -> boolean

function buildParserEnvForRun({ chatId, marketplace, settings, parserOverrides = null }) {
  const o = parserOverrides || {};
  const runMarketplace = o.marketplace || marketplace;
  const isWbOnly = runMarketplace === 'wb';
  const queryRaw = (o.query != null ? String(o.query) : process.env.PARSER_QUERY || 'iPhone 15').trim();
  const extraRaw = (o.extraKeywords != null ? String(o.extraKeywords) : process.env.PARSER_EXTRA_KEYWORDS || '').trim();
  const wbCombinedQuery = isWbOnly
    ? [queryRaw, extraRaw].filter(Boolean).join(' ').replace(/\s+/g, ' ').trim()
    : queryRaw;
  // main.js переключаем в env mode (без readline)
  return {
    ...process.env,
    PARSER_USE_ENV: '1',
    PARSER_MARKETPLACE: runMarketplace,
    // Always run unattended (no manual proxy prompts) for VPS bot runs
    AVITO_MANUAL_PROXY: '0',
    // На части VPS прокси стабильнее при установке на уровне launch.
    AVITO_PROXY_ON_LAUNCH: '1',
    AVITO_WAIT_ENTER: '0',
    // Ensure no WB home warmup (avoid going to WB home)
    WB_USE_HOME_WARMUP: '0',
    // Force headless by default on VPS
    PLAYWRIGHT_HEADLESS: '1',
    // Keep memory stable on 1GB RAM VPS
    NODE_OPTIONS: process.env.NODE_OPTIONS || '--max-old-space-size=384',
    // Для WB модель должна гарантированно быть в поисковой строке URL.
    PARSER_QUERY: wbCombinedQuery || 'iPhone',
    PARSER_EXTRA_KEYWORDS: isWbOnly ? '' : extraRaw,
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

/**
 * Завершение тихого автопарсинга ВБ: база при первом запуске, уведомления о новых позже.
 * @param {number} intervalMinutes — интервал до следующего запуска из настроек
 */
async function processWbAutoparseResult(chatId, exitCode, snapshotPath, intervalMinutes, stopped, createdExcelFiles = []) {
  const interval = Number.isFinite(Number(intervalMinutes)) && Number(intervalMinutes) > 0 ? Number(intervalMinutes) : 30;
  const nextIso = new Date(Date.now() + interval * 60_000).toISOString();

  const safeUnlink = (p) => {
    try {
      if (p && fs.existsSync(p)) fs.unlinkSync(p);
    } catch (_) {
      /* ignore */
    }
  };

  if (stopped) {
    await supabaseUpsertWbAutoparseState(chatId, {
      is_running: false,
      last_run_finished_at: new Date().toISOString(),
      last_run_ok: false,
      next_scheduled_at: nextIso,
    });
    safeUnlink(snapshotPath);
    return;
  }

  await supabaseUpsertWbAutoparseState(chatId, {
    is_running: false,
    last_run_finished_at: new Date().toISOString(),
    last_run_ok: exitCode === 0,
    next_scheduled_at: nextIso,
  });

  let rowState = null;
  try {
    rowState = await supabaseFetchWbAutoparseState(chatId);
  } catch (_) {
    rowState = null;
  }
  const saveExcelEnabled = rowState == null ? true : rowState.save_excel !== false;
  if (!saveExcelEnabled && Array.isArray(createdExcelFiles) && createdExcelFiles.length > 0) {
    for (const fp of createdExcelFiles) safeUnlink(fp);
  }

  if (exitCode !== 0 || !snapshotPath || !fs.existsSync(snapshotPath)) {
    safeUnlink(snapshotPath);
    return;
  }

  let data;
  try {
    data = JSON.parse(fs.readFileSync(snapshotPath, 'utf8'));
  } catch (e) {
    console.error('WB snapshot JSON read error:', String(e?.message || e || ''));
    safeUnlink(snapshotPath);
    return;
  }
  safeUnlink(snapshotPath);

  const rawItems = Array.isArray(data.items) ? data.items : [];
  const dedup = new Map();
  for (const it of rawItems) {
    const id = String(it.listingId || '').trim();
    if (!id) continue;
    if (!dedup.has(id)) dedup.set(id, it);
  }
  const list = [...dedup.values()];
  const ids = [...dedup.keys()];

  const baselineReady = Boolean(rowState?.baseline_ready);

  if (!baselineReady) {
    try {
      await supabaseInsertWbSeenBatch(chatId, ids);
      await supabaseUpsertWbAutoparseState(chatId, { baseline_ready: true });
    } catch (e) {
      console.error('WB baseline seed error:', String(e?.message || e || ''));
    }
    return;
  }

  let seen = new Set();
  try {
    seen = await supabaseFetchWbSeenIdsSet(chatId);
  } catch (e) {
    console.error('WB seen fetch error:', String(e?.message || e || ''));
  }

  const fresh = list.filter((it) => !seen.has(String(it.listingId || '').trim()));
  const maxNotify = 30;
  const newIdsAll = fresh.map((it) => String(it.listingId).trim()).filter(Boolean);

  for (let i = 0; i < Math.min(fresh.length, maxNotify); i += 1) {
    const it = fresh[i];
    const title = String(it.title || 'Без названия').slice(0, 400);
    const price = String(it.priceText || '—').slice(0, 120);
    const href = String(it.href || '').trim() || '—';
    const msg = `🆕 Новое на Wildberries\n\n📱 ${title}\n💰 ${price}\n🔗 ${href}`;
    try {
      await sendMessage(chatId, msg, mainKeyboard);
    } catch (e) {
      console.error('WB notify send error:', String(e?.message || e || ''));
    }
    await sleep(350);
  }

  if (newIdsAll.length) {
    try {
      await supabaseInsertWbSeenBatch(chatId, newIdsAll);
    } catch (e) {
      console.error('WB seen insert error:', String(e?.message || e || ''));
    }
  }
}

async function runParserForMarketplace({
  chatId,
  marketplace,
  parserOverrides = null,
  silent = false,
  wbSnapshotPath = null,
  wbAutoparseHook = null,
}) {
  if (!silent && wbAutoparseWbRunning) {
    await sendMessage(
      chatId,
      '⏳ Сейчас идёт автопарсинг Wildberries. Повторите ручной запуск позже.',
      mainKeyboard
    );
    return;
  }
  if (isParsing) {
    if (!silent) await sendMessage(chatId, 'Парсинг уже запущен. Подождите, пожалуйста.');
    return;
  }

  const markSilentWbAutoparse = Boolean(silent && marketplace === 'wb');
  isParsing = true;
  if (markSilentWbAutoparse) {
    wbAutoparseWbRunning = true;
  }
  lastRunMarketplace = marketplace;

  const state = getUserState(chatId);
  const settings = state.settings;
  state.stage = silent ? 'main' : 'parsing';

  const stopKeyboard = buildStopKeyboard();
  let stage1Sent = false;
  let stage2Sent = false;
  let stage3Emitted = false;

  const beforeFiles = listExcelFilesOnServer();
  const beforePaths = new Set(beforeFiles.map((f) => f.filePath));

  if (!silent) {
    await sendMessage(
      chatId,
      `🟢 Парсинг запущен: ${marketplace === 'avito' ? 'Авито' : 'ВБ'}.\nНажми «${STOP_PARSING_TEXT}».`,
      stopKeyboard
    );
  }

  const env = buildParserEnvForRun({ chatId, marketplace, settings, parserOverrides });
  if (wbSnapshotPath && marketplace === 'wb') {
    env.PARSER_WB_SNAPSHOT_JSON = wbSnapshotPath;
  }

  const child = spawn('node', ['main.js'], {
    cwd: PROJECT_DIR,
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
    detached: false,
  });

  activeChildByChatId.set(chatId, child);

  function handleParserLine(line) {
    if (silent) return;
    const s = String(line || '').trimEnd();
    if (!s) return;

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
    if (markSilentWbAutoparse) {
      wbAutoparseWbRunning = false;
    }
    activeChildByChatId.delete(chatId);
    state.stage = 'main';
    const stopped = Boolean(stopRequestedByChatId.get(chatId));
    stopRequestedByChatId.delete(chatId);

    if (wbAutoparseHook && marketplace === 'wb') {
      const afterFiles = listExcelFilesOnServer();
      const newOnes = afterFiles.filter((f) => !beforePaths.has(f.filePath));
      try {
        await wbAutoparseHook(code, stopped, newOnes);
      } catch (e) {
        console.error('wbAutoparseHook error:', String(e?.message || e || ''));
      }
      isParsing = false;
      lastRunMarketplace = null;
      return;
    }

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

let wbAutoparseTickBusy = false;

async function tickWbAutoparseScheduler() {
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY) return;
  if (isParsing || wbAutoparseTickBusy) return;
  let due;
  try {
    due = await supabaseFetchDueWbAutoparseUsers();
  } catch (e) {
    console.error('tickWbAutoparseScheduler list:', String(e?.message || e || ''));
    return;
  }
  if (!due.length) return;

  const job = due[0];
  const chatId = Number(job.telegram_user_id);
  if (!Number.isFinite(chatId)) return;

  wbAutoparseTickBusy = true;
  try {
    const rows = await supabaseFetchMarketSettingsAll(chatId).catch(() => []);
    const wbRow = rows.find((r) => String(r.marketplace || '').toLowerCase() === 'wb');
    if (!wbRow) {
      await supabaseUpsertWbAutoparseState(chatId, {
        next_scheduled_at: new Date(Date.now() + 30 * 60_000).toISOString(),
      });
      return;
    }

    const s = mapRowToMarketSettings(wbRow);
    const fp = wbSettingsFingerprintFromDraft({
      query: s.query,
      extraKeywords: s.extraKeywords,
      minPrice: s.minPrice,
      maxPrice: s.maxPrice,
      memory: s.memory,
      color: s.color,
    });

    const st = await supabaseFetchWbAutoparseState(chatId).catch(() => null);
    const storedFp = st?.settings_fingerprint || '';
    if (storedFp !== fp) {
      await supabaseDeleteWbSeenListings(chatId);
      await supabaseUpsertWbAutoparseState(chatId, {
        baseline_ready: false,
        settings_fingerprint: fp,
      });
    }

    const snapshotPath = path.join(PROJECT_DIR, `.wb_snap_${chatId}_${Date.now()}.json`);
    const intervalMinutes = Number(job.interval_minutes) > 0 ? Number(job.interval_minutes) : 30;

    await supabaseUpsertWbAutoparseState(chatId, {
      is_running: true,
      last_run_started_at: new Date().toISOString(),
      settings_fingerprint: fp,
    });

    const parserOverrides = {
      marketplace: 'wb',
      query: s.query || 'iPhone',
      extraKeywords: s.extraKeywords || '',
      city: '',
      minPrice: s.minPrice || 0,
      maxPrice: s.maxPrice || 0,
      memory: s.memory || '',
      onlyToday: false,
      color: s.color || '',
    };

    await runParserForMarketplace({
      chatId,
      marketplace: 'wb',
      parserOverrides,
      silent: true,
      wbSnapshotPath: snapshotPath,
      wbAutoparseHook: async (exitCode, stopped, newExcelFiles) => {
        await processWbAutoparseResult(
          chatId,
          exitCode,
          snapshotPath,
          intervalMinutes,
          stopped,
          (newExcelFiles || []).map((f) => f.filePath)
        );
      },
    });
  } catch (e) {
    console.error('tickWbAutoparseScheduler run:', String(e?.message || e || ''));
    try {
      await supabaseUpsertWbAutoparseState(chatId, {
        is_running: false,
        next_scheduled_at: new Date(Date.now() + 15 * 60_000).toISOString(),
      });
    } catch (_) {
      /* ignore */
    }
  } finally {
    wbAutoparseTickBusy = false;
  }
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

async function sendExcelFilesToChat(chatId) {
  const files = listExcelFilesOnServer();
  if (!files.length) {
    await sendMessage(chatId, 'Excel-файлов пока нет. Запусти парсинг и потом нажми `Эксель файлы`.', mainKeyboard);
    return;
  }

  await syncExcelFilesToDb(chatId);

  const listText =
    'На сервере найдено Excel файлов: \n' +
    files
      .slice(0, 30)
      .map((f) => `• ${f.fileName} (${formatBytes(f.sizeBytes)})`)
      .join('\n') +
    (files.length > 30 ? `\n... и ещё ${files.length - 30}` : '');

  await sendMessage(chatId, listText, mainKeyboard);
  const toSend = files.slice(0, 30);
  let sent = 0;
  for (const f of toSend) {
    try {
      await sendDocument(chatId, f.filePath, `📎 Файл: ${f.fileName}`);
      sent += 1;
      await sleep(250);
    } catch (e) {
      console.error('Send excel document error:', String(e?.message || e || ''));
    }
  }
  if (sent === 0) {
    await sendMessage(chatId, '⚠️ Не удалось отправить файлы в Telegram, но они сохранены на сервере.', mainKeyboard);
  } else if (files.length > toSend.length) {
    await sendMessage(chatId, `ℹ️ Отправил ${toSend.length} файлов. Остальные можно получить следующим запросом.`, mainKeyboard);
  }
}

async function handleMessage(msg) {
  if (!msg || !msg.chat || typeof msg.chat.id !== 'number') return;
  const chatId = msg.chat.id;
  const text = String(msg.text || '').trim();
  if (!text) return;

  const state = getUserState(chatId);

  if (text === WIZARD_CANCEL) {
    if (state.returnToAutoFiltersHub) {
      state.runDraft = null;
      state.configureOnly = false;
      state.selectedMarketplaceForRun = null;
      state.selectedMarketplace = null;
      state.launchMode = false;
      state.excelDeleteCandidates = [];
      state.manualRunEphemeral = false;
      state.returnToAutoFiltersHub = false;
      try {
        await sendAutoFiltersHubScreen(chatId, msg.from?.id, '❌ Мастер отменён.');
      } catch (e) {
        await sendMessage(chatId, `⚠️ ${String(e?.message || e || '')}`, buildAutoFiltersHubKeyboard());
      }
      return;
    }
    state.stage = 'main';
    state.runDraft = null;
    state.configureOnly = false;
    state.selectedMarketplaceForRun = null;
    state.selectedMarketplace = null;
    state.launchMode = false;
    state.excelDeleteCandidates = [];
    state.manualRunEphemeral = false;
    await sendMessage(chatId, '❌ Отменено. Возврат в главное меню.', mainKeyboard);
    return;
  }

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

  if (state.stage === 'auto_filters_hub') {
    if (text === AUTO_BACK_TO_AUTO_MAIN) {
      state.stage = 'auto_settings_main';
      await sendMessage(
        chatId,
        '⚙️ *Настройки автопарсинга*\n\nВыберите действие:',
        buildAutoSettingsMainKeyboard(),
        'Markdown'
      );
      return;
    }
    if (text === AUTO_EDIT_AVITO) {
      try {
        const rows = await supabaseFetchMarketSettingsAll(msg.from?.id);
        const avitoRow = rows.find((r) => String(r.marketplace || '').toLowerCase() === 'avito');
        state.marketSettings.avito = avitoRow ? mapRowToMarketSettings(avitoRow) : null;
        const current = state.marketSettings.avito;
        state.configureOnly = true;
        state.returnToAutoFiltersHub = true;
        state.runDraft = {
          marketplace: 'avito',
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
          '🛒 *Авито* — шаг 1/8: название поиска (для ручного запуска):',
          skipKeyboard(),
          'Markdown'
        );
      } catch (e) {
        await sendMessage(chatId, `⚠️ ${String(e?.message || e || '')}`, buildAutoFiltersHubKeyboard());
      }
      return;
    }
    if (text === AUTO_EDIT_WB) {
      try {
        const rows = await supabaseFetchMarketSettingsAll(msg.from?.id);
        const wbRow = rows.find((r) => String(r.marketplace || '').toLowerCase() === 'wb');
        state.marketSettings.wb = wbRow ? mapRowToMarketSettings(wbRow) : null;
        const current = state.marketSettings.wb;
        state.configureOnly = true;
        state.returnToAutoFiltersHub = true;
        state.runDraft = {
          marketplace: 'wb',
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
          '🛍️ *Wildberries* — шаг 1/7: название поиска (автопарсинг):',
          skipKeyboard(),
          'Markdown'
        );
      } catch (e) {
        await sendMessage(chatId, `⚠️ ${String(e?.message || e || '')}`, buildAutoFiltersHubKeyboard());
      }
      return;
    }
    await sendMessage(chatId, '👇 Выберите площадку кнопкой или вернитесь назад.', buildAutoFiltersHubKeyboard());
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
    state.stage = 'auto_settings_main';
    const st = await supabaseFetchWbAutoparseState(msg.from?.id).catch(() => null);
    await sendMessage(
      chatId,
      '⚙️ *Настройки автопарсинга*\n\n' +
        '*Wildberries:* по расписанию собирает выдачу; первый прогон создаёт базу без уведомлений, затем приходят только *новые* объявления.\n\n' +
        '*Фильтры:* для Авито и ВБ хранятся отдельно — задайте их в пункте «Настроить фильтры».\n\n' +
        'Выберите действие:',
      buildAutoSettingsMainKeyboard(st),
      'Markdown'
    );
    return;
  }

  if (text === MENU.checkAutoparse) {
    try {
      const st = await supabaseFetchWbAutoparseState(msg.from?.id);
      const runningDb = Boolean(st?.is_running);
      const runningLocal = (isParsing && lastRunMarketplace === 'wb') || wbAutoparseWbRunning;
      const running = runningDb || runningLocal;
      const fmt = (iso) => {
        if (!iso) return '—';
        try {
          return new Date(iso).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' });
        } catch (_) {
          return String(iso);
        }
      };
      let lines = ['🔎 *Статус автопарсинга ВБ*\n'];
      if (running) {
        lines.push('⏳ Сейчас выполняется парсинг Wildberries.');
      } else {
        lines.push('✅ Сейчас парсинг не выполняется.');
      }
      if (st) {
        lines.push(`\n⏱ Интервал: ${st.interval_minutes || 30} мин`);
        lines.push(st.enabled === false ? '\n⏸ Расписание: *выключено*' : '\n▶️ Расписание: *включено*');
        lines.push(st.save_excel === false ? '\n📊 Сохранение Excel: *ВЫКЛ*' : '\n📊 Сохранение Excel: *ВКЛ*');
        lines.push(`\n🕐 Последний старт: ${fmt(st.last_run_started_at)}`);
        const okTag =
          st.last_run_ok === false ? ' (с ошибкой)' : st.last_run_ok === true ? ' (успех)' : '';
        lines.push(`\n🕔 Последнее завершение: ${fmt(st.last_run_finished_at)}${okTag}`);
        lines.push(`\n📅 Следующий запуск (по плану): ${fmt(st.next_scheduled_at)}`);
        lines.push(`\n📚 База для сравнения: ${st.baseline_ready ? 'готова' : 'ещё не создана (первый прогон)'}`);
      } else {
        lines.push('\nВ БД ещё нет записи расписания. Откройте «Настроить периодичность».');
      }
      await sendMessage(chatId, lines.join(''), mainKeyboard, 'Markdown');
    } catch (e) {
      await sendMessage(chatId, `⚠️ Не удалось прочитать статус: ${String(e?.message || e || '')}`, mainKeyboard);
    }
    return;
  }

  if (state.stage === 'auto_settings_main') {
    if (text === MENU.backToMenu) {
      state.stage = 'main';
      await sendMessage(chatId, '⬅️ Главное меню.', mainKeyboard);
      return;
    }
    if (text === AUTO_WB_FILTERS) {
      try {
        await sendAutoFiltersHubScreen(chatId, msg.from?.id);
      } catch (e) {
        await sendMessage(chatId, `⚠️ ${String(e?.message || e || '')}`, buildAutoSettingsMainKeyboard());
      }
      return;
    }
    if (text === AUTO_WB_EXCEL_ON || text === AUTO_WB_EXCEL_OFF) {
      try {
        const st = await supabaseFetchWbAutoparseState(msg.from?.id);
        const current = st == null ? true : st.save_excel !== false;
        const next = !current;
        await supabaseUpsertWbAutoparseState(msg.from?.id, { save_excel: next });
        const updated = await supabaseFetchWbAutoparseState(msg.from?.id);
        await sendMessage(
          chatId,
          next
            ? '✅ Сохранение Excel для автопарсинга ВБ: *ВКЛ*.'
            : '✅ Сохранение Excel для автопарсинга ВБ: *ВЫКЛ*.',
          buildAutoSettingsMainKeyboard(updated),
          'Markdown'
        );
      } catch (e) {
        await sendMessage(chatId, `⚠️ ${String(e?.message || e || '')}`, buildAutoSettingsMainKeyboard());
      }
      return;
    }
    if (text === AUTO_WB_INTERVAL) {
      try {
        const st = await supabaseFetchWbAutoparseState(msg.from?.id);
        state.stage = 'wb_auto_interval';
        await sendMessage(
          chatId,
          '⏱ *Периодичность* — как часто после завершения парсинга ждать следующий запуск.\n\n' +
            'Выберите интервал или включите/выключите автозапуск.\n' +
            'Текущее: ' +
            (st ? `${st.interval_minutes || 30} мин, ${st.enabled === false ? 'ВЫКЛ' : 'ВКЛ'}` : 'по умолчанию 30 мин'),
          buildWbIntervalKeyboard(st),
          'Markdown'
        );
        return;
      } catch (e) {
        await sendMessage(chatId, `⚠️ ${String(e?.message || e || '')}`, buildAutoSettingsMainKeyboard());
        return;
      }
    }
    await sendMessage(chatId, '👇 Выберите пункт меню автопарсинга.', buildAutoSettingsMainKeyboard());
    return;
  }

  if (state.stage === 'wb_auto_interval') {
    if (text === MENU.backToMenu) {
      state.stage = 'auto_settings_main';
      await sendMessage(chatId, '⬅️ Назад.', buildAutoSettingsMainKeyboard());
      return;
    }
    const intervalMap = {
      '10 мин': 10,
      '15 мин': 15,
      '30 мин': 30,
      '45 мин': 45,
      '60 мин': 60,
    };
    try {
      if (text === AUTO_WB_ENABLED) {
        await supabaseUpsertWbAutoparseState(msg.from?.id, {
          enabled: true,
          next_scheduled_at: new Date().toISOString(),
        });
        const st = await supabaseFetchWbAutoparseState(msg.from?.id);
        await sendMessage(chatId, '✅ Автопарсинг *включён*. Первый слот — с ближайшей минуты.', buildWbIntervalKeyboard(st), 'Markdown');
        return;
      }
      if (text === AUTO_WB_DISABLED) {
        await supabaseUpsertWbAutoparseState(msg.from?.id, { enabled: false });
        const st = await supabaseFetchWbAutoparseState(msg.from?.id);
        await sendMessage(chatId, '⏸ Автопарсинг *выключен*. Расписание не запускается.', buildWbIntervalKeyboard(st), 'Markdown');
        return;
      }
      const mins = intervalMap[text];
      if (mins) {
        await supabaseUpsertWbAutoparseState(msg.from?.id, {
          interval_minutes: mins,
          next_scheduled_at: new Date().toISOString(),
        });
        const st = await supabaseFetchWbAutoparseState(msg.from?.id);
        await sendMessage(
          chatId,
          `✅ Интервал сохранён: *${mins} мин*. Следующая проверка запланирована.`,
          buildWbIntervalKeyboard(st),
          'Markdown'
        );
        return;
      }
    } catch (e) {
      await sendMessage(chatId, `⚠️ ${String(e?.message || e || '')}`, buildWbIntervalKeyboard({}));
      return;
    }
    await sendMessage(chatId, 'Выберите интервал кнопкой.', buildWbIntervalKeyboard(await supabaseFetchWbAutoparseState(msg.from?.id)));
    return;
  }

  if (text === MENU.guide) {
    state.stage = 'main';
    await sendMessage(
      chatId,
      '📘 *Инструкция*\n\n' +
        '1️⃣ _⚙️ Настройки автопарсинга_ → «Настроить фильтры» (видны сохранённые настройки Авито и ВБ, выбор площадки) или «Настроить периодичность» (только ВБ). Первый автообход ВБ создаёт базу без уведомлений, затем — только *новые*.\n\n' +
        '2️⃣ _🔎 Проверить автопарсинг_ — статус и время следующего автообхода ВБ.\n\n' +
        '3️⃣ _🚀 Ручной запуск_ → площадка → режим:\n\n' +
        '    • _▶️ Продолжить с настройками автопарсинга_\n' +
        '    • _✍️ Задать вручную_\n\n' +
        '4️⃣ На шагах мастера: _❌ Отмена_ — возврат к экрану фильтров или в меню.\n\n' +
        '5️⃣ При *ручном* парсинге бот пишет этапы:\n\n' +
        '    • 🌐 Захожу на страницу\n' +
        '    • ✅ Страница открыта, собираю данные\n' +
        '    • ✅ Парсинг завершен, ваш файл сохранен\n\n' +
        '6️⃣ _📁 Эксель файлы_:\n\n' +
        '    • _👀 Посмотреть_ — отправка файлов\n' +
        '    • _🗑 Удалить_ — с сервера и из БД\n\n' +
        '────────────\n\n' +
        '🩹 При ручном парсинге: _⛔ Остановить парсинг_.',
      mainKeyboard,
      'Markdown'
    );
    return;
  }

  if (text === MENU.excels) {
    state.stage = 'excel_menu';
    await sendMessage(chatId, '📁 Раздел Excel файлов. Выберите действие:', buildExcelMenuKeyboard());
    return;
  }

  if (state.stage === 'excel_menu') {
    if (text === EXCEL_VIEW) {
      state.stage = 'main';
      await sendExcelFilesToChat(chatId);
      return;
    }
    if (text === EXCEL_DELETE) {
      const files = listExcelFilesOnServer();
      if (!files.length) {
        state.stage = 'main';
        await sendMessage(chatId, 'Excel-файлов для удаления нет.', mainKeyboard);
        return;
      }
      state.stage = 'excel_delete_pick';
      state.excelDeleteCandidates = files.map((f) => f.fileName);
      await sendMessage(chatId, '🗑 Выберите файл для удаления:', buildExcelDeleteKeyboard(files));
      return;
    }
    if (text === MENU.backToMenu) {
      state.stage = 'main';
      await sendMessage(chatId, '⬅️ Возврат в меню.', mainKeyboard);
      return;
    }
    await sendMessage(chatId, '👇 Выберите действие кнопкой снизу.', buildExcelMenuKeyboard());
    return;
  }

  if (state.stage === 'excel_delete_pick') {
    if (text === MENU.backToMenu) {
      state.stage = 'main';
      state.excelDeleteCandidates = [];
      await sendMessage(chatId, '⬅️ Возврат в меню.', mainKeyboard);
      return;
    }
    const fileName = String(text || '').trim();
    const currentFiles = listExcelFilesOnServer();
    const target = currentFiles.find((f) => f.fileName === fileName);
    if (!target) {
      await sendMessage(chatId, '⚠️ Такой файл не найден. Выберите файл кнопкой из списка.', buildExcelDeleteKeyboard(currentFiles));
      return;
    }
    try {
      if (fs.existsSync(target.filePath)) {
        fs.unlinkSync(target.filePath);
      }
    } catch (e) {
      await sendMessage(chatId, `⚠️ Не удалось удалить файл с сервера: ${String(e?.message || e || '')}`, buildExcelDeleteKeyboard(currentFiles));
      return;
    }
    try {
      await supabaseDeleteExcelFileByPath(target.filePath);
    } catch (e) {
      console.error('Excel DB delete error:', String(e?.message || e || ''));
    }
    const after = listExcelFilesOnServer();
    if (!after.length) {
      state.stage = 'main';
      state.excelDeleteCandidates = [];
      await sendMessage(chatId, `✅ Файл удален: ${target.fileName}\nБольше файлов не осталось.`, mainKeyboard);
      return;
    }
    state.excelDeleteCandidates = after.map((f) => f.fileName);
    await sendMessage(chatId, `✅ Файл удален: ${target.fileName}\nВыберите следующий файл для удаления:`, buildExcelDeleteKeyboard(after));
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
    await sendMessage(
      chatId,
      `⚙️ Настройка ${m === 'avito' ? 'Авито' : 'ВБ'}.\n📝 Шаг 1: Введите название поиска (например: iPhone):`,
      skipKeyboard()
    );
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
      state.manualRunEphemeral = true;
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
        skipKeyboard()
      );
      return;
    }

    if (text === RUN_CONTINUE_AUTO) {
      state.manualRunEphemeral = false;
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
      state.manualRunEphemeral = false;
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
    await sendMessage(chatId, '📝 Шаг 2/7: Введите модель (например: 15 Pro Max) или пропустите:', skipKeyboard());
    return;
  }

  if (state.stage === 'run_model') {
    state.runDraft.extraKeywords = text === WIZARD_SKIP ? '' : text;
    if (state.runDraft.marketplace === 'avito') {
      state.stage = 'run_city';
      await sendMessage(chatId, '🌆 Шаг 3/8: Введите город Avito (например: Москва) или пропустите:', skipKeyboard());
      return;
    }
    state.stage = 'run_min_price';
    await sendMessage(chatId, '💰 Шаг 3/7: Цена ОТ (только число) или 0:', numberKeyboard('0'));
    return;
  }

  if (state.stage === 'run_city') {
    state.runDraft.city = text === WIZARD_SKIP ? 'moskva' : text;
    state.stage = 'run_min_price';
    await sendMessage(chatId, '💰 Шаг 4/8: Цена ОТ (только число) или 0:', numberKeyboard('0'));
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
      numberKeyboard('0')
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
      const ephemeralManual = Boolean(state.manualRunEphemeral && !state.configureOnly);

      if (!ephemeralManual) {
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
      }

      state.stage = 'main';
      if (state.configureOnly) {
        state.configureOnly = false;
        const backAuto = state.returnToAutoFiltersHub;
        state.returnToAutoFiltersHub = false;
        if (backAuto && d.marketplace === 'wb') {
          let wbNotice = '✅ Фильтры Wildberries сохранены в БД.\n\n';
          try {
            const prev = await supabaseFetchWbAutoparseState(msg.from?.id);
            const fp = wbSettingsFingerprintFromDraft(d);
            const fpChanged = (prev?.settings_fingerprint || '') !== fp;
            if (fpChanged) {
              await supabaseDeleteWbSeenListings(msg.from?.id);
            }
            await supabaseUpsertWbAutoparseState(msg.from?.id, {
              settings_fingerprint: fp,
              baseline_ready: fpChanged ? false : Boolean(prev?.baseline_ready),
              interval_minutes: prev?.interval_minutes > 0 ? prev.interval_minutes : 30,
              enabled: prev == null ? true : prev.enabled !== false,
              next_scheduled_at: new Date().toISOString(),
            });
            if (fpChanged) {
              wbNotice +=
                '📭 База объявлений для автопарсинга очищена.\n\n' +
                'Следующий автозапуск Wildberries создаст новую базу объявлений (по этому прогону уведомлений не будет). После этого снова будут приходить только новые позиции.';
            } else {
              wbNotice +=
                'Набор фильтров не менялся — таблица «уже виденных» объявлений не сбрасывалась.';
            }
          } catch (e) {
            console.error('wb_autoparse on filter save:', String(e?.message || e || ''));
            wbNotice += '⚠️ Не удалось обновить состояние автопарсинга в БД — см. логи сервера.';
          }
          await sendAutoFiltersHubScreen(chatId, msg.from?.id, wbNotice);
          return;
        }
        if (backAuto && d.marketplace === 'avito') {
          await sendAutoFiltersHubScreen(chatId, msg.from?.id, '✅ Фильтры Авито сохранены в БД.');
          return;
        }
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

      state.manualRunEphemeral = false;
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
      if (state.returnToAutoFiltersHub) {
        state.returnToAutoFiltersHub = false;
        state.runDraft = null;
        state.configureOnly = false;
        state.manualRunEphemeral = false;
        try {
          await sendAutoFiltersHubScreen(chatId, msg.from?.id);
        } catch (e) {
          await sendMessage(chatId, `⚠️ ${String(e?.message || e || '')}`, buildAutoFiltersHubKeyboard());
        }
        return;
      }
      state.stage = 'main';
      state.runDraft = null;
      state.configureOnly = false;
      state.manualRunEphemeral = false;
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
  await supabaseResetAllWbAutoparseRunning();
  setInterval(() => {
    tickWbAutoparseScheduler().catch((e) => console.error('tickWbAutoparseScheduler:', String(e?.message || e || '')));
  }, 60_000);
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

