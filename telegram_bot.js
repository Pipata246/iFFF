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
  manualRun: 'Ручной запуск',
  autoSettings: 'Настройки автопарсинга',
  guide: 'Инструкция',
  excels: 'Эксель файлы',
  backToMenu: 'Назад в меню',
};

const STOP_PARSING_TEXT = 'Остановить парсинг';

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
      const resp = await fetch(`${TG_API}/${method}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(payload || {}),
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

async function sendMessage(chatId, text, replyMarkup) {
  return tgApi('sendMessage', {
    chat_id: chatId,
    text,
    reply_markup: replyMarkup || mainKeyboard,
  });
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
    });
  }
  return userStateByChatId.get(chatId);
}

function buildMarketplaceKeyboard() {
  // EXACTLY 2 buttons as requested
  return {
    keyboard: [[{ text: 'Авито' }, { text: 'ВБ' }]],
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

  const resp = await fetch(`${SUPABASE_URL}/rest/v1/excel_files`, {
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
    throw new Error(`Supabase excel_files upsert failed: HTTP ${resp.status}: ${txt}`);
  }
}

let isParsing = false;
let lastRunMarketplace = null;
const activeChildByChatId = new Map(); // chatId -> child process
const stopRequestedByChatId = new Map(); // chatId -> boolean

function buildParserEnvForRun({ chatId, marketplace, settings }) {
  // main.js переключаем в env mode (без readline)
  return {
    ...process.env,
    PARSER_USE_ENV: '1',
    PARSER_MARKETPLACE: marketplace,
    // Always run unattended (no manual proxy prompts) for VPS bot runs
    AVITO_MANUAL_PROXY: '0',
    AVITO_WAIT_ENTER: '0',
    // Ensure no WB home warmup (avoid going to WB home)
    WB_USE_HOME_WARMUP: '0',
    // Force headless by default on VPS
    PLAYWRIGHT_HEADLESS: '1',
    // Keep memory stable on 1GB RAM VPS
    NODE_OPTIONS: process.env.NODE_OPTIONS || '--max-old-space-size=384',
    PARSER_QUERY: process.env.PARSER_QUERY || 'iPhone 15',
    PARSER_EXTRA_KEYWORDS: process.env.PARSER_EXTRA_KEYWORDS || '',
    PARSER_CITY: process.env.PARSER_CITY || 'moskva',
    PARSER_MIN_PRICE: settings.priceFilterEnabled ? String(settings.minPrice) : '0',
    PARSER_MAX_PRICE: settings.priceFilterEnabled ? String(settings.maxPrice) : '0',
    PARSER_MEMORY: settings.memoryFilterEnabled ? String(settings.memoryGb) : '',
    PARSER_SELLER_TYPE: settings.sellerType || 'any',
    PARSER_MIN_RATING: String(settings.minRating || 0),
    PARSER_ONLY_TODAY: settings.onlyToday ? '1' : '0',
    PARSER_COLOR: marketplace === 'wb' || marketplace === 'both' ? settings.colorFilterEnabled ? settings.color : '' : '',
  };
}

async function runParserForMarketplace({ chatId, marketplace }) {
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
  const mainLogKeywords = ['Шаг', 'ОБНАРУЖЕН БЛОК', 'ПОВТОР', 'УСПЕХ', 'Файл результатов'];
  let stage1Sent = false;
  let stage2Sent = false;
  let stage3Sent = false;
  let lastLogSendAt = 0;
  const logTail = [];

  const beforeFiles = listExcelFilesOnServer();
  const beforePaths = new Set(beforeFiles.map((f) => f.filePath));

  await sendMessage(
    chatId,
    `Парсинг запущен: ${marketplace === 'avito' ? 'Авито' : 'ВБ'}.\nНажми кнопку «${STOP_PARSING_TEXT}», чтобы остановить.`,
    stopKeyboard
  );

  const env = buildParserEnvForRun({ chatId, marketplace, settings });

  // Run main.js directly so we can stop it and stream stdout/stderr.
  const child = spawn('node', ['main.js'], {
    cwd: PROJECT_DIR,
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
    detached: false,
  });

  activeChildByChatId.set(chatId, child);

  function extractResultsFile(line) {
    const m = line && line.match(/(results_[^\s]+\.xlsx)/i);
    return m ? m[1] : null;
  }

  async function maybeSendLogs(force = false) {
    const now = Date.now();
    if (!force && now - lastLogSendAt < 8000) return;
    lastLogSendAt = now;
    const slice = logTail.slice(-20);
    if (!slice.length) return;
    await sendMessage(chatId, `Логи (последние):\n${slice.join('\n')}`, stopKeyboard);
  }

  function handleParserLine(line) {
    const s = String(line || '').trimEnd();
    if (!s) return;

    const important = mainLogKeywords.some((k) => s.includes(k));
    if (important) {
      logTail.push(s);
      if (logTail.length > 80) logTail.splice(0, logTail.length - 80);
    }

    if (!stage1Sent && s.includes('Шаг 1') && s.includes('Запуск браузера')) {
      stage1Sent = true;
      sendMessage(chatId, '1) Открываю страницу', stopKeyboard).catch(() => {});
    }

    if (
      !stage2Sent &&
      (s.includes('Шаг 5') ||
        s.includes('Парсинг страницы выдачи') ||
        s.includes('Ожидание карточек') ||
        s.includes('Карточки товаров') ||
        s.includes('Скролл ленты'))
    ) {
      stage2Sent = true;
      sendMessage(chatId, '2) Страница открыта, начинаю парсинг', stopKeyboard).catch(() => {});
    }

    if (!stage3Sent && s.includes('Файл результатов')) {
      stage3Sent = true;
      const resultsFile = extractResultsFile(s);
      const msg = resultsFile
        ? `3) Парсинг завершен, ваш файл сохранен: ${resultsFile}`
        : '3) Парсинг завершен, ваш файл сохранен';
      sendMessage(chatId, msg, stopKeyboard).catch(() => {});
      maybeSendLogs(true).catch(() => {});
    } else if (important && (s.includes('Шаг') || s.includes('ОБНАРУЖЕН БЛОК') || s.includes('ПОВТОР'))) {
      // occasionally push log chunk
      maybeSendLogs(false).catch(() => {});
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
        await sendMessage(chatId, 'Парсинг остановлен.', mainKeyboard);
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
      if (newOnes.length === 0) {
        await sendMessage(
          chatId,
          `Парсинг завершен (exit code: ${code}). Но новых Excel файлов не найдено.`,
          mainKeyboard
        );
      } else {
        // store excel metadata to DB
        if (SUPABASE_URL && SUPABASE_ANON_KEY) {
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

        await sendMessage(
          chatId,
          `Готово! Создано ${newOnes.length} Excel файлов:\n` +
            newOnes.map((f) => `• ${f.fileName}`).join('\n'),
          mainKeyboard
        );
      }
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
    await sendMessage(chatId, 'Парсинг остановлен.', mainKeyboard);
    return;
  }

  // Upsert Telegram user in DB on /start
  if (/^\/start(?:@\w+)?(?:\s+.*)?$/i.test(text)) {
    try {
      await supabaseUpsertUser(msg.from?.id, msg.from || {});
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
    await sendMessage(chatId, 'Выберите площадку:', buildMarketplaceKeyboard());
    return;
  }

  if (text === MENU.autoSettings) {
    state.stage = 'settings';
    state.launchMode = false;
    state.selectedMarketplace = null;
    await sendMessage(
      chatId,
      'Настройки автопарсинга. Нажимай кнопки ДА/НЕТ и значения.',
      buildSettingsKeyboard(state.settings, { launchMode: false })
    );
    return;
  }

  if (text === MENU.guide) {
    state.stage = 'main';
    await sendMessage(
      chatId,
      'Как пользоваться:\n' +
        '1) `Настройки автопарсинга` — включи нужные фильтры кнопками.\n' +
        '2) `Ручной запуск` — выбери `Авито` или `ВБ`.\n' +
        '3) `Эксель файлы` — получишь список созданных Excel.\n',
      mainKeyboard
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
    return;
  }

  if (text === MENU.backToMenu) {
    state.stage = 'main';
    await sendMessage(chatId, 'Ок, меню.', mainKeyboard);
    return;
  }

  // Stage: choosing marketplace
  if (state.stage === 'choosing_market') {
    if (text === 'Авито') {
      state.selectedMarketplace = 'avito';
      state.launchMode = true;
      state.stage = 'settings';
      await sendMessage(
        chatId,
        'Настройки парсинга для Авито. Выбери фильтры и нажми `Запустить парсинг`.',
        buildSettingsKeyboard(state.settings, { launchMode: true })
      );
      return;
    }
    if (text === 'ВБ') {
      state.selectedMarketplace = 'wb';
      state.launchMode = true;
      state.stage = 'settings';
      await sendMessage(
        chatId,
        'Настройки парсинга для ВБ. Выбери фильтры и нажми `Запустить парсинг`.',
        buildSettingsKeyboard(state.settings, { launchMode: true })
      );
      return;
    }
    // ignore other texts in this stage
    await sendMessage(chatId, 'Выбери: Авито или ВБ.', buildMarketplaceKeyboard());
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

