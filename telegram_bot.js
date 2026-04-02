const fs = require('fs');
const path = require('path');

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
      if (process.env[key] == null || process.env[key] === '') {
        process.env[key] = val;
      }
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

const MENU = {
  manualRun: 'Ручной запуск',
  autoSettings: 'Настройки автопарсинга',
  guide: 'Инструкция',
  excels: 'Эксель файлы',
};

const replyKeyboard = {
  keyboard: [
    [{ text: MENU.manualRun }, { text: MENU.autoSettings }],
    [{ text: MENU.guide }, { text: MENU.excels }],
  ],
  resize_keyboard: true,
  one_time_keyboard: false,
};

const startText =
  '🤖 Привет! Это мини-инструкция по боту.\n\n' +
  '🔎 Бот помогает отслеживать выгодные iPhone на Авито и Wildberries.\n\n' +
  '⏱️ Проверка запускается автоматически и в ручном режиме.\n\n' +
  '📌 Можно настраивать параметры: цена, память, цвет и площадка.\n\n' +
  '📁 Результаты сохраняются в Excel.\n\n' +
  '⚡️ Отправьте команду или нажмите кнопку меню.';

const TG_API = `https://api.telegram.org/bot${token}`;
let offset = Number(process.env.TELEGRAM_BOT_OFFSET || 0);
let running = true;

const SUPABASE_URL = (process.env.SUPABASE_URL || '').trim().replace(/\/+$/, '');
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || '';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function isRetryableNetErr(err) {
  const s = String(err?.message || err || '').toUpperCase();
  return (
    s.includes('ECONNRESET') ||
    s.includes('ETIMEDOUT') ||
    s.includes('EAI_AGAIN') ||
    s.includes('ENOTFOUND') ||
    s.includes('UNDICI') ||
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
      await sleep(800 * attempt);
    }
  }
  throw lastErr;
}

async function sendMessage(chatId, text) {
  return tgApi('sendMessage', {
    chat_id: chatId,
    text,
    reply_markup: replyKeyboard,
  });
}

async function handleMessage(msg) {
  if (!msg || !msg.chat || typeof msg.chat.id !== 'number') return;
  const chatId = msg.chat.id;
  const text = String(msg.text || '').trim();
  if (!text) return;

  if (/^\/start(?:@\w+)?(?:\s+.*)?$/i.test(text)) {
    // Insert user into Supabase on /start.
    // If Supabase vars are missing, we silently skip to keep bot working.
    try {
      const from = msg.from;
      const tgUserId = from && from.id != null ? String(from.id) : '';
      const hasSupabaseCfg = Boolean(SUPABASE_URL && SUPABASE_ANON_KEY);
      if (!hasSupabaseCfg) {
        console.warn('Supabase insert skipped: missing SUPABASE_URL and/or SUPABASE_ANON_KEY');
      }
      if (SUPABASE_URL && SUPABASE_ANON_KEY && tgUserId) {
        const payload = [
          {
            telegram_user_id: tgUserId,
            telegram_username: from.username || null,
            first_name: from.first_name || null,
            last_name: from.last_name || null,
          },
        ];

        const resp = await fetch(`${SUPABASE_URL}/rest/v1/users`, {
          method: 'POST',
          headers: {
            apikey: SUPABASE_ANON_KEY,
            Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
            'content-type': 'application/json',
            Prefer: 'resolution=merge-duplicates,return=minimal',
          },
          body: JSON.stringify(payload),
        });
        if (!resp.ok) {
          const txt = await resp.text().catch(() => '');
          throw new Error(`Supabase HTTP ${resp.status}: ${txt || '(empty body)'}`);
        }
      }
    } catch (err) {
      console.error('Supabase insert error:', String(err?.message || err || ''));
    }

    await sendMessage(chatId, startText);
    return;
  }

  if (
    text === MENU.manualRun ||
    text === MENU.autoSettings ||
    text === MENU.guide ||
    text === MENU.excels
  ) {
    await sendMessage(chatId, 'Все работает');
  }
}

async function pollLoop() {
  console.log('Telegram-бот запущен (long polling, lightweight).');
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
