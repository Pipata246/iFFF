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
