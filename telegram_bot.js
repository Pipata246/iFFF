const TelegramBot = require('node-telegram-bot-api');

const token = process.env.TELEGRAM_BOT_TOKEN || process.env.BOT_TOKEN;
if (!token) {
  console.error('Ошибка: не задан TELEGRAM_BOT_TOKEN (или BOT_TOKEN).');
  process.exit(1);
}

const bot = new TelegramBot(token, { polling: true });

const MENU = {
  manualRun: 'Ручной запуск',
  autoSettings: 'Настройки автопарсинга',
  guide: 'Инструкция',
  excels: 'Эксель файлы',
};

const replyKeyboard = {
  keyboard: [[{ text: MENU.manualRun }, { text: MENU.autoSettings }], [{ text: MENU.guide }, { text: MENU.excels }]],
  resize_keyboard: true,
  one_time_keyboard: false,
  selective: false,
};

const startText =
  '🤖 Привет! Это мини-инструкция по боту.\n\n' +
  '🔎 Бот помогает отслеживать выгодные iPhone на Авито и Wildberries.\n\n' +
  '⏱️ Проверка запускается регулярно (например, каждые 15 минут) и в ручном режиме.\n\n' +
  '📌 Параметры поиска настраиваются: цена, память, цвет и площадки.\n\n' +
  '📁 Результаты сохраняются в Excel — можно быстро открыть и проверить новые предложения.\n\n' +
  '⚡️ Вы видите выгодные варианты первыми и не пропускаете свежие объявления.';

bot.onText(/^\/start$/i, async (msg) => {
  await bot.sendMessage(msg.chat.id, startText, {
    reply_markup: replyKeyboard,
  });
});

bot.on('message', async (msg) => {
  const text = (msg.text || '').trim();
  if (!text || text.startsWith('/start')) return;

  if (
    text === MENU.manualRun ||
    text === MENU.autoSettings ||
    text === MENU.guide ||
    text === MENU.excels
  ) {
    await bot.sendMessage(msg.chat.id, 'Все работает', {
      reply_markup: replyKeyboard,
    });
    return;
  }
});

console.log('Telegram-бот запущен.');
