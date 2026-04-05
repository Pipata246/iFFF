/**
 * Однократно: войти на wildberries.ru и сохранить cookies/localStorage в JSON для PARSER_WB_STORAGE_STATE.
 *
 * Wildberries часто блокирует встроенный Chromium Playwright («подозрительная активность»).
 * По умолчанию на Windows/macOS открывается установленный Google Chrome (меньше ложных блокировок).
 *
 * PowerShell (без прокси из browser.js — как в прошлой инструкции):
 *   $env:AVITO_NO_PROXY="1"; $env:PLAYWRIGHT_HEADLESS="0"; npm run wb:save-session
 *
 * Если Chrome нет — встроенный Chromium:
 *   $env:WB_SESSION_CHANNEL="chromium"; ...
 *
 * Вместо Chrome — системный Edge:
 *   $env:WB_SESSION_CHANNEL="msedge"; ...
 *
 * Если WB всё равно блокирует — вариант без «ботового» запуска: свой Chrome + отладка, см. save_wb_storage_cdp.js и npm run wb:save-session-cdp
 */

const readline = require('readline');
const path = require('path');
const { launchBrowser, newStealthContext } = require('./browser');

/**
 * @returns {string|undefined} channel для Playwright или undefined = bundled Chromium
 */
function pickWbSessionChannel() {
  const raw = String(process.env.WB_SESSION_CHANNEL || '').trim().toLowerCase();
  if (raw === '0' || raw === 'chromium' || raw === 'bundled' || raw === 'playwright') {
    return undefined;
  }
  if (raw === 'edge' || raw === 'msedge') {
    return 'msedge';
  }
  if (raw === 'chrome') {
    return 'chrome';
  }
  if (process.platform === 'win32' || process.platform === 'darwin') {
    return 'chrome';
  }
  return undefined;
}

async function main() {
  const outArg = process.argv[2] || 'wb_storage.json';
  const outAbs = path.isAbsolute(outArg) ? outArg : path.join(process.cwd(), outArg);

  if (!process.env.PLAYWRIGHT_HEADLESS) {
    process.env.PLAYWRIGHT_HEADLESS = '0';
  }

  const channel = pickWbSessionChannel();
  if (channel) {
    console.log(`Будет использован системный браузер (channel=${channel}), не встроенный Chromium Playwright.\n`);
  } else {
    console.log('Будет использован встроенный Chromium Playwright (если WB пишет «подозрительная активность» — см. шапку файла).\n');
  }

  console.log(
    'Войдите в Wildberries. Если видите «Подозрительная активность» и таймер — дождитесь окончания и обновления страницы.'
  );
  console.log('Иногда помогает: другой браузер (WB_SESSION_CHANNEL=msedge), другой интернет или повтор через 10–30 минут.\n');

  if (!(process.env.AVITO_NO_PROXY === '1' || process.env.AVITO_NO_PROXY === 'true')) {
    console.log(
      'Подсказка: с домашнего интернета часто нужен прокси OFF для этого шага (таймауты). Парсер на VPS прокси не трогайте.\n'
    );
    console.log('  $env:AVITO_NO_PROXY="1"; $env:PLAYWRIGHT_HEADLESS="0"; npm run wb:save-session\n');
  }

  let browser;
  try {
    browser = channel ? await launchBrowser({ channel }) : await launchBrowser();
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    if (channel) {
      console.warn(`Запуск с channel=${channel} не удался (${msg}). Пробуем встроенный Chromium…\n`);
      browser = await launchBrowser();
    } else {
      throw e;
    }
  }

  const context = await newStealthContext(browser);
  const page = await context.newPage();
  try {
    await page.goto('https://www.wildberries.ru/', {
      waitUntil: 'domcontentloaded',
      timeout: 180_000,
    });
  } catch (e) {
    console.error('Переход на WB:', e && e.message ? e.message : e);
  }

  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  await new Promise((resolve) => {
    rl.question('Нажмите Enter здесь после успешного входа и проверки цены с кошельком… ', () => {
      rl.close();
      resolve();
    });
  });

  await context.storageState({ path: outAbs });
  await browser.close();
  console.log('\nСохранено:', outAbs);
  console.log('В .env на VPS:');
  console.log(`PARSER_WB_STORAGE_STATE=${outAbs}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
