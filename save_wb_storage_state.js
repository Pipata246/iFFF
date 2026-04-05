/**
 * Однократно: войти на wildberries.ru в Chromium и сохранить cookies/localStorage
 * в JSON для PARSER_WB_STORAGE_STATE (чтобы парсер видел цены с WB Кошельком).
 *
 * Локально (окно браузера):
 *   npm run wb:save-session
 *   или: PLAYWRIGHT_HEADLESS=0 node save_wb_storage_state.js /путь/wb_storage.json
 *
 * На VPS лучше сохранить сессию там же (тот же IP/прокси, что у парсера), иначе WB может сбросить вход.
 */

const readline = require('readline');
const path = require('path');
const { launchBrowser, newStealthContext } = require('./browser');

async function main() {
  const outArg = process.argv[2] || 'wb_storage.json';
  const outAbs = path.isAbsolute(outArg) ? outArg : path.join(process.cwd(), outArg);

  if (!process.env.PLAYWRIGHT_HEADLESS) {
    process.env.PLAYWRIGHT_HEADLESS = '0';
  }

  console.log(
    'Откроется Chromium. Войдите в аккаунт Wildberries (и при необходимости откройте любой товар и проверьте цену с кошельком).'
  );
  console.log('Используйте тот же режим прокси, что у парсера (или без прокси — как в .env при сохранении).\n');

  const browser = await launchBrowser();
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
    rl.question('Нажмите Enter здесь после успешного входа на WB… ', () => {
      rl.close();
      resolve();
    });
  });

  await context.storageState({ path: outAbs });
  await browser.close();
  console.log('\nСохранено:', outAbs);
  console.log('В .env укажите (на VPS — абсолютный путь, chmod 600):');
  console.log(`PARSER_WB_STORAGE_STATE=${outAbs}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
