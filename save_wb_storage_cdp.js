/**
 * Альтернатива save_wb_storage_state.js: без запуска браузера Playwright.
 * Вы сами открываете обычный Chrome, заходите на WB — скрипт только читает сессию.
 *
 * Шаги (Windows):
 *
 * 1) Закройте все окна Chrome.
 *
 * 2) Запустите Chrome отдельно (закройте другие окна Chrome):
 *
 *    cmd.exe:
 *    "%ProgramFiles%\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --user-data-dir="%TEMP%\wb_ifind_profile"
 *
 *    PowerShell:
 *    & "$env:ProgramFiles\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --user-data-dir="$env:TEMP\wb_ifind_profile"
 *
 * 3) В открывшемся Chrome зайдите на https://www.wildberries.ru/ , войдите в аккаунт,
 *    откройте товар и убедитесь, что видна цена с WB Кошельком.
 *
 * 4) В другом окне терминала (в папке проекта):
 *
 *    node save_wb_storage_cdp.js wb_storage.json
 *
 *    (если порт другой: set WB_CDP_URL=http://127.0.0.1:9223 и тот же порт в шаге 2)
 *
 * 5) Закройте Chrome. Файл wb_storage.json положите на VPS и в .env:
 *    PARSER_WB_STORAGE_STATE=/полный/путь/wb_storage.json
 *
 * Edge вместо Chrome:
 *    "%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe" --remote-debugging-port=9222 --user-data-dir="%TEMP%\wb_ifind_profile"
 */

const path = require('path');
const { chromium } = require('playwright');

const CDP_URL = (process.env.WB_CDP_URL || 'http://127.0.0.1:9222').trim();

async function main() {
  const outArg = process.argv[2] || 'wb_storage.json';
  const outAbs = path.isAbsolute(outArg) ? outArg : path.join(process.cwd(), outArg);

  console.log('Подключение к браузеру:', CDP_URL);
  console.log('Должен быть запущен Chrome/Edge с --remote-debugging-port и вы уже вошли на WB.\n');

  let browser;
  try {
    browser = await chromium.connectOverCDP(CDP_URL);
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    console.error('Ошибка подключения:', msg);
    console.error('\nЗапустите Chrome так (cmd.exe):');
    console.error(
      '  "%ProgramFiles%\\Google\\Chrome\\Application\\chrome.exe" --remote-debugging-port=9222 --user-data-dir="%TEMP%\\wb_ifind_profile"'
    );
    process.exit(1);
  }

  try {
    const contexts = browser.contexts();
    if (!contexts.length) {
      console.error('Нет контекста браузера — откройте хотя бы одну вкладку в Chrome.');
      process.exit(1);
    }
    await contexts[0].storageState({ path: outAbs });
    console.log('\nГотово:', path.resolve(outAbs));
    console.log('На VPS в .env: PARSER_WB_STORAGE_STATE=' + outAbs.replace(/\\/g, '/'));
  } finally {
    try {
      await browser.close();
    } catch (_) {
      /* ignore */
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
