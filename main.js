/**
 * Точка входа: выбор площадки (Avito / Wildberries / обе подряд), ввод фильтров, Playwright, Excel.
 * Запуск: node main.js
 */

const readline = require('readline');
const { launchBrowser, newStealthContext, isProxyDisabled, PROXY_URL } = require('./browser');
const {
  log,
  logStep,
  logRetry,
  logBlock,
  randomDelay,
  randomInt,
  dedupeByHref,
  resultsPath,
  initSessionResultsOutput,
  waitEnterBeforeCloseBrowser,
} = require('./utils');
const {
  buildSearchUrl,
  detectBlock,
  hasAvitoEmptySerpMessage,
  looksLikeAvitoSkeletonNoItems,
  parseListings,
  filterListings,
  saveToExcel,
  buildSearchParamsExportRows,
} = require('./parser');
const { buildWbSearchParamsExportRows } = require('./parser_wb');
const { runAttemptWb } = require('./wb_runner');

/**
 * Верхняя граница запусков runAttempt за один вызов main (капча/IP, сеть, Excel и т.д.).
 * Ниже пересчитывается с учётом AVITO_IP_ROTATION_TRIES.
 */
const MAX_RUN_ATTEMPTS_MIN = 15;

/** После сбоя DOM на выдаче — следующий runAttempt откроет этот URL. */
let resumeListingUrl = null;
/** Номер страницы выдачи (1-based), соответствующий сохранённому URL. */
let resumeSerpPageIndex = null;

/** Лист «Фильтры запуска» в Excel: одна или две секции (Avito + WB). */
let sessionFilterExportRows = null;

/**
 * Номер страницы выдачи из query ?p= (у Avito вторая страница обычно p=2).
 * @param {string} urlStr
 * @returns {number}
 */
function parseSerpPageIndexFromUrl(urlStr) {
  try {
    const u = new URL(urlStr);
    const p = u.searchParams.get('p');
    if (p != null && p !== '') {
      const n = parseInt(p, 10);
      if (Number.isFinite(n) && n >= 1) return n;
    }
  } catch (_) {
    /* ignore */
  }
  return 1;
}

/**
 * При IP-блоке / капче запоминаем текущий URL выдачи и номер страницы — после ротации IP следующий runAttempt
 * откроет ту же вкладку (ту же позицию в поиске), а не первую страницу с нуля.
 * @param {{ domGateCtx: { page: import('playwright').Page | null }, crawlState: { serpPageIndex: number }, openUrl: string }} ctx
 */
function rememberAvitoListingForIpRetry(ctx) {
  const { domGateCtx, crawlState, openUrl } = ctx;
  let url = openUrl;
  try {
    const p = domGateCtx.page;
    if (p && typeof p.url === 'function') {
      const u = p.url();
      if (u && /avito\.ru/i.test(u) && !/^(chrome-error|about:|chrome:\/\/)/i.test(u)) {
        url = u;
      }
    }
  } catch (_) {
    /* keep openUrl */
  }
  if (!/avito\.ru/i.test(url)) {
    url = openUrl;
  }
  resumeListingUrl = url;
  resumeSerpPageIndex = crawlState.serpPageIndex;
  log(
    `  IP-блок / капча: сохранена позиция выдачи (стр. ${resumeSerpPageIndex}) — после смены IP откроем тот же адрес.`
  );
  log(`  ${resumeListingUrl}`);
}

/** Параметры filterListings из collectParams */
function listingsFilterOpts(params) {
  return {
    extraKeywords: params.extraKeywords,
    memory: params.memory,
    minPrice: params.minPrice,
    maxPrice: params.maxPrice,
    sellerType: params.sellerType,
    minRating: params.minRating,
    onlyToday: params.onlyToday === true,
    limit: 0,
  };
}

/** Повторы записи Excel, если файл открыт в Excel (Windows: EBUSY). */
const EXCEL_CHECKPOINT_RETRIES = 10;
const EXCEL_CHECKPOINT_RETRY_MS_MIN = 2000;
const EXCEL_CHECKPOINT_RETRY_MS_MAX = 3500;

/**
 * @param {unknown} e
 */
function isExcelFileBusyError(e) {
  const code = e && typeof e === 'object' && 'code' in e ? /** @type {{ code?: string }} */ (e).code : '';
  const msg = e && typeof e === 'object' && 'message' in e ? String(/** @type {{ message?: string }} */ (e).message) : String(e);
  return code === 'EBUSY' || /EBUSY|resource busy|locked/i.test(msg);
}

/**
 * Защита от потери данных: перезаписывает Excel накопленными строками после полной обработки
 * страницы выдачи, до клика «Далее» (при сбое на следующей странице в файле останутся предыдущие).
 * Не рвёт парсинг, если файл занят: ждёт и повторяет; в крайнем случае пишет предупреждение и продолжает.
 * @param {Awaited<ReturnType<typeof parseListings>>} raw
 * @param {Awaited<ReturnType<typeof collectParams>>} params
 * @param {string} checkpointLabel
 */
async function saveListingsCheckpoint(raw, params, checkpointLabel) {
  const filtered = filterListings(dedupeByHref(raw), listingsFilterOpts(params));
  const rowsForExcel = filtered.map((r) => ({ ...r, marketplace: 'Avito' }));
  const excelMeta =
    sessionFilterExportRows && sessionFilterExportRows.length > 0
      ? { checkpoint: checkpointLabel, filterExportRows: sessionFilterExportRows }
      : { checkpoint: checkpointLabel, searchParams: params };
  for (let i = 0; i < EXCEL_CHECKPOINT_RETRIES; i++) {
    try {
      saveToExcel(rowsForExcel, excelMeta);
      return;
    } catch (e) {
      if (!isExcelFileBusyError(e) || i === EXCEL_CHECKPOINT_RETRIES - 1) {
        if (isExcelFileBusyError(e)) {
          log(
            `  не удалось записать checkpoint (${checkpointLabel}): файл занят (закройте ${resultsPath()} в Excel). Парсинг продолжается без сохранения этого шага.`
          );
          return;
        }
        throw e;
      }
      log(
        `  Excel занят (попытка ${i + 1}/${EXCEL_CHECKPOINT_RETRIES}), повтор записи через паузу…`
      );
      await randomDelay(EXCEL_CHECKPOINT_RETRY_MS_MIN, EXCEL_CHECKPOINT_RETRY_MS_MAX);
    }
  }
}

/** Сколько пауз ротации после IP_BLOCK/капчи по умолчанию (если AVITO_IP_ROTATION_TRIES не задан). */
const DEFAULT_IP_ROTATION_WAITS = 5;

/**
 * Верхняя граница пауз ~2 мин при капче/IP на Wildberries за один запуск main.
 * Avito использует только AVITO_IP_ROTATION_TRIES; на WB лимит не больше этого числа (и не больше eff. Avito).
 */
const WB_IP_ROTATION_MAX_WAITS = 5;

/**
 * Сколько раз после IP_BLOCK ждать ротацию прокси (120–130 с) и снова запускать браузер.
 * По умолчанию 5 — чтобы пережить несколько блокировок подряд на мобильном прокси.
 * 0 в переменной — для IP_BLOCK минимум 1 пауза, если не задано AVITO_IP_BLOCK_NO_WAIT=1.
 * PowerShell: $env:AVITO_IP_ROTATION_TRIES=3 — ровно три паузы; без пауз: AVITO_IP_BLOCK_NO_WAIT=1
 */
function maxIpRotationWaits() {
  const raw = process.env.AVITO_IP_ROTATION_TRIES;
  if (raw === '0') return 0;
  if (raw == null || raw === '') return DEFAULT_IP_ROTATION_WAITS;
  const n = parseInt(raw, 10);
  return Number.isFinite(n) && n >= 0 ? n : DEFAULT_IP_ROTATION_WAITS;
}

/** Лимит пауз при IP_BLOCK с учётом минимум одной паузы под ротацию мобильного прокси. */
function effectiveIpRotationWaitLimit() {
  const raw = maxIpRotationWaits();
  const skip = process.env.AVITO_IP_BLOCK_NO_WAIT === '1' || process.env.AVITO_IP_BLOCK_NO_WAIT === 'true';
  if (skip) return raw;
  return Math.max(1, raw);
}

/** Лимит пауз ротации для цикла Wildberries: не выше WB_IP_ROTATION_MAX_WAITS и не выше настроек Avito. */
function effectiveWbIpRotationWaitLimit() {
  return Math.min(WB_IP_ROTATION_MAX_WAITS, effectiveIpRotationWaitLimit());
}

/** Стартовая диагностика: прокси и лимит пауз ротации (чтобы не гадать, почему нет 2 мин ожидания). */
function logProxyAndRotationSettings() {
  if (isProxyDisabled()) {
    log('  Прокси: ВЫКЛЮЧЕН (AVITO_NO_PROXY) — трафик идёт напрямую, не через mobile proxy в browser.js.');
  } else {
    try {
      const u = new URL(PROXY_URL);
      const port = u.port || (u.protocol === 'https:' ? '443' : '80');
      log(`  Прокси: ВКЛЮЧЕН — ${u.hostname}:${port} (HTTP-прокси из browser.js, с логином/паролем для мобильного выхода).`);
    } catch {
      log('  Прокси: ВКЛЮЧЕН (PROXY_URL в browser.js).');
    }
  }
  const rawEnv = process.env.AVITO_IP_ROTATION_TRIES;
  const effRaw = maxIpRotationWaits();
  const effIp = effectiveIpRotationWaitLimit();
  const shown =
    rawEnv === undefined || rawEnv === ''
      ? `не задано → по умолчанию ${DEFAULT_IP_ROTATION_WAITS}`
      : `"${rawEnv}"`;
  const skip = process.env.AVITO_IP_BLOCK_NO_WAIT === '1' || process.env.AVITO_IP_BLOCK_NO_WAIT === 'true';
  log(
    `  AVITO_IP_ROTATION_TRIES: ${shown} → пауз 120–130 с при IP_BLOCK: в конфиге ${effRaw}, фактически для повтора ${effIp}${skip ? ' (AVITO_IP_BLOCK_NO_WAIT — без принудительного минимума)' : ''}.`
  );
  if (effRaw === 0 && !skip) {
    log('  При 0 в TRIES после блокировки всё равно одна пауза ~2 мин под новый IP; полностью без ожидания: AVITO_IP_BLOCK_NO_WAIT=1');
  }
  const wbIpWaits = effectiveWbIpRotationWaitLimit();
  log(
    `  Wildberries: при капче/IP те же паузы ~120–130 с; пауз ротации не больше ${WB_IP_ROTATION_MAX_WAITS} (сейчас ${wbIpWaits}, если AVITO_IP_ROTATION_TRIES меньше — берётся меньшее).`
  );
  log(
    `  За один запуск main: до ${Math.max(MAX_RUN_ATTEMPTS_MIN, effectiveIpRotationWaitLimit() + 10)} попыток открыть Avito (с учётом капчи и прочих сбоев).`
  );
  log(
    `  За один запуск main: до ${Math.max(MAX_RUN_ATTEMPTS_MIN, wbIpWaits + 10)} попыток открыть Wildberries (с учётом капчи, DOM и прочих сбоев).`
  );
}

/** Пауза перед повтором после блокировки по IP (мобильный прокси ~2 мин ротация), мс */
const IP_ROTATION_WAIT_MIN_MS = 120_000;
const IP_ROTATION_WAIT_MAX_MS = 130_000;

/** Ожидание ответа сервера при переходе на Avito (мс); ждём событие load */
const NAV_TIMEOUT_MS = 180_000;

/** Ожидание появления карточек объявлений в DOM (мс) после последней перезагрузки */
const SELECTOR_TIMEOUT_MS = 120_000;

/** После навигации ждём событие load перед проверкой капчи и карточек (мс) */
const PAGE_LOAD_TIMEOUT_MS = 90_000;

/** Перезагрузки вкладки при «голом» DOM без закрытия браузера (капча не трогаем). */
const MAX_DOM_RELOAD_ATTEMPTS = 3;

/** До первой перезагрузки ждём карточки чуть меньше — быстрее пробуем reload. */
const CARD_WAIT_BEFORE_FIRST_RELOAD_MS = 50_000;

/** После каждой перезагрузки ждём дольше. */
const CARD_WAIT_AFTER_DOM_RELOAD_MS = 95_000;

const ITEM_SELECTOR = '[data-marker="item"]';

/**
 * После открытия страницы выдачи: load → капча/блок → карточки в DOM.
 * Один и тот же порядок для первой страницы и после каждого «Далее».
 * @param {import('playwright').Page} page
 * @param {string} pageLabel
 * @param {{ onIpBlock: () => never }} hooks
 * @param {{ isFirstPage?: boolean }} [options]
 * @param {{ onDomGateFailedAutoClose?: () => void }} [flow] — после исчерпания перезагрузок DOM: закрыть браузер без Enter (main запустит новый)
 */
async function gateListingPageReady(page, pageLabel, hooks, options = {}, flow) {
  const isFirstPage = options.isFirstPage === true;

  const markDomGateFailedAutoClose = () => {
    if (flow && typeof flow.onDomGateFailedAutoClose === 'function') {
      flow.onDomGateFailedAutoClose();
    }
  };

  if (isFirstPage) {
    logStep(
      6,
      'Загрузка страницы выдачи',
      `${pageLabel} — событие load, затем капча и карточки`
    );
  } else {
    log(`  [${pageLabel}] загрузка: load → капча / блок → карточки в DOM`);
  }

  await page.waitForLoadState('load', { timeout: PAGE_LOAD_TIMEOUT_MS }).catch((e) => {
    log(
      `  [${pageLabel}] load не за ${PAGE_LOAD_TIMEOUT_MS / 1000} с — продолжаем (${String(
        e.message
      ).slice(0, 96)})`
    );
  });

  await randomDelay(1800, 4200);

  if (isFirstPage) {
    logStep(7, 'Проверка на капчу / ограничение доступа', pageLabel);
  } else {
    log(`  [${pageLabel}] проверка: капча / ограничение доступа…`);
  }
  if (await detectBlock(page)) {
    logBlock(`[${pageLabel}] обнаружена капча или экран ограничения`);
    hooks.onIpBlock();
  }
  if (isFirstPage) {
    logStep(7, 'Ограничений в тексте не видно', 'продолжаем');
  }

  if (isFirstPage) {
    logStep(
      8,
      'Ожидание карточек объявлений в DOM',
      `селектор ${ITEM_SELECTOR}; при «голом» HTML — до ${MAX_DOM_RELOAD_ATTEMPTS} перезагрузок вкладки без закрытия браузера`
    );
  } else {
    log(
      `  [${pageLabel}] ожидание карточек; при сбое вёрстки — перезагрузка вкладки (до ${MAX_DOM_RELOAD_ATTEMPTS} раз)…`
    );
  }

  let gotCards = false;
  for (let domTry = 0; domTry <= MAX_DOM_RELOAD_ATTEMPTS; domTry++) {
    if (domTry > 0) {
      log(
        `  [${pageLabel}] перезагрузка страницы в той же сессии (${domTry}/${MAX_DOM_RELOAD_ATTEMPTS}) — восстановление DOM…`
      );
      try {
        const resp = await page.reload({ waitUntil: 'load', timeout: NAV_TIMEOUT_MS });
        if (resp) {
          const st = resp.status();
          if (st === 403 || st === 429) {
            log(`  ответ после перезагрузки: HTTP ${st} — ограничение по IP`);
            logNetFailureHelp(new Error(`HTTP ${st}`));
            hooks.onIpBlock();
          }
          if (st >= 400) {
            markDomGateFailedAutoClose();
            throw new Error(`Перезагрузка Avito: HTTP ${st}`);
          }
        }
      } catch (reloadErr) {
        const m = reloadErr && reloadErr.message ? reloadErr.message : String(reloadErr);
        if (m === 'IP_BLOCK') throw reloadErr;
        if (isLikelyProxyOrTunnelDrop(reloadErr)) {
          logNetFailureHelp(reloadErr);
          hooks.onIpBlock();
        }
        throw reloadErr;
      }
      await page.waitForLoadState('load', { timeout: PAGE_LOAD_TIMEOUT_MS }).catch(() => {});
      await randomDelay(2000, 4500);
      if (await detectBlock(page)) {
        logBlock(`[${pageLabel}] после перезагрузки — капча / блок`);
        hooks.onIpBlock();
      }
    }

    const waitMs = domTry === 0 ? CARD_WAIT_BEFORE_FIRST_RELOAD_MS : CARD_WAIT_AFTER_DOM_RELOAD_MS;
    try {
      await page.waitForSelector(ITEM_SELECTOR, { timeout: waitMs });
      gotCards = true;
      break;
    } catch {
      if (await detectBlock(page)) {
        logBlock(`[${pageLabel}] карточек нет — при проверке виден блок / капча`);
        hooks.onIpBlock();
      }
      if (await hasAvitoEmptySerpMessage(page)) {
        throw new Error(
          'Avito: пустая выдача по запросу (на странице сообщение о том, что ничего не найдено).'
        );
      }
      const skeleton = await looksLikeAvitoSkeletonNoItems(page);
      if (domTry >= MAX_DOM_RELOAD_ATTEMPTS) {
        log(
          `  [${pageLabel}] карточек нет после ${MAX_DOM_RELOAD_ATTEMPTS} перезагрузок вкладки`
        );
        markDomGateFailedAutoClose();
        throw new Error(
          'На странице не появились объявления за отведённое время. Проверьте прокси, запрос или вёрстку Avito.'
        );
      }
      if (skeleton) {
        log(`  [${pageLabel}] похоже на страницу без нормальной вёрстки выдачи (скелет HTML)`);
        continue;
      }
      if (domTry === 0) {
        log(`  [${pageLabel}] карточек нет — одна перезагрузка вкладки на случай медленной подгрузки JS…`);
        continue;
      }
      markDomGateFailedAutoClose();
      throw new Error(
        'На странице не появились объявления за отведённое время. Проверьте прокси, запрос или вёрстку Avito.'
      );
    }
  }

  if (!gotCards) {
    markDomGateFailedAutoClose();
    throw new Error(
      'На странице не появились объявления за отведённое время. Проверьте прокси, запрос или вёрстку Avito.'
    );
  }

  const cnt = await page.locator(ITEM_SELECTOR).count();
  if (isFirstPage) {
    logStep(8, 'Карточки объявлений появились', `в DOM: ${cnt} шт.`);
  } else {
    log(`  [${pageLabel}] карточки в DOM: ${cnt} шт.`);
  }
}

/** Максимум страниц выдачи (пагинация), защита от цикла. */
const MAX_SEARCH_PAGES = 80;

/** Скролл одной страницы: столько попыток, пока счётчик карточек не перестанет расти. */
const LIST_PAGE_SCROLL_MAX = 5;

/**
 * Мягкий «человеческий» темп: длиннее ждём, мельче крутим колёсико — меньше резких движений после входа.
 */
const SOFT = {
  beforeGotoMin: 600,
  beforeGotoMax: 2200,
  afterNavMin: 6500,
  afterNavMax: 13000,
  warmupScrollsMin: 5,
  warmupScrollsMax: 8,
  warmupWheelMin: 70,
  warmupWheelMax: 240,
  warmupPauseMin: 2800,
  warmupPauseMax: 6500,
  beforeCardsMin: 4000,
  beforeCardsMax: 9000,
  humanRoundsMin: 5,
  humanRoundsMax: 8,
  humanWheelMin: 90,
  humanWheelMax: 320,
  humanPauseMin: 3500,
  humanPauseMax: 8000,
  towardBottomStepsMin: 5,
  towardBottomStepsMax: 9,
  towardBottomPauseMin: 2500,
  towardBottomPauseMax: 5500,
  afterBottomMin: 4500,
  afterBottomMax: 9000,
  listWheelMin: 180,
  listWheelMax: 420,
  listPauseMin: 2400,
  listPauseMax: 5200,
  pageTurnMin: 3000,
  pageTurnMax: 6000,
};

/**
 * Клик по «следующей» странице выдачи (у Avito это пагинация, а не бесконечная лента).
 * @param {import('playwright').Page} page
 * @returns {Promise<boolean>}
 */
async function clickNextSearchPage(page) {
  const beforeUrl = page.url();
  let firstHref = '';
  try {
    firstHref = (await page.locator(`${ITEM_SELECTOR} a[href]`).first().getAttribute('href')) || '';
  } catch {
    firstHref = '';
  }

  const nextLocators = [
    page.locator('a[data-marker="pagination-button/next"]').first(),
    page.locator('[data-marker="pagination-button/next"]').first(),
    page.locator('a[rel="next"]').first(),
  ];

  let clicked = false;
  for (const loc of nextLocators) {
    const visible = await loc.isVisible().catch(() => false);
    if (!visible) continue;
    if ((await loc.getAttribute('aria-disabled').catch(() => null)) === 'true') continue;
    try {
      await loc.click({ timeout: 10_000 });
      clicked = true;
      break;
    } catch {
      // пробуем следующий селектор
    }
  }

  if (!clicked) {
    const byRole = page.getByRole('link', { name: /далее|следующ/i }).first();
    if (await byRole.isVisible().catch(() => false)) {
      try {
        await byRole.click({ timeout: 10_000 });
        clicked = true;
      } catch {
        clicked = false;
      }
    }
  }

  if (!clicked) return false;

  try {
    await page.waitForFunction(
      ({ prevUrl, prevHref }) => {
        if (window.location.href !== prevUrl) return true;
        const a = document.querySelector('[data-marker="item"] a[href]');
        const h = a ? a.getAttribute('href') || '' : '';
        return prevHref ? h !== prevHref : h.length > 0;
      },
      { prevUrl: beforeUrl, prevHref: firstHref },
      { timeout: 25_000 }
    );
  } catch {
    await randomDelay(3500, 7000);
  }

  await randomDelay(1500, 3500);
  return true;
}

/**
 * Сколько страниц в пагинации выдачи (если разметка Avito узнаваема). Иначе null — листаем «Далее» до упора.
 * @param {import('playwright').Page} page
 * @returns {Promise<number|null>}
 */
async function detectPaginationTotalPages(page) {
  const n = await page.evaluate(() => {
    let max = 0;
    const reMarker =
      /pagination-button\/page(?:-|:|\()?(\d+)(?:\))?/i;
    document.querySelectorAll('[data-marker]').forEach((el) => {
      const marker = el.getAttribute('data-marker') || '';
      let m = marker.match(reMarker);
      if (!m) m = marker.match(/pagination[^/]*\/page[^0-9]*(\d{1,3})/i);
      if (m) {
        const v = parseInt(m[1], 10);
        if (Number.isFinite(v) && v < 500) max = Math.max(max, v);
      }
    });
    document.querySelectorAll('a[href*="p="]').forEach((a) => {
      try {
        const u = new URL(/** @type {HTMLAnchorElement} */ (a).href, location.origin);
        const p = u.searchParams.get('p');
        if (p) {
          const v = parseInt(p, 10);
          if (Number.isFinite(v) && v > 0 && v < 500) max = Math.max(max, v);
        }
      } catch {
        /* ignore */
      }
    });
    document.querySelectorAll('[class*="pagination"] a, nav a').forEach((a) => {
      const t = (a.textContent || '').trim().replace(/\s+/g, '');
      if (/^\d{1,3}$/.test(t)) {
        const v = parseInt(t, 10);
        if (v > 0 && v < 500) max = Math.max(max, v);
      }
    });
    return max > 0 ? max : 0;
  });
  return n > 0 ? n : null;
}

/**
 * Если прокси требует отдельный вход в браузере: AVITO_MANUAL_PROXY=1, затем Enter после входа.
 * @returns {Promise<void>}
 */
function waitProxyManualGate() {
  const on = process.env.AVITO_MANUAL_PROXY === '1' || process.env.AVITO_MANUAL_PROXY === 'true';
  if (!on) return Promise.resolve();
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  return new Promise((resolve) => {
    log('');
    log('=== Ручной вход в прокси (AVITO_MANUAL_PROXY) ===');
    log('В окне Chromium: Ctrl+T → откройте страницу авторизации вашего прокси (как в инструкции провайдера).');
    log('Введите логин/пароль на сайте провайдера, дождитесь успешного доступа.');
    log('Затем вернитесь в эту консоль.');
    rl.question('Нажмите Enter здесь, когда вход в прокси выполнен… ', () => {
      rl.close();
      resolve();
    });
  });
}

/**
 * Пояснение при net::ERR_HTTP_RESPONSE_CODE_FAILURE и т.п.
 * @param {unknown} err
 */
function logNetFailureHelp(err) {
  const s = String(err && err.message ? err.message : err);
  if (!s.includes('ERR_') && !s.includes('net::') && !s.includes('HTTP')) return;
  log('  --- Что произошло ---');
  if (/ERR_CONNECTION_CLOSED|ERR_CONNECTION_RESET|ERR_EMPTY_RESPONSE/i.test(s)) {
    log('  Соединение с сайтом оборвалось до нормального HTTP-ответа (часто прокси рвёт туннель к HTTPS или сторона Avito сбрасывает TCP).');
    log('  Скрипт не меняет IP сам: у мобильного прокси новый адрес обычно после паузы ~2 мин или по правилам провайдера — уточните у них.');
  } else {
    log('  Chromium получил ошибочный HTTP-код или сетевой сбой через прокси.');
  }
  log('  --- Что проверить ---');
  log('  1) Тест без прокси: $env:AVITO_NO_PROXY=1; node main.js — если Avito откроется, проблема в прокси/его сессии.');
  log('  2) Веб-вход у прокси: $env:AVITO_MANUAL_PROXY=1; node main.js');
  log('  3) У поддержки прокси: смена IP по времени, лимиты на HTTPS CONNECT к avito.ru, не «залип» ли выходной IP.');
  log('  4) Логин в browser.js (буква O / цифра 0) и пароль актуальны.');
}

/**
 * Ошибки уровня TCP/туннеля — как IP_BLOCK: пауза под ротацию мобильного прокси и повтор.
 * @param {unknown} err
 * @returns {boolean}
 */
function isLikelyProxyOrTunnelDrop(err) {
  const s = String(err && err.message ? err.message : err);
  const u = s.toUpperCase();
  if (u.includes('ERR_CONNECTION_CLOSED')) return true;
  if (u.includes('ERR_CONNECTION_RESET')) return true;
  if (u.includes('ERR_EMPTY_RESPONSE')) return true;
  if (u.includes('ERR_CONNECTION_REFUSED')) return true;
  if (u.includes('ERR_SSL_PROTOCOL_ERROR')) return true;
  if (u.includes('ERR_TUNNEL_CONNECTION_FAILED')) return true;
  if (u.includes('ECONNRESET')) return true;
  if (u.includes('ECONNREFUSED')) return true;
  return false;
}

/**
 * @param {import('readline').Interface} rl
 * @param {string} q
 * @returns {Promise<string>}
 */
function ask(rl, q) {
  return new Promise((resolve) => {
    rl.question(q, (ans) => resolve(ans.trim()));
  });
}

/**
 * @returns {Promise<import('./parser').SearchParams & { memory: string, minRating: number, onlyToday: boolean, marketplace: 'avito'|'wb'|'both', color: string, wbRatingMode: 'any'|'with'|'without' }>}
 */
async function collectParams() {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

  try {
    const platRaw = await ask(
      rl,
      'Площадка: 1 — только Avito, 2 — только Wildberries, 3 — Avito и WB подряд (сначала Avito): '
    );
    const p0 = platRaw.trim().toLowerCase();
    let marketplace = 'avito';
    if (p0 === '2' || p0 === 'wb' || p0 === 'вб') marketplace = 'wb';
    else if (
      p0 === '3' ||
      p0 === 'both' ||
      /оба|обе|все|avito.*wb|wb.*avito|вместе/i.test(platRaw)
    ) {
      marketplace = 'both';
    }

    const query = await ask(rl, 'Поисковый запрос (например iPhone): ');
    const extraKeywords = await ask(rl, 'Доп. ключевые слова через пробел (например 17 Pro Max), можно пусто: ');

    let city = '';
    if (marketplace === 'avito' || marketplace === 'both') {
      city = await ask(rl, 'Город Avito (например Москва или moskva): ');
    }

    const minPriceStr = await ask(rl, 'Цена от, руб. (0 = не важно; для WB также в ссылке priceU): ');
    const maxPriceStr = await ask(rl, 'Цена до, руб. (0 = не важно): ');
    const memory = await ask(rl, 'Память, ГБ (например 256), пусто = любая: ');

    let sellerType = 'any';
    let minRating = 0;
    let onlyToday = false;
    if (marketplace === 'avito' || marketplace === 'both') {
      const sellerRaw = await ask(rl, 'Avito — тип продавца: private | company | any: ');
      sellerType = (sellerRaw || 'any').toLowerCase();
      if (!['private', 'company', 'any'].includes(sellerType)) sellerType = 'any';
      const minRatingStr = await ask(rl, 'Avito — мин. рейтинг продавца (0-5, 0 = не важно): ');
      minRating = parseFloat(minRatingStr.replace(',', '.')) || 0;
      const onlyTodayRaw = await ask(
        rl,
        'Avito — только «за сегодня» по подписи на карточке? да / нет: '
      );
      const ot = (onlyTodayRaw || '').trim().toLowerCase();
      onlyToday =
        ot === 'да' || ot === 'д' || ot === '+' || ot === 'yes' || ot === 'y' || ot === '1' || ot === 'true';
    }

    let color = '';
    /** @type {'any'|'with'|'without'} */
    let wbRatingMode = 'any';
    if (marketplace === 'wb' || marketplace === 'both') {
      color = await ask(rl, 'Wildberries — цвет в названии (подстрока, пусто = любой): ');
      const wr = await ask(
        rl,
        'Wildberries — оценки на карточке: 1 — не важно, 2 — только с рейтингом, 3 — только без рейтинга: '
      );
      const w = (wr || '').trim().toLowerCase();
      if (w === '2' || /только\s*с|с\s*рейтинг|есть\s*оцен/i.test(w)) wbRatingMode = 'with';
      else if (w === '3' || /без\s*рейтинг|нет\s*рейтинг|без\s*оцен/i.test(w)) wbRatingMode = 'without';
    }

    const minPrice = parseInt(minPriceStr, 10) || 0;
    const maxPrice = parseInt(maxPriceStr, 10) || 0;

    return {
      marketplace,
      query,
      extraKeywords,
      city,
      minPrice,
      maxPrice,
      memory,
      sellerType,
      minRating,
      onlyToday,
      color,
      wbRatingMode,
    };
  } finally {
    rl.close();
  }
}

/**
 * Строки для листа фильтров: одна площадка или Avito+WB.
 * @param {{ marketplace: string }} params
 */
function buildSessionFilterExportRows(params) {
  if (params.marketplace === 'avito') return buildSearchParamsExportRows(params);
  if (params.marketplace === 'wb') return buildWbSearchParamsExportRows(params);
  return [
    ...buildSearchParamsExportRows(params),
    { Параметр: '—', Значение: '—' },
    ...buildWbSearchParamsExportRows(params),
  ];
}

/**
 * После открытия страницы: неспешно смотрим верх, без резких скачков.
 * @param {import('playwright').Page} page
 */
async function gentleWarmupScroll(page) {
  const n = randomInt(SOFT.warmupScrollsMin, SOFT.warmupScrollsMax);
  log(`  мягкое просматривание страницы (${n} коротких прокруток, с паузами)`);
  for (let i = 0; i < n; i++) {
    await page.mouse.wheel(0, randomInt(SOFT.warmupWheelMin, SOFT.warmupWheelMax));
    await randomDelay(SOFT.warmupPauseMin, SOFT.warmupPauseMax);
    if (randomInt(0, 3) === 0) {
      await randomDelay(800, 2200);
    }
  }
}

/**
 * Скролл и паузы «по-человечески»: мелкие шаги вниз и длинные паузы.
 * @param {import('playwright').Page} page
 */
async function humanBehavior(page) {
  const rounds = randomInt(SOFT.humanRoundsMin, SOFT.humanRoundsMax);
  log(`  имитация чтения ленты: ${rounds} мелких прокруток с длинными паузами`);
  for (let i = 0; i < rounds; i++) {
    await page.mouse.wheel(0, randomInt(SOFT.humanWheelMin, SOFT.humanWheelMax));
    await randomDelay(SOFT.humanPauseMin, SOFT.humanPauseMax);
  }
  const steps = randomInt(SOFT.towardBottomStepsMin, SOFT.towardBottomStepsMax);
  log(`  поэтапный спуск к низу страницы (${steps} шагов)`);
  for (let s = 0; s < steps; s++) {
    await page.evaluate(() => {
      const h = document.body?.scrollHeight ?? 0;
      const y = window.scrollY;
      const delta = Math.min(380, Math.max(120, (h - y) / 5));
      window.scrollBy(0, delta);
    });
    await randomDelay(SOFT.towardBottomPauseMin, SOFT.towardBottomPauseMax);
  }
  await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
  await randomDelay(SOFT.afterBottomMin, SOFT.afterBottomMax);
  log('  имитация прокрутки завершена');
}

/**
 * Скролл текущей страницы: догружаем карточки, если лента подмешивает их скроллом; иначе выходим (дальше — пагинация).
 * @param {import('playwright').Page} page
 */
async function scrollForMoreListings(page) {
  const maxIter = LIST_PAGE_SCROLL_MAX;
  log(
    `  догрузка карточек скроллом: до ${maxIter} прокруток (ещё объявления — на других страницах выдачи)`
  );
  let last = await page.locator(ITEM_SELECTOR).count().catch(() => 0);
  let stableRounds = 0;
  for (let i = 0; i < maxIter; i++) {
    const n = await page.locator(ITEM_SELECTOR).count().catch(() => 0);
    log(`  прокрутка ${i + 1}/${maxIter}: карточек в DOM — ${n}`);
    if (n === last) {
      stableRounds += 1;
      if (stableRounds >= 2) {
        log('  число карточек не меняется — хватает скролла для этой страницы');
        break;
      }
    } else {
      stableRounds = 0;
      last = n;
    }
    await page.mouse.wheel(0, randomInt(SOFT.listWheelMin, SOFT.listWheelMax));
    await randomDelay(SOFT.listPauseMin, SOFT.listPauseMax);
  }
  const finalN = await page.locator(ITEM_SELECTOR).count().catch(() => 0);
  log(`  итого карточек на странице в DOM — ${finalN}`);
}

/**
 * @param {Awaited<ReturnType<typeof collectParams>>} params
 * @returns {Promise<Array<{ title: string, priceText: string, href: string, city: string }>>}
 */
async function runAttempt(params) {
  /** При IP_BLOCK закрываем Chromium сразу, без Enter — дальше пауза на ротацию прокси */
  let skipEnterBeforeClose = false;

  const domGateCtx = { page: /** @type {import('playwright').Page | null} */ (null) };

  const builtSearchUrl = buildSearchUrl({
    query: params.query,
    extraKeywords: params.extraKeywords,
    city: params.city,
    minPrice: params.minPrice,
    maxPrice: params.maxPrice,
    sellerType: params.sellerType,
  });

  const openingFromResume = resumeListingUrl != null;
  const openUrl = openingFromResume ? resumeListingUrl : builtSearchUrl;
  /** @type {number|null} */
  let savedResumePageIdx = null;
  if (openingFromResume) {
    savedResumePageIdx = resumeSerpPageIndex;
    resumeListingUrl = null;
    resumeSerpPageIndex = null;
    log(
      `  Возобновление: открываем сохранённый URL страницы выдачи${savedResumePageIdx != null ? ` (стр. ${savedResumePageIdx})` : ''}.`
    );
  }

  const crawlState = {
    serpPageIndex: savedResumePageIdx != null ? savedResumePageIdx : 1,
  };

  logStep(
    1,
    'Запуск браузера',
    isProxyDisabled() ? 'Chromium без прокси (AVITO_NO_PROXY)' : 'Chromium + прокси из browser.js'
  );
  const browser = await launchBrowser();

  logStep(2, 'Создание контекста', 'stealth, ru-RU, случайный viewport и user-agent');
  const context = await newStealthContext(browser);

  logStep(3, 'Открыта новая вкладка', '');
  const page = await context.newPage();
  domGateCtx.page = page;

  await waitProxyManualGate();

  const ipHooks = {
    onIpBlock() {
      skipEnterBeforeClose = true;
      rememberAvitoListingForIpRetry({ domGateCtx, crawlState, openUrl });
      throw new Error('IP_BLOCK');
    },
  };

  const domGateFlow = {
    onDomGateFailedAutoClose() {
      skipEnterBeforeClose = true;
      try {
        const p = domGateCtx.page;
        if (p) {
          resumeListingUrl = p.url();
          resumeSerpPageIndex = crawlState.serpPageIndex;
          log(
            `  Запомнили для следующей попытки: стр. ${resumeSerpPageIndex} — ${resumeListingUrl}`
          );
        }
      } catch (_) {
        /* ignore */
      }
      log(
        '  Выдача не поднялась после перезагрузок вкладки — закрываем Chromium без паузы Enter; следующая попытка откроет этот же адрес выдачи.'
      );
    },
  };

  try {
    log('  мягкий режим: длинные паузы и мелкие скроллы, чтобы вход на страницу выглядел спокойнее');
    log(
      `  сценарий: переход → ожидание и просмотр → парсинг стр.1 → скролл и страницы выдачи (лимит ${MAX_SEARCH_PAGES})`
    );
    log('  короткая пауза перед переходом по ссылке…');
    await randomDelay(SOFT.beforeGotoMin, SOFT.beforeGotoMax);

    logStep(4, 'Адрес поиска Avito', openUrl);
    logStep(
      5,
      'Переход на Avito',
      `ожидание load (полнее, чем domcontentloaded), таймаут ${NAV_TIMEOUT_MS / 1000} с`
    );

    try {
      const response = await page.goto(openUrl, { waitUntil: 'load', timeout: NAV_TIMEOUT_MS });
      if (response) {
        const st = response.status();
        if (st === 403 || st === 429) {
          log(`  ответ сервера: HTTP ${st} — ограничение по IP / частота запросов`);
          logNetFailureHelp(new Error(`HTTP ${st}`));
          skipEnterBeforeClose = true;
          rememberAvitoListingForIpRetry({ domGateCtx, crawlState, openUrl });
          throw new Error('IP_BLOCK');
        }
        if (st >= 400) {
          log(`  ответ сервера: HTTP ${st}`);
          logNetFailureHelp(new Error(`HTTP ${st}`));
          throw new Error(`Открытие Avito: HTTP ${st}`);
        }
      }
    } catch (navErr) {
      const m = navErr && navErr.message ? navErr.message : String(navErr);
      if (m === 'IP_BLOCK') throw navErr;
      if (isLikelyProxyOrTunnelDrop(navErr)) {
        log(`  навигация: ${m}`);
        logNetFailureHelp(navErr);
        log('  Считаем это сбоем канала через прокси → пауза ротации (как при IP_BLOCK), затем повтор при лимите AVITO_IP_ROTATION_TRIES.');
        skipEnterBeforeClose = true;
        rememberAvitoListingForIpRetry({ domGateCtx, crawlState, openUrl });
        throw new Error('IP_BLOCK');
      }
      logNetFailureHelp(navErr);
      throw navErr;
    }

    log('  после перехода: load, капча и карточки — единая проверка (шаги 6–8 в логе)');
    await gateListingPageReady(
      page,
      openingFromResume ? 'возобновление сохранённой выдачи' : 'страница выдачи 1',
      ipHooks,
      { isFirstPage: true },
      domGateFlow
    );

    let listingPageIdx =
      savedResumePageIdx != null
        ? savedResumePageIdx
        : openingFromResume
          ? parseSerpPageIndexFromUrl(page.url())
          : 1;
    if (!openingFromResume) {
      listingPageIdx = 1;
    } else {
      log(`  Продолжаем с страницы выдачи: ${listingPageIdx}`);
    }
    crawlState.serpPageIndex = listingPageIdx;

    log('  короткая пауза перед сбором данных со страницы…');
    await randomDelay(2500, 5500);

    /** @type {Awaited<ReturnType<typeof parseListings>>} */
    let raw = [];

    logStep(9, `Парсинг страницы выдачи ${listingPageIdx}`, 'первый проход');
    let batch = await parseListings(page);
    raw.push(...batch);
    log(`  собрано записей: ${batch.length}`);

    logStep(10, `Скролл страницы выдачи ${listingPageIdx}`, 'догрузка карточек и просмотр пагинации внизу');
    await humanBehavior(page);
    await scrollForMoreListings(page);
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await randomDelay(2500, 5000);

    const totalPages = await detectPaginationTotalPages(page);
    if (totalPages != null) {
      log(`  по блоку пагинации: всего страниц выдачи — ${totalPages}`);
    } else {
      log('  число страниц по вёрстке не распознано — перейдём по «Далее» до конца выдачи');
    }

    logStep(11, 'Повторная проверка на блок после скролла', '');
    if (await detectBlock(page)) {
      logBlock('после прокрутки');
      skipEnterBeforeClose = true;
      rememberAvitoListingForIpRetry({ domGateCtx, crawlState, openUrl });
      throw new Error('IP_BLOCK');
    }
    log('  блокировка не обнаружена');

    logStep(
      12,
      `Парсинг страницы выдачи ${listingPageIdx}`,
      'второй проход после скролла (объединяем с первым)'
    );
    batch = await parseListings(page);
    raw.push(...batch);
    raw = dedupeByHref(raw);
    log(`  после объединения уникальных на странице ${listingPageIdx}: ${raw.length}`);

    await saveListingsCheckpoint(
      raw,
      params,
      `страница выдачи ${listingPageIdx} (перед переходом на следующую)`
    );
    while (listingPageIdx < MAX_SEARCH_PAGES) {
      if (totalPages != null && listingPageIdx >= totalPages) {
        log(`  достигнута страница ${totalPages} по пагинации — дальше не переходим`);
        break;
      }
      const moved = await clickNextSearchPage(page);
      if (!moved) {
        log(`  следующей страницы нет — обработано страниц выдачи: ${listingPageIdx}`);
        break;
      }
      listingPageIdx += 1;
      crawlState.serpPageIndex = listingPageIdx;
      log(
        `  открыта страница выдачи ${listingPageIdx}${totalPages != null ? ` из ${totalPages}` : ''}`
      );

      await gateListingPageReady(
        page,
        `страница выдачи ${listingPageIdx}`,
        ipHooks,
        { isFirstPage: false },
        domGateFlow
      );

      await page.evaluate(() => window.scrollTo(0, 0));
      await randomDelay(2000, 4500);
      await gentleWarmupScroll(page);
      await scrollForMoreListings(page);
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await randomDelay(2500, 5000);

      logStep(
        12,
        'Парсинг страницы выдачи',
        `№ ${listingPageIdx}${totalPages != null ? ` из ${totalPages}` : ''}`
      );
      batch = await parseListings(page);
      raw.push(...batch);
      raw = dedupeByHref(raw);
      log(`  на странице ${listingPageIdx}: ${batch.length} записей, всего уникальных: ${raw.length}`);

      await saveListingsCheckpoint(
        raw,
        params,
        `страница выдачи ${listingPageIdx} (перед переходом на следующую)`
      );
    }

    log(`  сырых уникальных записей после обхода всех страниц: ${raw.length}`);

    raw = dedupeByHref(raw);
    log(`  после удаления дубликатов по ссылке: ${raw.length}`);

    const filtered = filterListings(raw, listingsFilterOpts(params));
    logStep(13, 'Фильтрация по вашим условиям', `найдено подходящих: ${filtered.length}`);
    logStep(
      14,
      'Итог',
      `${resultsPath()} — ${filtered.length} подходящих (Excel обновлялся после каждой обработанной страницы выдачи)`
    );

    resumeListingUrl = null;
    resumeSerpPageIndex = null;
    return filtered;
  } finally {
    if (!skipEnterBeforeClose) {
      logStep(15, 'Закрытие браузера', 'пауза только если AVITO_WAIT_ENTER=1');
      await waitEnterBeforeCloseBrowser('Avito');
    } else {
      log(
        '  Закрываем браузер без ожидания Enter (блок IP / сбой DOM / следующая попытка в main).'
      );
    }
    log('  Закрытие Chromium…');
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
  }
}

/**
 * Повторы при IP_BLOCK и прочих сбоях (отдельный счётчик для каждого вызова).
 * @template T
 * @param {string} label
 * @param {() => Promise<T>} fn
 * @param {{ ipWaitLimit?: number }} [options] — лимит пауз ~2 мин при IP_BLOCK (по умолчанию effectiveIpRotationWaitLimit())
 * @returns {Promise<T>}
 */
async function runIpRetryLoop(label, fn, options = {}) {
  let lastErr = null;
  let ipRotationWaitsDone = 0;
  const ipWaitLimitRaw = maxIpRotationWaits();
  const skipNoWait =
    process.env.AVITO_IP_BLOCK_NO_WAIT === '1' || process.env.AVITO_IP_BLOCK_NO_WAIT === 'true';
  const ipWaitLimit =
    options != null && options.ipWaitLimit != null && Number.isFinite(options.ipWaitLimit)
      ? Math.max(0, Math.floor(options.ipWaitLimit))
      : effectiveIpRotationWaitLimit();
  const maxRunAttempts = Math.max(MAX_RUN_ATTEMPTS_MIN, ipWaitLimit + 10);

  for (let attempt = 1; attempt <= maxRunAttempts; attempt++) {
    try {
      if (attempt > 1) {
        logRetry(`${label}: попытка ${attempt} из ${maxRunAttempts}`);
      }
      return await fn();
    } catch (e) {
      lastErr = e;
      const msg = e && e.message ? e.message : String(e);

      if (msg === 'IP_BLOCK') {
        if (ipRotationWaitsDone >= ipWaitLimit) {
          logBlock(
            ipWaitLimitRaw === 0 && skipNoWait
              ? `${label}: ограничение по IP — пауза ротации отключена (AVITO_IP_BLOCK_NO_WAIT).`
              : `${label}: ограничение по IP — исчерпаны паузы ротации (${ipRotationWaitsDone}/${ipWaitLimit}).`
          );
          break;
        }
        ipRotationWaitsDone += 1;
        const waitMs = randomInt(IP_ROTATION_WAIT_MIN_MS, IP_ROTATION_WAIT_MAX_MS);
        logBlock(
          `${label}: ограничение по IP / капча — пауза ~${Math.round(waitMs / 1000)} с (${ipRotationWaitsDone}/${ipWaitLimit}), затем повтор…`
        );
        await randomDelay(waitMs, waitMs);
        continue;
      }

      logRetry(`${label}: ${msg} (пауза перед следующей попыткой)`);
      await randomDelay(3000, 6000);
    }
  }

  console.error(
    `${label}: не удалось завершить за ${maxRunAttempts} попыток. Последняя ошибка:`,
    lastErr
  );
  process.exit(1);
}

async function main() {
  log('СТАРТ — парсер Avito / Wildberries (Playwright, Excel)');
  log('  По завершении парсинга браузер закрывается сам. Чтобы ждать Enter в консоли: AVITO_WAIT_ENTER=1.');
  logProxyAndRotationSettings();

  let params;
  try {
    params = await collectParams();
  } catch (e) {
    console.error(e);
    process.exit(1);
    return;
  }

  if (!params.query) {
    console.error('Ошибка: пустой поисковый запрос.');
    process.exit(1);
    return;
  }

  initSessionResultsOutput();
  sessionFilterExportRows = buildSessionFilterExportRows(params);
  log(
    `  Файл результатов (новый на этот запуск; время в имени — пояс Самары/Саратова, UTC+4): ${resultsPath()}`
  );

  /** @type {Array<object>} */
  let priorForWb = [];

  if (params.marketplace === 'avito' || params.marketplace === 'both') {
    const avitoRows = await runIpRetryLoop('Avito', () => runAttempt(params));
    if (params.marketplace === 'both') {
      priorForWb = avitoRows.map((r) => ({ ...r, marketplace: 'Avito' }));
    }
  }

  if (params.marketplace === 'wb' || params.marketplace === 'both') {
    await runIpRetryLoop(
      'Wildberries',
      () =>
        runAttemptWb(params, {
          priorExcelRows: priorForWb,
          filterExportRows: sessionFilterExportRows,
        }),
      { ipWaitLimit: effectiveWbIpRotationWaitLimit() }
    );
  }
}

main();
