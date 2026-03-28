/**
 * Сценарий Playwright для Wildberries: та же идея, что runAttempt в main (гейт DOM, скролл, пагинация, Excel).
 */

const readline = require('readline');
const { launchBrowser, newStealthContext, isProxyDisabled, PROXY_URL } = require('./browser');
const {
  log,
  logStep,
  randomDelay,
  randomInt,
  dedupeByHref,
  resultsPath,
} = require('./utils');
const { detectBlock, saveToExcel } = require('./parser');
const {
  buildWbSearchUrl,
  parseWbSerpPageIndexFromUrl,
  hasWbEmptySerpMessage,
  looksLikeWbSkeletonNoItems,
  parseWbListings,
  filterWbListings,
  wbListingsFilterOpts,
} = require('./parser_wb');

let resumeListingUrlWb = null;
let resumeSerpPageIndexWb = null;

const NAV_TIMEOUT_MS = 180_000;
const SELECTOR_TIMEOUT_MS = 120_000;
const PAGE_LOAD_TIMEOUT_MS = 90_000;
const MAX_DOM_RELOAD_ATTEMPTS = 3;
const CARD_WAIT_BEFORE_FIRST_RELOAD_MS = 50_000;
const CARD_WAIT_AFTER_DOM_RELOAD_MS = 95_000;
const MAX_SEARCH_PAGES_WB = 80;
const LIST_PAGE_SCROLL_MAX = 5;

/** Карточки товара на выдаче WB (несколько вариантов вёрстки). */
const WB_ITEM_SELECTOR =
  'article.product-card__wrapper, article[data-nm-id], article.product-card';

const SOFT = {
  beforeGotoMin: 600,
  beforeGotoMax: 2200,
  warmupScrollsMin: 5,
  warmupScrollsMax: 8,
  warmupWheelMin: 70,
  warmupWheelMax: 240,
  warmupPauseMin: 2800,
  warmupPauseMax: 6500,
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
};

function waitProxyManualGate() {
  const on = process.env.AVITO_MANUAL_PROXY === '1' || process.env.AVITO_MANUAL_PROXY === 'true';
  if (!on) return Promise.resolve();
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  return new Promise((resolve) => {
    log('');
    log('=== Ручной вход в прокси (AVITO_MANUAL_PROXY) ===');
    rl.question('Нажмите Enter здесь, когда вход в прокси выполнен… ', () => {
      rl.close();
      resolve();
    });
  });
}

function waitEnterBeforeCloseBrowser() {
  if (process.env.AVITO_AUTO_CLOSE === '1' || process.env.AVITO_AUTO_CLOSE === 'true') {
    return Promise.resolve();
  }
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  return new Promise((resolve) => {
    rl.question(
      '>>> Окно браузера остаётся открытым (WB). Нажмите Enter здесь, чтобы закрыть Chromium… ',
      () => {
        rl.close();
        resolve();
      }
    );
  });
}

function logNetFailureHelp(err) {
  const s = String(err && err.message ? err.message : err);
  if (!s.includes('ERR_') && !s.includes('net::') && !s.includes('HTTP')) return;
  log('  --- Сеть / прокси (WB) ---');
  log(`  ${s.slice(0, 200)}`);
}

function isLikelyProxyOrTunnelDrop(err) {
  const s = String(err && err.message ? err.message : err);
  const u = s.toUpperCase();
  return (
    u.includes('ERR_CONNECTION_CLOSED') ||
    u.includes('ERR_CONNECTION_RESET') ||
    u.includes('ERR_EMPTY_RESPONSE') ||
    u.includes('ERR_TUNNEL_CONNECTION_FAILED') ||
    u.includes('ECONNRESET')
  );
}

/**
 * @param {import('playwright').Page} page
 * @param {string} pageLabel
 * @param {{ onIpBlock: () => never }} hooks
 * @param {{ isFirstPage?: boolean }} [options]
 * @param {{ onDomGateFailedAutoClose?: () => void }} [flow]
 */
async function gateWbListingPageReady(page, pageLabel, hooks, options = {}, flow) {
  const isFirstPage = options.isFirstPage === true;

  const markDomGateFailedAutoClose = () => {
    if (flow && typeof flow.onDomGateFailedAutoClose === 'function') flow.onDomGateFailedAutoClose();
  };

  if (isFirstPage) {
    logStep(6, 'Загрузка выдачи Wildberries', `${pageLabel} — load, затем проверка блокировки и карточек`);
  } else {
    log(`  [${pageLabel}] WB: load → блок → карточки`);
  }

  await page.waitForLoadState('load', { timeout: PAGE_LOAD_TIMEOUT_MS }).catch((e) => {
    log(`  [${pageLabel}] load не за таймаут — продолжаем (${String(e.message).slice(0, 80)})`);
  });
  await randomDelay(1800, 4200);

  if (await detectBlock(page)) {
    log(`  [${pageLabel}] похоже на капчу / ограничение`);
    hooks.onIpBlock();
  }

  let gotCards = false;
  for (let domTry = 0; domTry <= MAX_DOM_RELOAD_ATTEMPTS; domTry++) {
    if (domTry > 0) {
      log(`  [${pageLabel}] перезагрузка вкладки WB (${domTry}/${MAX_DOM_RELOAD_ATTEMPTS})…`);
      try {
        const resp = await page.reload({ waitUntil: 'load', timeout: NAV_TIMEOUT_MS });
        if (resp) {
          const st = resp.status();
          if (st === 403 || st === 429) {
            logNetFailureHelp(new Error(`HTTP ${st}`));
            hooks.onIpBlock();
          }
          if (st >= 400) {
            markDomGateFailedAutoClose();
            throw new Error(`Перезагрузка WB: HTTP ${st}`);
          }
        }
      } catch (reloadErr) {
        const m = reloadErr && reloadErr.message ? reloadErr.message : String(reloadErr);
        if (m === 'IP_BLOCK') throw reloadErr;
        if (isLikelyProxyOrTunnelDrop(reloadErr)) hooks.onIpBlock();
        throw reloadErr;
      }
      await page.waitForLoadState('load', { timeout: PAGE_LOAD_TIMEOUT_MS }).catch(() => {});
      await randomDelay(2000, 4500);
      if (await detectBlock(page)) hooks.onIpBlock();
    }

    const waitMs = domTry === 0 ? CARD_WAIT_BEFORE_FIRST_RELOAD_MS : CARD_WAIT_AFTER_DOM_RELOAD_MS;
    try {
      await page.waitForSelector(WB_ITEM_SELECTOR, { timeout: waitMs });
      gotCards = true;
      break;
    } catch {
      if (await detectBlock(page)) hooks.onIpBlock();
      if (await hasWbEmptySerpMessage(page)) {
        throw new Error('Wildberries: пустая выдача по запросу.');
      }
      const skeleton = await looksLikeWbSkeletonNoItems(page);
      if (domTry >= MAX_DOM_RELOAD_ATTEMPTS) {
        markDomGateFailedAutoClose();
        throw new Error(
          'На странице WB не появились карточки товаров. Проверьте прокси или вёрстку сайта.'
        );
      }
      if (skeleton) continue;
      if (domTry === 0) continue;
      markDomGateFailedAutoClose();
      throw new Error('На странице WB не появились карточки товаров за отведённое время.');
    }
  }

  if (!gotCards) {
    markDomGateFailedAutoClose();
    throw new Error('На странице WB не появились карточки товаров.');
  }

  const cnt = await page.locator(WB_ITEM_SELECTOR).count();
  log(`  [${pageLabel}] карточек WB в DOM: ${cnt}`);
}

async function gentleWarmupScroll(page) {
  const n = randomInt(SOFT.warmupScrollsMin, SOFT.warmupScrollsMax);
  log(`  мягкое просматривание страницы WB (${n} прокруток)`);
  for (let i = 0; i < n; i++) {
    await page.mouse.wheel(0, randomInt(SOFT.warmupWheelMin, SOFT.warmupWheelMax));
    await randomDelay(SOFT.warmupPauseMin, SOFT.warmupPauseMax);
  }
}

async function humanBehavior(page) {
  const rounds = randomInt(SOFT.humanRoundsMin, SOFT.humanRoundsMax);
  for (let i = 0; i < rounds; i++) {
    await page.mouse.wheel(0, randomInt(SOFT.humanWheelMin, SOFT.humanWheelMax));
    await randomDelay(SOFT.humanPauseMin, SOFT.humanPauseMax);
  }
  const steps = randomInt(SOFT.towardBottomStepsMin, SOFT.towardBottomStepsMax);
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
}

async function scrollForMoreWbListings(page) {
  let last = await page.locator(WB_ITEM_SELECTOR).count().catch(() => 0);
  let stableRounds = 0;
  for (let i = 0; i < LIST_PAGE_SCROLL_MAX; i++) {
    const n = await page.locator(WB_ITEM_SELECTOR).count().catch(() => 0);
    log(`  WB скролл ${i + 1}/${LIST_PAGE_SCROLL_MAX}: карточек — ${n}`);
    if (n === last) {
      stableRounds += 1;
      if (stableRounds >= 2) break;
    } else {
      stableRounds = 0;
      last = n;
    }
    await page.mouse.wheel(0, randomInt(SOFT.listWheelMin, SOFT.listWheelMax));
    await randomDelay(SOFT.listPauseMin, SOFT.listPauseMax);
  }
}

/**
 * @param {import('playwright').Page} page
 * @returns {Promise<boolean>}
 */
async function clickNextWbPage(page) {
  const beforeUrl = page.url();
  let firstHref = '';
  try {
    firstHref =
      (await page.locator(`${WB_ITEM_SELECTOR} a[href*="detail"]`).first().getAttribute('href')) || '';
  } catch {
    firstHref = '';
  }

  const nextLocators = [
    page.locator('a.pagination__next:not([aria-disabled="true"])').first(),
    page.locator('a[rel="next"]').first(),
    page.getByRole('link', { name: /Следующая|Далее/i }).first(),
  ];

  let clicked = false;
  for (const loc of nextLocators) {
    if (!(await loc.isVisible().catch(() => false))) continue;
    try {
      await loc.click({ timeout: 10_000 });
      clicked = true;
      break;
    } catch {
      /* next */
    }
  }

  if (!clicked) {
    try {
      const u = new URL(page.url());
      if (!u.hostname.includes('wildberries')) return false;
      const cur = parseInt(u.searchParams.get('page') || '1', 10) || 1;
      u.searchParams.set('page', String(cur + 1));
      await page.goto(u.href, { waitUntil: 'load', timeout: NAV_TIMEOUT_MS });
      await randomDelay(2000, 4500);
      if (page.url() === beforeUrl) return false;
      return true;
    } catch {
      return false;
    }
  }

  try {
    await page.waitForFunction(
      ({ prevUrl, prevHref }) => {
        if (window.location.href !== prevUrl) return true;
        const a = document.querySelector(`${WB_ITEM_SELECTOR.split(',')[0].trim()} a[href*="detail"]`);
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
 * @param {import('playwright').Page} page
 * @returns {Promise<number|null>}
 */
async function detectPaginationTotalPagesWb(page) {
  const n = await page.evaluate(() => {
    let max = 0;
    document.querySelectorAll('a[href*="page="]').forEach((a) => {
      try {
        const u = new URL(/** @type {HTMLAnchorElement} */ (a).href, location.origin);
        const p = u.searchParams.get('page');
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

const EXCEL_CHECKPOINT_RETRIES = 10;
const EXCEL_CHECKPOINT_RETRY_MS_MIN = 2000;
const EXCEL_CHECKPOINT_RETRY_MS_MAX = 3500;

function isExcelFileBusyError(e) {
  const code = e && typeof e === 'object' && 'code' in e ? /** @type {{ code?: string }} */ (e).code : '';
  const msg =
    e && typeof e === 'object' && 'message' in e ? String(/** @type {{ message?: string }} */ (e).message) : String(e);
  return code === 'EBUSY' || /EBUSY|resource busy|locked/i.test(msg);
}

/**
 * @param {Awaited<ReturnType<typeof parseWbListings>>} raw
 * @param {object} params
 * @param {string} checkpointLabel
 * @param {Array<object>} priorRows — уже сохранённые строки (например Avito)
 * @param {Array<{ Параметр: string, Значение: string }>|undefined} filterExportRows
 */
async function saveWbCheckpoint(raw, params, checkpointLabel, priorRows, filterExportRows) {
  const filtered = filterWbListings(dedupeByHref(raw), wbListingsFilterOpts(params));
  const wbTagged = filtered.map((r) => ({ ...r, marketplace: 'Wildberries' }));
  const combined = [...priorRows, ...wbTagged];
  for (let i = 0; i < EXCEL_CHECKPOINT_RETRIES; i++) {
    try {
      saveToExcel(combined, { checkpoint: checkpointLabel, filterExportRows });
      return;
    } catch (e) {
      if (!isExcelFileBusyError(e) || i === EXCEL_CHECKPOINT_RETRIES - 1) {
        if (isExcelFileBusyError(e)) {
          log(
            `  не удалось записать checkpoint WB (${checkpointLabel}): закройте ${resultsPath()} в Excel.`
          );
          return;
        }
        throw e;
      }
      await randomDelay(EXCEL_CHECKPOINT_RETRY_MS_MIN, EXCEL_CHECKPOINT_RETRY_MS_MAX);
    }
  }
}

/**
 * @param {object} params — query, extraKeywords, minPrice, maxPrice, memory, color, wbRatingMode
 * @param {{ priorExcelRows?: Array<object>, filterExportRows?: Array<{ Параметр: string, Значение: string }> }} [opts]
 * @returns {Promise<Array<object>>} отфильтрованные строки (без поля marketplace; с ним в Excel)
 */
async function runAttemptWb(params, opts = {}) {
  let skipEnterBeforeClose = false;
  const priorExcelRows = opts.priorExcelRows || [];
  const filterExportRows = opts.filterExportRows;

  const domGateCtx = { page: /** @type {import('playwright').Page | null} */ (null) };

  const builtUrl = buildWbSearchUrl({
    query: params.query,
    extraKeywords: params.extraKeywords,
    minPrice: params.minPrice,
    maxPrice: params.maxPrice,
    page: 1,
  });

  const openingFromResume = resumeListingUrlWb != null;
  const openUrl = openingFromResume ? resumeListingUrlWb : builtUrl;
  /** @type {number|null} */
  let savedResumePageIdx = null;
  if (openingFromResume) {
    savedResumePageIdx = resumeSerpPageIndexWb;
    resumeListingUrlWb = null;
    resumeSerpPageIndexWb = null;
    log(
      `  WB возобновление: открываем сохранённый URL${savedResumePageIdx != null ? ` (стр. ${savedResumePageIdx})` : ''}.`
    );
  }

  const crawlState = { serpPageIndex: 1 };

  logStep(1, 'Запуск браузера (Wildberries)', isProxyDisabled() ? 'без прокси' : 'с прокси');
  const browser = await launchBrowser();
  const context = await newStealthContext(browser);
  const page = await context.newPage();
  domGateCtx.page = page;

  await waitProxyManualGate();

  const ipHooks = {
    onIpBlock() {
      skipEnterBeforeClose = true;
      throw new Error('IP_BLOCK');
    },
  };

  const domGateFlow = {
    onDomGateFailedAutoClose() {
      skipEnterBeforeClose = true;
      try {
        const p = domGateCtx.page;
        if (p) {
          resumeListingUrlWb = p.url();
          resumeSerpPageIndexWb = crawlState.serpPageIndex;
          log(`  WB запомнили: стр. ${resumeSerpPageIndexWb} — ${resumeListingUrlWb}`);
        }
      } catch (_) {
        /* ignore */
      }
      log('  WB: выдача не поднялась — закрываем браузер без Enter, будет повтор.');
    },
  };

  try {
    log('  сценарий WB: переход → гейт → парсинг → скролл → страницы выдачи');
    await randomDelay(SOFT.beforeGotoMin, SOFT.beforeGotoMax);
    logStep(4, 'Адрес поиска Wildberries', openUrl);

    try {
      const response = await page.goto(openUrl, { waitUntil: 'load', timeout: NAV_TIMEOUT_MS });
      if (response) {
        const st = response.status();
        if (st === 403 || st === 429) {
          skipEnterBeforeClose = true;
          throw new Error('IP_BLOCK');
        }
        if (st >= 400) {
          logNetFailureHelp(new Error(`HTTP ${st}`));
          throw new Error(`WB: HTTP ${st}`);
        }
      }
    } catch (navErr) {
      const m = navErr && navErr.message ? navErr.message : String(navErr);
      if (m === 'IP_BLOCK') throw navErr;
      if (isLikelyProxyOrTunnelDrop(navErr)) {
        skipEnterBeforeClose = true;
        throw new Error('IP_BLOCK');
      }
      throw navErr;
    }

    crawlState.serpPageIndex = savedResumePageIdx != null ? savedResumePageIdx : 1;
    await gateWbListingPageReady(
      page,
      openingFromResume ? 'возобновление выдачи WB' : 'страница 1',
      ipHooks,
      { isFirstPage: true },
      domGateFlow
    );

    let listingPageIdx =
      savedResumePageIdx != null
        ? savedResumePageIdx
        : openingFromResume
          ? parseWbSerpPageIndexFromUrl(page.url())
          : 1;
    if (!openingFromResume) {
      listingPageIdx = 1;
    } else {
      log(`  WB: продолжаем со страницы выдачи ${listingPageIdx}`);
    }
    crawlState.serpPageIndex = listingPageIdx;

    await randomDelay(2500, 5500);
    let raw = [];
    let batch = await parseWbListings(page);
    raw.push(...batch);
    log(`  WB собрано записей (первый проход): ${batch.length}`);

    await humanBehavior(page);
    await scrollForMoreWbListings(page);
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await randomDelay(2500, 5000);

    const totalPages = await detectPaginationTotalPagesWb(page);
    if (totalPages != null) log(`  WB пагинация: до ${totalPages} стр. (оценка по ссылкам)`);

    batch = await parseWbListings(page);
    raw.push(...batch);
    raw = dedupeByHref(raw);
    log(`  WB после скролла и второго прохода, уникальных: ${raw.length}`);

    await saveWbCheckpoint(
      raw,
      params,
      `WB страница ${listingPageIdx} (перед следующей)`,
      priorExcelRows,
      filterExportRows
    );

    while (listingPageIdx < MAX_SEARCH_PAGES_WB) {
      if (totalPages != null && listingPageIdx >= totalPages) {
        log(`  WB: достигнута стр. ${totalPages}`);
        break;
      }
      const moved = await clickNextWbPage(page);
      if (!moved) {
        log(`  WB: следующей страницы нет, обработано ${listingPageIdx}`);
        break;
      }
      listingPageIdx += 1;
      crawlState.serpPageIndex = listingPageIdx;
      log(`  WB открыта страница ${listingPageIdx}`);

      await gateWbListingPageReady(
        page,
        `WB страница ${listingPageIdx}`,
        ipHooks,
        { isFirstPage: false },
        domGateFlow
      );

      await page.evaluate(() => window.scrollTo(0, 0));
      await randomDelay(2000, 4500);
      await gentleWarmupScroll(page);
      await scrollForMoreWbListings(page);
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await randomDelay(2500, 5000);

      batch = await parseWbListings(page);
      raw.push(...batch);
      raw = dedupeByHref(raw);
      log(`  WB стр. ${listingPageIdx}: +${batch.length}, всего уникальных: ${raw.length}`);

      await saveWbCheckpoint(
        raw,
        params,
        `WB страница ${listingPageIdx} (перед следующей)`,
        priorExcelRows,
        filterExportRows
      );
    }

    raw = dedupeByHref(raw);
    const filtered = filterWbListings(raw, wbListingsFilterOpts(params));
    log(`  WB итого после фильтров: ${filtered.length}`);
    return filtered;
  } finally {
    if (!skipEnterBeforeClose) {
      logStep(15, 'Пауза перед закрытием (WB)', '');
      await waitEnterBeforeCloseBrowser();
    } else {
      log('  WB: закрываем браузер без Enter (блок / сбой / повтор).');
    }
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
  }
}

module.exports = {
  runAttemptWb,
};
