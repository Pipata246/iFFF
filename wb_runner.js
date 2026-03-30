/**
 * Сценарий Playwright для Wildberries: та же идея, что runAttempt в main (гейт DOM, скролл, пагинация, Excel).
 */

const readline = require('readline');
const { launchBrowser, newStealthContext, isProxyDisabled, PROXY_URL } = require('./browser');
const {
  log,
  logStep,
  logBlock,
  randomDelay,
  randomInt,
  dedupeByHref,
  resultsPath,
  waitEnterBeforeCloseBrowser,
} = require('./utils');
const { detectBlock, saveToExcel } = require('./parser');
const {
  buildWbSearchUrl,
  normalizeWbSearchUrlSingleFeed,
  hasWbEmptySerpMessage,
  looksLikeWbSkeletonNoItems,
  parseWbListings,
  filterWbListings,
  wbListingsFilterOpts,
} = require('./parser_wb');

let resumeListingUrlWb = null;
let resumeSerpPageIndexWb = null;

/**
 * @param {{ domGateCtx: { page: import('playwright').Page | null }, crawlState: { serpPageIndex: number }, openUrl: string }} ctx
 */
function rememberWbListingForIpRetry(ctx) {
  const { domGateCtx, crawlState, openUrl } = ctx;
  let url = openUrl;
  try {
    const p = domGateCtx.page;
    if (p && typeof p.url === 'function') {
      const u = p.url();
      if (u && /wildberries\.ru/i.test(u) && !/^(chrome-error|about:|chrome:\/\/)/i.test(u)) {
        url = u;
      }
    }
  } catch (_) {
    /* keep openUrl */
  }
  if (!/wildberries\.ru/i.test(url)) {
    url = openUrl;
  }
  resumeListingUrlWb = normalizeWbSearchUrlSingleFeed(url);
  resumeSerpPageIndexWb = 1;
  log(
    `  WB: капча / IP-блок — сохранена позиция выдачи (стр. ${resumeSerpPageIndexWb}); закроем браузер и повторим после ротации мобильного прокси.`
  );
  log(`  ${resumeListingUrlWb}`);
}

const NAV_TIMEOUT_MS = 180_000;
const PAGE_LOAD_TIMEOUT_MS = 90_000;
/** Перезагрузки вкладки при «голом» DOM (как main.js у Avito): domTry 1..MAX = ровно MAX перезагрузок после первой загрузки. */
const MAX_DOM_RELOAD_ATTEMPTS = 3;
const CARD_WAIT_BEFORE_FIRST_RELOAD_MS = 50_000;
const CARD_WAIT_AFTER_DOM_RELOAD_MS = 95_000;
/** Скролл ленты WB: столько циклов «вниз + пауза» (подгрузка без пагинации). */
const WB_FEED_SCROLL_MAX = 120;
/** Подряд одинаковый счётчик карточек — считаем, лента закончилась. */
const WB_FEED_STABLE_ROUNDS = 4;

/** Карточки товара на выдаче WB (несколько вариантов вёрстки). */
const WB_ITEM_SELECTOR =
  'article.product-card__wrapper, article[data-nm-id], article.product-card';

/**
 * Код ответа на главный документ часто приходит от прокси/CDN (например 498) до готовности SPA.
 * Не завершаем сценарий сразу — ждём загрузку и даём гейту проверить карточки.
 */
const WB_NAV_SOFT_HTTP = new Set([408, 425, 498, 499, 500, 502, 503, 504]);

/** Главная для «теплого» захода до URL поиска (меньше триггеров антибота, чем cold goto в каталог). */
const WB_HOME_URL = 'https://www.wildberries.ru/';

/**
 * @param {import('playwright').Page} page
 * @param {number} reportedStatus
 */
async function waitForWbDomAfterSoftHttp(page, reportedStatus) {
  log(
    `  главный ответ HTTP ${reportedStatus} — часто «шум» прокси до готовности WB; ждём load и догрузку вёрстки (reload тут не делаем)…`
  );
  await page.waitForLoadState('load', { timeout: PAGE_LOAD_TIMEOUT_MS }).catch((e) => {
    log(`  повторное ожидание load: ${String(e.message || e).slice(0, 96)}`);
  });
  await page.waitForLoadState('domcontentloaded', { timeout: 45_000 }).catch(() => {});
  await randomDelay(12_000, 22_000);
  // Важно: никаких reload здесь делать не надо.
  // До шага DOM-gate у нас ещё не выполнена проверка капчи/блока,
  // и WB часто банит за лишние перезагрузки в момент "browser check".
  // DOM-gate (gateWbListingPageReady) уже сделает reload только когда карточки реально не появились.
}

const SOFT = {
  /** Перед любой навигацией WB — «остывание» после открытия вкладки. */
  wbEntryPreambleMin: 2500,
  wbEntryPreambleMax: 7000,
  /** Если WB_SKIP_HOME_WARMUP=1 — только эти паузы перед прямым goto на выдачу. */
  beforeGotoMin: 4000,
  beforeGotoMax: 11_000,
  /** После загрузки главной — дать сайту и скриптам «успокоиться». */
  wbHomeSettleMin: 10_000,
  wbHomeSettleMax: 22_000,
  /** После активности на главной — перед переходом в поиск. */
  wbBeforeSearchFromHomeMin: 5000,
  wbBeforeSearchFromHomeMax: 14_000,
  /** После гейта выдачи — как «осмотр страницы» перед парсингом (сопоставимо с паузой Avito). */
  afterListingGateMin: 9000,
  afterListingGateMax: 18_000,
  /** WB: дольше «остываем» после load перед проверкой капчи. */
  wbSettleAfterLoadMin: 12_000,
  wbSettleAfterLoadMax: 24_000,
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
 * До перехода на выдачу: неспешные движения мыши и короткий скролл (имитация осмотра главной).
 * @param {import('playwright').Page} page
 */
async function wbWarmHumanPresence(page) {
  const vp = page.viewportSize();
  const w = Math.max(200, vp?.width ?? 1280);
  const h = Math.max(200, vp?.height ?? 720);
  for (let i = 0, n = randomInt(4, 8); i < n; i++) {
    await page.mouse.move(randomInt(120, w - 120), randomInt(100, h - 100), {
      steps: randomInt(18, 42),
    });
    await randomDelay(500, 1800);
  }
  for (let i = 0, n = randomInt(2, 5); i < n; i++) {
    await page.mouse.wheel(0, randomInt(30, 110));
    await randomDelay(800, 2400);
  }
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
    logStep(
      6,
      'Загрузка выдачи Wildberries',
      `${pageLabel} — событие load, затем капча и карточки в DOM (как на Avito)`
    );
  } else {
    log(`  [${pageLabel}] WB: load → капча / блок → карточки в DOM`);
  }

  await page.waitForLoadState('load', { timeout: PAGE_LOAD_TIMEOUT_MS }).catch((e) => {
    log(
      `  [${pageLabel}] load не за ${PAGE_LOAD_TIMEOUT_MS / 1000} с — продолжаем (${String(
        e.message
      ).slice(0, 96)})`
    );
  });

  await randomDelay(SOFT.wbSettleAfterLoadMin, SOFT.wbSettleAfterLoadMax);

  if (isFirstPage) {
    logStep(7, 'Проверка на капчу / ограничение (WB)', pageLabel);
  } else {
    log(`  [${pageLabel}] WB: проверка капчи / антибота…`);
  }
  if (await detectBlock(page)) {
    logBlock(`[${pageLabel}] Wildberries — капча или экран ограничения; закрываем браузер и ждём новый IP`);
    hooks.onIpBlock();
  }
  if (isFirstPage) {
    logStep(7, 'Ограничений в тексте и виджетах не видно (WB)', 'продолжаем');
  }

  if (isFirstPage) {
    logStep(
      8,
      'Ожидание карточек товаров в DOM',
      `${WB_ITEM_SELECTOR}; при «голом» HTML — до ${MAX_DOM_RELOAD_ATTEMPTS} перезагрузок вкладки (капчу не трогаем — только новый IP)`
    );
  } else {
    log(
      `  [${pageLabel}] WB: ожидание карточек; перезагрузка вкладки без смены IP только если нет капчи…`
    );
  }

  let gotCards = false;
  for (let domTry = 0; domTry <= MAX_DOM_RELOAD_ATTEMPTS; domTry++) {
    if (domTry > 0) {
      log(
        `  [${pageLabel}] перезагрузка страницы WB в той же сессии (${domTry}/${MAX_DOM_RELOAD_ATTEMPTS})…`
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
            throw new Error(`Перезагрузка WB: HTTP ${st}`);
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
      await randomDelay(SOFT.wbSettleAfterLoadMin, SOFT.wbSettleAfterLoadMax);
      if (await detectBlock(page)) {
        logBlock(`[${pageLabel}] после перезагрузки WB — капча / блок`);
        hooks.onIpBlock();
      }
    }

    const waitMs = domTry === 0 ? CARD_WAIT_BEFORE_FIRST_RELOAD_MS : CARD_WAIT_AFTER_DOM_RELOAD_MS;
    try {
      await page.waitForSelector(WB_ITEM_SELECTOR, { timeout: waitMs });
      gotCards = true;
      break;
    } catch {
      if (await detectBlock(page)) {
        logBlock(`[${pageLabel}] карточек нет — при проверке видна капча / блок WB`);
        hooks.onIpBlock();
      }
      if (await hasWbEmptySerpMessage(page)) {
        throw new Error('Wildberries: пустая выдача по запросу.');
      }
      const skeleton = await looksLikeWbSkeletonNoItems(page);
      if (domTry >= MAX_DOM_RELOAD_ATTEMPTS) {
        log(`  [${pageLabel}] карточек нет после ${MAX_DOM_RELOAD_ATTEMPTS} перезагрузок вкладки`);
        markDomGateFailedAutoClose();
        throw new Error(
          'На странице WB не появились карточки товаров. Проверьте прокси, запрос или вёрстку сайта.'
        );
      }
      if (skeleton) {
        log(`  [${pageLabel}] похоже на страницу без нормальной вёрстки выдачи WB`);
        continue;
      }
      if (domTry === 0) {
        log(
          `  [${pageLabel}] карточек нет — одна перезагрузка вкладки на случай медленной подгрузки JS…`
        );
        continue;
      }
      markDomGateFailedAutoClose();
      throw new Error(
        'На странице WB не появились карточки товаров за отведённое время. Проверьте прокси или вёрстку.'
      );
    }
  }

  if (!gotCards) {
    markDomGateFailedAutoClose();
    throw new Error('На странице WB не появились карточки товаров.');
  }

  const cnt = await page.locator(WB_ITEM_SELECTOR).count();
  if (isFirstPage) {
    logStep(8, 'Карточки товаров WB в DOM', `${cnt} шт.`);
  } else {
    log(`  [${pageLabel}] карточек WB в DOM: ${cnt} шт.`);
  }
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

/**
 * Wildberries: одна «страница» поиска = бесконечная лента; крутим вниз, пока число карточек перестанет расти.
 * @param {import('playwright').Page} page
 * @param {{ maxIter?: number, stableRounds?: number }} [opts]
 */
async function scrollWbFeedUntilStable(page, opts = {}) {
  const maxIter = Number.isFinite(opts.maxIter) && opts.maxIter > 0 ? Math.floor(opts.maxIter) : WB_FEED_SCROLL_MAX;
  const stableNeed =
    Number.isFinite(opts.stableRounds) && opts.stableRounds > 0
      ? Math.floor(opts.stableRounds)
      : WB_FEED_STABLE_ROUNDS;
  let last = await page.locator(WB_ITEM_SELECTOR).count().catch(() => 0);
  let stable = 0;
  log(
    `  WB: догрузка ленты скроллом (без вкладок/страниц), до ${maxIter} циклов, стабильность ×${stableNeed}…`
  );
  for (let i = 0; i < maxIter; i++) {
    await page.evaluate(() => {
      const h = document.body?.scrollHeight ?? 0;
      const y = window.scrollY;
      const step = Math.min(950, Math.max(350, (h - y) / 2.5));
      window.scrollBy(0, step);
    });
    await page.mouse.wheel(0, randomInt(SOFT.listWheelMin, SOFT.listWheelMax + 150));
    await randomDelay(SOFT.listPauseMin + 500, SOFT.listPauseMax + 2000);
    if (i % 6 === 5) {
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await randomDelay(2200, 4500);
    }
    const n = await page.locator(WB_ITEM_SELECTOR).count().catch(() => 0);
    if (i % 12 === 11 || i === 0) {
      log(`  WB лента: цикл ${i + 1}/${maxIter}, карточек в DOM — ${n}`);
    }
    if (n === last) {
      stable += 1;
      if (stable >= stableNeed) {
        log(
          `  WB: карточек ${n} — не менялось ${stableNeed} раз подряд, лента исчерпана или лимит подгрузки`
        );
        break;
      }
    } else {
      stable = 0;
      last = n;
    }
  }
  await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
  await randomDelay(4000, 7500);
  const finalN = await page.locator(WB_ITEM_SELECTOR).count().catch(() => 0);
  log(`  WB: после прокрутки ленты карточек в DOM — ${finalN}`);
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
    memory: params.memory,
    page: 1,
  });

  const openingFromResume = resumeListingUrlWb != null;
  const resumedUrl = openingFromResume ? resumeListingUrlWb : null;
  if (openingFromResume) {
    resumeListingUrlWb = null;
    resumeSerpPageIndexWb = null;
    log('  WB возобновление: сохранённый адрес выдачи (одна лента, без «страниц»).');
  }
  const openUrl = normalizeWbSearchUrlSingleFeed(resumedUrl || builtUrl);

  const crawlState = { serpPageIndex: 1 };

  logStep(1, 'Запуск браузера (Wildberries)', isProxyDisabled() ? 'без прокси' : 'с прокси');
  if (isProxyDisabled()) {
    log('  WB: AVITO_NO_PROXY — без прокси, тот же режим, что у Avito.');
  } else {
    try {
      const u = new URL(PROXY_URL);
      const port = u.port || (u.protocol === 'https:' ? '443' : '80');
      log(
        `  WB: используется тот же мобильный HTTP-прокси, что и у Avito — ${u.hostname}:${port} (browser.js).`
      );
    } catch {
      log('  WB: прокси из browser.js — общий с Avito.');
    }
  }
  const browser = await launchBrowser();
  const context = await newStealthContext(browser);
  const page = await context.newPage();
  domGateCtx.page = page;

  await waitProxyManualGate();

  const ipHooks = {
    onIpBlock() {
      skipEnterBeforeClose = true;
      rememberWbListingForIpRetry({ domGateCtx, crawlState, openUrl });
      throw new Error('IP_BLOCK');
    },
  };

  const domGateFlow = {
    onDomGateFailedAutoClose() {
      skipEnterBeforeClose = true;
      try {
        const p = domGateCtx.page;
        if (p) {
          resumeListingUrlWb = normalizeWbSearchUrlSingleFeed(p.url());
          resumeSerpPageIndexWb = 1;
          log(`  WB запомнили адрес ленты: ${resumeListingUrlWb}`);
        }
      } catch (_) {
        /* ignore */
      }
      log(
        '  WB: до 3 перезагрузок вкладки в этом браузере исчерпаны (или таймаут карточек) — закрываем Chromium без Enter; следующая попытка откроет сохранённый URL. Пауза перед повтором короче, чем при капче (~2 мин).'
      );
    },
  };

  try {
    log('  сценарий WB: мягкий вход → гейт → парсинг → длинный скролл ленты (без пагинации)');
    const skipHomeWarmup =
      process.env.WB_SKIP_HOME_WARMUP === '1' || process.env.WB_SKIP_HOME_WARMUP === 'true';
    if (skipHomeWarmup) {
      log('  WB: WB_SKIP_HOME_WARMUP — без захода на главную, только пауза перед выдачей.');
    }
    await randomDelay(SOFT.wbEntryPreambleMin, SOFT.wbEntryPreambleMax);

    if (!skipHomeWarmup) {
      log('  WB: мягкий вход — сначала главная wildberries.ru, затем страница поиска…');
      try {
        /** @type {import('playwright').Response | null} */
        let homeResp = null;
        try {
          homeResp = await page.goto(WB_HOME_URL, {
            waitUntil: 'domcontentloaded',
            timeout: NAV_TIMEOUT_MS,
          });
        } catch (homeErr) {
          const hm = homeErr && homeErr.message ? homeErr.message : String(homeErr);
          if (hm === 'IP_BLOCK') throw homeErr;
          if (isLikelyProxyOrTunnelDrop(homeErr)) {
            skipEnterBeforeClose = true;
            rememberWbListingForIpRetry({ domGateCtx, crawlState, openUrl });
            throw new Error('IP_BLOCK');
          }
          log(`  WB: главная с задержкой (${hm.slice(0, 120)}) — ждём load в вкладке…`);
          await page.waitForLoadState('load', { timeout: PAGE_LOAD_TIMEOUT_MS }).catch(() => {});
          await randomDelay(8000, 15_000);
          homeResp = null;
        }
        if (homeResp) {
          const stHome = homeResp.status();
          if (stHome === 403 || stHome === 429) {
            skipEnterBeforeClose = true;
            rememberWbListingForIpRetry({ domGateCtx, crawlState, openUrl });
            throw new Error('IP_BLOCK');
          }
          if (WB_NAV_SOFT_HTTP.has(stHome)) {
            await waitForWbDomAfterSoftHttp(page, stHome);
          }
        }
      } catch (homeNavErr) {
        const m = homeNavErr && homeNavErr.message ? homeNavErr.message : String(homeNavErr);
        if (m === 'IP_BLOCK') throw homeNavErr;
        if (isLikelyProxyOrTunnelDrop(homeNavErr)) {
          skipEnterBeforeClose = true;
          rememberWbListingForIpRetry({ domGateCtx, crawlState, openUrl });
          throw new Error('IP_BLOCK');
        }
        log(`  WB: главная не прошла (${m.slice(0, 140)}) — всё равно открываем URL поиска…`);
        await randomDelay(6000, 12_000);
      }
      await page.waitForLoadState('load', { timeout: PAGE_LOAD_TIMEOUT_MS }).catch(() => {});
      await randomDelay(SOFT.wbHomeSettleMin, SOFT.wbHomeSettleMax);
      // Перед любыми "активностями" мышью/скроллом проверяем, не запустилась ли
      // "проверка браузера" / капча. Если она есть — НЕ трогаем страницу,
      // закрываем браузер и уходим на IP-ротацию.
      if (await detectBlock(page)) {
        logBlock('WB: на главной обнаружена капча/проверка браузера — закрываем браузер и ждём ротацию IP');
        skipEnterBeforeClose = true;
        rememberWbListingForIpRetry({ domGateCtx, crawlState, openUrl });
        throw new Error('IP_BLOCK');
      }
      await wbWarmHumanPresence(page);
      await randomDelay(SOFT.wbBeforeSearchFromHomeMin, SOFT.wbBeforeSearchFromHomeMax);
    } else {
      await randomDelay(SOFT.beforeGotoMin, SOFT.beforeGotoMax);
    }

    logStep(4, 'Адрес поиска Wildberries', openUrl);

    try {
      /** @type {import('playwright').Response | null} */
      let response = null;
      try {
        response = await page.goto(openUrl, {
          waitUntil: 'domcontentloaded',
          timeout: NAV_TIMEOUT_MS,
        });
      } catch (gotoErr) {
        const gm = gotoErr && gotoErr.message ? gotoErr.message : String(gotoErr);
        if (gm === 'IP_BLOCK') throw gotoErr;
        if (isLikelyProxyOrTunnelDrop(gotoErr)) {
          skipEnterBeforeClose = true;
          rememberWbListingForIpRetry({ domGateCtx, crawlState, openUrl });
          throw new Error('IP_BLOCK');
        }
        log(
          `  WB: переход прервался (${gm.slice(0, 120)}) — ждём, пока страница догрузится в текущей вкладке…`
        );
        await page.waitForLoadState('load', { timeout: PAGE_LOAD_TIMEOUT_MS }).catch(() => {});
        await randomDelay(10_000, 20_000);
        response = null;
      }

      if (response) {
        const st = response.status();
        if (st === 403 || st === 429) {
          skipEnterBeforeClose = true;
          rememberWbListingForIpRetry({ domGateCtx, crawlState, openUrl });
          throw new Error('IP_BLOCK');
        }
        if (WB_NAV_SOFT_HTTP.has(st)) {
          await waitForWbDomAfterSoftHttp(page, st);
        } else if (st >= 400) {
          logNetFailureHelp(new Error(`HTTP ${st}`));
          throw new Error(`WB: HTTP ${st}`);
        }
      }
    } catch (navErr) {
      const m = navErr && navErr.message ? navErr.message : String(navErr);
      if (m === 'IP_BLOCK') throw navErr;
      if (/^WB: HTTP \d+/i.test(m)) throw navErr;
      if (isLikelyProxyOrTunnelDrop(navErr)) {
        skipEnterBeforeClose = true;
        rememberWbListingForIpRetry({ domGateCtx, crawlState, openUrl });
        throw new Error('IP_BLOCK');
      }
      throw navErr;
    }

    await gateWbListingPageReady(
      page,
      openingFromResume ? 'возобновление выдачи WB' : 'страница 1',
      ipHooks,
      { isFirstPage: true },
      domGateFlow
    );

    crawlState.serpPageIndex = 1;

    log('  пауза перед сбором данных с выдачи WB (осторожный осмотр страницы, как на Avito)…');
    await randomDelay(SOFT.afterListingGateMin, SOFT.afterListingGateMax);
    let raw = [];
    let batch = await parseWbListings(page);
    raw.push(...batch);
    log(`  WB собрано записей (первый проход, верх ленты): ${batch.length}`);

    logStep(10, 'Скролл ленты Wildberries', 'имитация чтения и подгрузка карточек до упора');
    await humanBehavior(page);
    await scrollWbFeedUntilStable(page);

    batch = await parseWbListings(page);
    raw.push(...batch);
    raw = dedupeByHref(raw);
    log(`  WB после полного скролла ленты, уникальных: ${raw.length}`);

    log('  финальный проход: снова вверх и укороченный скролл ленты для догрузки…');
    await page.evaluate(() => window.scrollTo(0, 0));
    await randomDelay(2000, 4000);
    await gentleWarmupScroll(page);
    await humanBehavior(page);
    await scrollWbFeedUntilStable(page, { maxIter: 70, stableRounds: 3 });
    batch = await parseWbListings(page);
    raw.push(...batch);
    raw = dedupeByHref(raw);
    log(`  WB после финального прохода, уникальных: ${raw.length}`);

    await saveWbCheckpoint(
      raw,
      params,
      'WB вся лента выдачи (одна страница поиска)',
      priorExcelRows,
      filterExportRows
    );

    raw = dedupeByHref(raw);
    const filtered = filterWbListings(raw, wbListingsFilterOpts(params));
    log(`  WB итого после фильтров: ${filtered.length}`);
    resumeListingUrlWb = null;
    resumeSerpPageIndexWb = null;
    return filtered;
  } finally {
    if (!skipEnterBeforeClose) {
      logStep(15, 'Закрытие браузера (WB)', 'пауза только если AVITO_WAIT_ENTER=1');
      await waitEnterBeforeCloseBrowser('Wildberries');
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
