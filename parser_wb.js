/**
 * Wildberries: URL поиска, парсинг карточек выдачи, фильтрация.
 * Вёрстка WB часто меняется — селекторы с запасными вариантами внутри page.evaluate.
 */

const { parsePriceNumber } = require('./utils');

/**
 * Число для filterWbListings: при двух ценах в одной строке (витрина + WB Кошелёк) нужна **минимальная** сумма.
 * parsePriceNumber склеивает все цифры подряд → получается гигантское число → всё отсекается по maxPrice и в Excel 0 строк.
 * @param {string} priceText
 * @returns {number|null}
 */
function parseWbPriceNumberForFilter(priceText) {
  const t = String(priceText || '')
    .replace(/\u00a0|\u2009|\u202f|\u2007/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!t) return null;
  const amounts = [];
  const withRub = /(\d(?:[\d\s])*)\s*(?:₽|руб\.?|RUB)(?!\w)/gi;
  let m;
  while ((m = withRub.exec(t)) !== null) {
    const n = parseInt(m[1].replace(/\D/g, ''), 10);
    if (Number.isFinite(n) && n >= 500 && n < 50_000_000) amounts.push(n);
  }
  if (amounts.length > 0) return Math.min(...amounts);
  const spaced = t.match(/\d{1,3}(?:\s\d{3})+/g) || [];
  for (const frag of spaced) {
    const n = parseInt(frag.replace(/\D/g, ''), 10);
    if (Number.isFinite(n) && n >= 3_000 && n < 50_000_000) amounts.push(n);
  }
  if (amounts.length > 0) return Math.min(...amounts);
  const compact = t.match(/\b\d{5,7}\b/g) || [];
  for (const frag of compact) {
    const n = parseInt(frag, 10);
    if (Number.isFinite(n) && n >= 8_000 && n < 50_000_000) amounts.push(n);
  }
  if (amounts.length > 0) return Math.min(...amounts);
  const fallback = parsePriceNumber(t);
  if (fallback != null && fallback >= 500 && fallback < 10_000_000) return fallback;
  return null;
}

/**
 * @typedef {Object} WbSearchParams
 * @property {string} query
 * @property {string} [extraKeywords]
 * @property {string} [memory] — строка из консоли (например 128), для URL f4424
 * @property {string} [color] — строка цвета из консоли (например "Синий"), для URL f14177449
 * @property {number} minPrice
 * @property {number} maxPrice
 * @property {number} [page] — 1-based для пагинации в URL
 */

/**
 * Фильтр «встроенная память» в поиске WB: в URL добавляются `f4424=<id>` и `meta_charcs=true`.
 * По вашему URL/скриншоту: f4424=25425 соответствует 256 ГБ.
 * Другие объёмы: на wildberries.ru выберите фильтр памяти, скопируйте число после f4424 из адреса и добавьте строку в объект ниже.
 */
const WB_MEMORY_FACET_PARAM = 'f4424';
const WB_MEMORY_GB_TO_F4424 = {
  128: 12868,
  256: 25425,
  512: 117419,
  1024: 231154,
};

/** Цвет в URL WB (facet id из строки поиска). */
const WB_COLOR_FACET_PARAM = 'f14177449';
const WB_COLOR_TO_F14177449 = {
  бежевый: 20214644,
  beige: 20214644,
  белый: 12065905,
  white: 12065905,
  голубой: 20214449,
  lightblue: 20214449,
  'светло-синий': 20214449,
  желтый: 14185777,
  жёлтый: 14185777,
  yellow: 14185777,
  зеленый: 14835931,
  зелёный: 14835931,
  green: 14835931,
  коричневый: 20214658,
  brown: 20214658,
  красный: 11807341,
  red: 11807341,
  оранжевый: 20214770,
  orange: 20214770,
  розовый: 11807342,
  pink: 11807342,
  серый: 20214430,
  grey: 20214430,
  gray: 20214430,
  синий: 20214646,
  blue: 20214646,
  фиолетовый: 14185662,
  purple: 14185662,
  черный: 13600062,
  чёрный: 13600062,
  black: 13600062,
};

/**
 * @param {string} memoryRaw
 * @returns {number|null} объём в ГБ для карты WB_MEMORY_GB_TO_F4424
 */
function parseWbMemoryGb(memoryRaw) {
  const s = String(memoryRaw || '').trim().replace(/\s+/g, '');
  // Если пользователь ввёл "1ТБ" — интерпретируем как 1024 ГБ для маппинга f4424.
  const hasTb = /tb|тб|терабайт/i.test(s);
  const digits = s.replace(/\D/g, '');
  if (!digits) return null;
  const n = parseInt(digits, 10);
  if (!Number.isFinite(n) || n <= 0) return null;
  return hasTb ? n * 1024 : n;
}

/**
 * На WB: всегда sort=priceup (рост цены), без priceU в URL — иначе при пустой выдаче сайт подмешивает мусор.
 * Ценовой диапазон отсекается в коде после парсинга карточек (см. filterWbListings + ранний стоп скролла в wb_runner).
 * Память/цвет — f4424 / f14177449 + meta_charcs=true при известных id.
 * @param {WbSearchParams} p
 * @returns {string}
 */
function buildWbSearchUrl(p) {
  const extraRaw = (p.extraKeywords || '').trim();
  const fullQ = [String(p.query || '').trim(), extraRaw].filter(Boolean).join(' ').trim();
  const u = new URL('https://www.wildberries.ru/catalog/0/search.aspx');

  const pageNum = Number.isFinite(p.page) && p.page >= 1 ? Math.floor(p.page) : 1;
  u.searchParams.set('page', String(pageNum));
  u.searchParams.set('sort', 'priceup');
  u.searchParams.set('search', fullQ);

  const memGb = parseWbMemoryGb(p.memory || '');
  let hasMetaCharsFacet = false;
  if (memGb != null && memGb in WB_MEMORY_GB_TO_F4424) {
    const v = WB_MEMORY_GB_TO_F4424[memGb];
    u.searchParams.set(WB_MEMORY_FACET_PARAM, String(v));
    hasMetaCharsFacet = true;
  }

  const colorRaw = String(p.color || '').trim().toLowerCase();
  if (colorRaw && colorRaw in WB_COLOR_TO_F14177449) {
    u.searchParams.set(WB_COLOR_FACET_PARAM, String(WB_COLOR_TO_F14177449[colorRaw]));
    hasMetaCharsFacet = true;
  }
  if (hasMetaCharsFacet) {
    u.searchParams.set('meta_charcs', 'true');
  }

  return u.href;
}

/**
 * @param {string} urlStr
 * @returns {number}
 */
function parseWbSerpPageIndexFromUrl(urlStr) {
  try {
    const u = new URL(urlStr);
    const p = u.searchParams.get('page');
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
 * Выдача WB на поиске — бесконечная лента без «страниц»; в URL всегда держим page=1,
 * чтобы не уходить на ложную пагинацию после смены IP / возобновления.
 * @param {string} urlStr
 * @returns {string}
 */
function normalizeWbSearchUrlSingleFeed(urlStr) {
  try {
    const u = new URL(urlStr);
    if (!u.hostname.toLowerCase().includes('wildberries')) return urlStr;
    u.searchParams.set('page', '1');
    u.searchParams.set('sort', 'priceup');
    u.searchParams.delete('priceU');
    return u.href;
  } catch (_) {
    return urlStr;
  }
}

/**
 * @param {import('playwright').Page} page
 */
async function hasWbEmptySerpMessage(page) {
  const body = await page.locator('body').innerText().catch(() => '');
  const low = body.toLowerCase();
  return (
    /ничего не найдено|по вашему запросу ничего|товаров не найдено|0\s+товар|нет\s+товаров/i.test(
      low
    ) || /не\s+нашли\s+подходящ/i.test(low)
  );
}

/**
 * @param {import('playwright').Page} page
 */
async function looksLikeWbSkeletonNoItems(page) {
  return page.evaluate(() => {
    const h = (location.hostname || '').toLowerCase();
    if (!h.includes('wildberries')) return false;
    const cards = document.querySelectorAll(
      'article.product-card__wrapper, article[data-nm-id], article.product-card, [data-nm-id]'
    );
    if (cards.length > 0) return false;
    const t = (document.body && document.body.innerText) || '';
    if (t.length < 100) return false;
    return /wildberries|вайлдберриз/i.test(t) && t.length < 8000;
  });
}

/**
 * @returns {Promise<Array<{ title: string, priceText: string, href: string, city: string, memoryLabel: string, sellerName: string, publishedLabel: string, sellerKind: string, rating: number|null, reviewsCount: number|null, colorHint: string }>>}
 * @param {import('playwright').Page} page
 */
async function parseWbListings(page) {
  const items = await page.evaluate(() => {
    const base = window.location.origin || 'https://www.wildberries.ru';

    function isProductHref(href) {
      if (!href || href.startsWith('javascript:')) return false;
      try {
        const u = new URL(href, base);
        const host = u.hostname.toLowerCase();
        if (!host.includes('wildberries')) return false;
        return /\/catalog\/\d+\/detail\.aspx/i.test(u.pathname) || /\/catalog\/[^/]+\/detail\.aspx/i.test(u.pathname);
      } catch {
        return false;
      }
    }

    function pickCardRoots() {
      const sels = [
        'article.product-card__wrapper',
        'article[data-nm-id]',
        'article.product-card',
        '[data-nm-id].product-card',
        '.product-card',
      ];
      const seen = new Set();
      const roots = [];
      for (const sel of sels) {
        document.querySelectorAll(sel).forEach((el) => {
          if (!(el instanceof HTMLElement)) return;
          if (seen.has(el)) return;
          seen.add(el);
          roots.push(el);
        });
      }
      return roots;
    }

    function cardMainLink(root) {
      const prefer = root.querySelector(
        'a[href*="/catalog/"][href*="detail.aspx"], a[href*="/catalog/"][href*="/detail/"]'
      );
      if (prefer && 'href' in prefer && isProductHref(/** @type {HTMLAnchorElement} */ (prefer).href))
        return /** @type {HTMLAnchorElement} */ (prefer);
      for (const a of root.querySelectorAll('a[href*="/catalog/"]')) {
        if (isProductHref(/** @type {HTMLAnchorElement} */ (a).href)) return /** @type {HTMLAnchorElement} */ (a);
      }
      return null;
    }

    function pickTitle(root, mainA) {
      const el =
        root.querySelector('[class*="product-card__name"]') ||
        root.querySelector('[class*="goods-name"]') ||
        root.querySelector('span[class*="name"]');
      const fromEl = el?.textContent?.trim();
      if (fromEl) return fromEl.replace(/\s+/g, ' ');
      const t = mainA?.getAttribute('aria-label') || mainA?.textContent?.trim();
      return t ? t.replace(/\s+/g, ' ') : '';
    }

    /**
     * На карточке несколько сумм (старая, со скидкой, с WB Кошельком). Берём минимальную.
     * На WB часто нет символа ₽ в том же textNode — тогда ищем группы «102 032» / «102032».
     */
    function normalizeMoneySpaces(s) {
      return String(s || '')
        .replace(/\u00a0|\u2009|\u202f|\u2007/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
    }

    function rubAmountsInText(s) {
      const t = normalizeMoneySpaces(s);
      const out = [];
      const reStrict = /(\d(?:[\d\s])*)\s*(?:₽|руб\.?|RUB)\b/gi;
      let m;
      while ((m = reStrict.exec(t)) !== null) {
        const n = parseInt(m[1].replace(/\D/g, ''), 10);
        if (Number.isFinite(n) && n >= 100 && n < 50_000_000) out.push(n);
      }
      if (out.length) return out;
      const spaced = t.match(/\d{1,3}(?:\s\d{3})+/g) || [];
      for (const frag of spaced) {
        const n = parseInt(frag.replace(/\D/g, ''), 10);
        if (Number.isFinite(n) && n >= 3_000 && n < 50_000_000) out.push(n);
      }
      if (out.length) return out;
      const compact = t.match(/\b\d{5,7}\b/g) || [];
      for (const frag of compact) {
        const n = parseInt(frag, 10);
        if (Number.isFinite(n) && n >= 8_000 && n < 50_000_000) out.push(n);
      }
      return out;
    }

    function minRubInText(s) {
      const nums = rubAmountsInText(s);
      if (!nums.length) return null;
      return Math.min(...nums);
    }

    /**
     * WB в data-атрибутах и JSON кладёт суммы в копейках (priceU). Если число не кратно 100 — считаем уже рублями.
     */
    function wbKopecksOrRubToRub(raw) {
      const n = parseInt(String(raw).replace(/\D/g, ''), 10);
      if (!Number.isFinite(n) || n < 50) return null;
      if (n % 100 === 0) return Math.round(n / 100);
      return n;
    }

    function addRubFieldsFromAttrString(str, bucket) {
      if (!str || str.length < 12 || str.length > 400000 || !/\d/.test(str)) return;
      if (!/price|sale|wallet|basic|product/i.test(str)) return;
      const re =
        /"(?:salePriceU|priceU|basicPriceU|productPriceU|totalPriceU|walletPriceU|priceWithSaleU)"\s*:\s*(\d+)/gi;
      let m;
      while ((m = re.exec(str)) !== null) {
        const rub = wbKopecksOrRubToRub(m[1]);
        if (rub != null && rub >= 500 && rub < 50_000_000) bucket.add(rub);
      }
    }

    function collectRubFromEmbeddedJson(root, bucket) {
      const walk = (el) => {
        if (!(el instanceof HTMLElement)) return;
        const attrs = el.attributes;
        if (attrs && attrs.length) {
          for (let i = 0; i < attrs.length; i++) {
            addRubFieldsFromAttrString(attrs[i].value, bucket);
          }
        }
        for (const c of el.children) walk(c);
      };
      walk(root);
    }

    function pickNmId(root, href) {
      const d0 = root.getAttribute('data-nm-id');
      if (d0 && /^\d{5,20}$/.test(String(d0).trim())) return String(d0).trim();
      const inner = root.querySelector('[data-nm-id]');
      if (inner) {
        const d1 = inner.getAttribute('data-nm-id');
        if (d1 && /^\d{5,20}$/.test(String(d1).trim())) return String(d1).trim();
      }
      try {
        const u = new URL(href, base);
        const m = u.pathname.match(/\/catalog\/(\d{5,20})\/detail/i);
        if (m) return m[1];
      } catch {
        /* ignore */
      }
      return '';
    }

    /**
     * Собираем все разумные суммы по карточке и берём минимум (витрина / скидка / кошелёк часто в разных узлах и JSON).
     */
    function pickPrice(root) {
      const rubs = new Set();

      function addRub(n) {
        if (n == null || !Number.isFinite(n)) return;
        const x = Math.round(n);
        if (x < 500 || x >= 50_000_000) return;
        rubs.add(x);
      }

      function addFromText(t) {
        const lo = minRubInText(normalizeMoneySpaces(t));
        if (lo != null) addRub(lo);
      }

      const walletSelectors = [
        '[class*="price-wallet"]',
        '[class*="walletPrice"]',
        '[class*="wallet-price"]',
        '[class*="PriceWallet"]',
        '[class*="lowerPrice"]',
        '[class*="price--"]',
        '[class*="c-price"]',
        '[class*="_wallet"] [class*="price"]',
        '[class*="wallet"] [class*="price"]',
      ];
      for (const sel of walletSelectors) {
        let w = null;
        try {
          w = root.querySelector(sel);
        } catch {
          w = null;
        }
        if (!w) continue;
        addFromText(w.textContent || '');
      }

      let nodes;
      try {
        nodes = root.querySelectorAll(
          'ins.price, ins[class*="price"], .price, [class*="product-card__price"], [class*="price"]'
        );
      } catch {
        nodes = [];
      }
      nodes.forEach((el) => {
        if (!(el instanceof HTMLElement)) return;
        addFromText(el.textContent || '');
      });

      collectRubFromEmbeddedJson(root, rubs);

      try {
        root.querySelectorAll('[aria-label], [title]').forEach((el) => {
          addFromText(el.getAttribute('aria-label') || '');
          addFromText(el.getAttribute('title') || '');
        });
      } catch {
        /* ignore */
      }

      addFromText(root.innerText || '');

      if (rubs.size > 0) {
        const best = Math.min(...rubs);
        return `${best} ₽`;
      }

      const el =
        root.querySelector('ins.price') ||
        root.querySelector('.price') ||
        root.querySelector('[class*="price"]') ||
        root.querySelector('[class*="product-card__price"]');
      return el ? normalizeMoneySpaces(el.textContent || '') : '';
    }

    function pickMemoryFromText(text) {
      const hay = (text || '').replace(/\s+/g, ' ');
      const m = hay.match(/(\d{2,4})\s*(gb|гб|гиг(?:абайт)?|tb|тб)/i);
      if (m) {
        const n = m[1];
        const u = m[2].toLowerCase();
        return /tb|тб/.test(u) ? `${n} ТБ` : `${n} ГБ`;
      }
      const m2 = hay.match(/\b(64|128|256|512|1024)\s*(gb|гб)?\b/i);
      if (m2) return `${m2[1]} ГБ`;
      return '';
    }

    function pickSpecsBlock(root) {
      const lines = [];
      root.querySelectorAll('li, [class*="param"], [class*="characteristic"], span').forEach((el) => {
        const t = (el.textContent || '').trim().replace(/\s+/g, ' ');
        if (t.length > 6 && t.length < 120 && /гб|gb|памят|экран|камер/i.test(t)) lines.push(t);
      });
      return lines.join(' ');
    }

    function pickBrand(root, title) {
      const el = root.querySelector('[class*="brand"]');
      const b = el?.textContent?.trim();
      if (b && b.length < 80) return b;
      const t = title || '';
      const m = t.match(/^([A-Za-zА-ЯЁа-яё0-9.&-]+)\s*[\/\s]/);
      return m ? m[1] : '';
    }

    const roots = pickCardRoots();
    const out = [];
    const seen = new Set();

    for (const root of roots) {
      const mainA = cardMainLink(root);
      if (!mainA) continue;
      let href = '';
      try {
        href = new URL(mainA.getAttribute('href') || '', base).href.split('#')[0];
      } catch {
        continue;
      }
      if (!href || seen.has(href)) continue;
      seen.add(href);

      const title = pickTitle(root, mainA);
      const priceText = pickPrice(root);
      const nmId = pickNmId(root, href);
      const specs = pickSpecsBlock(root);
      const blockText = (root.textContent || '').replace(/\s+/g, ' ');
      // На WB память может встречаться не только в title/specs, поэтому ищем по всему тексту карточки.
      const memoryLabel = pickMemoryFromText(`${title} ${specs} ${blockText}`);
      const brand = pickBrand(root, title);
      const colorHint = '';

      out.push({
        title,
        priceText,
        nmId,
        href,
        city: '—',
        memoryLabel,
        sellerName: brand || 'Wildberries',
        publishedLabel: '—',
        sellerKind: 'company',
        // На WB рейтинг отключён: не парсим и не фильтруем.
        rating: null,
        reviewsCount: null,
        colorHint,
        // Для устойчивых фильтров (title на WB иногда извлекается неполностью).
        filterText: blockText,
      });
    }

    return out;
  });

  return items.map((it) => ({
    ...it,
    priceNum: parseWbPriceNumberForFilter(it.priceText),
  }));
}

/**
 * @param {Awaited<ReturnType<typeof parseWbListings>>} items
 * @param {object} opts
 * @param {number} opts.minPrice
 * @param {number} opts.maxPrice
 * @param {number} opts.limit
 */
function filterWbListings(items, opts) {
  // Память/цвет — в URL (f4424, f14177449). Цена — здесь (в URL не передаём priceU).
  let out = items.slice();

  const minP = opts.minPrice > 0 ? opts.minPrice : 0;
  const maxP = opts.maxPrice > 0 ? opts.maxPrice : 0;
  if (minP > 0 || maxP > 0) {
    out = out.filter((it) => {
      const p = it.priceNum;
      if (p == null || !Number.isFinite(p)) return false;
      if (minP > 0 && p < minP) return false;
      if (maxP > 0 && p > maxP) return false;
      return true;
    });
  }

  if (opts.limit > 0) out = out.slice(0, opts.limit);
  return out;
}

/**
 * @param {object} params
 * @returns {object}
 */
function wbListingsFilterOpts(params) {
  return {
    minPrice: Number.isFinite(params.minPrice) && params.minPrice > 0 ? Math.floor(params.minPrice) : 0,
    maxPrice: Number.isFinite(params.maxPrice) && params.maxPrice > 0 ? Math.floor(params.maxPrice) : 0,
    limit: 0,
  };
}

/**
 * @param {object} params — поля WB + query, extraKeywords, …
 * @returns {Array<{ Параметр: string, Значение: string }>}
 */
function buildWbSearchParamsExportRows(params) {
  const u = buildWbSearchUrl({
    query: params.query,
    extraKeywords: params.extraKeywords,
    minPrice: params.minPrice,
    maxPrice: params.maxPrice,
    memory: params.memory,
    color: params.color,
    page: 1,
  });
  const memGb = parseWbMemoryGb(params.memory || '');
  const memInUrl =
    memGb != null && memGb in WB_MEMORY_GB_TO_F4424
      ? `${WB_MEMORY_FACET_PARAM}=${WB_MEMORY_GB_TO_F4424[memGb]}, meta_charcs=true`
      : memGb != null
        ? `нет в таблице id (добавьте ${memGb} ГБ в WB_MEMORY_GB_TO_F4424 в parser_wb.js по URL с сайта)`
        : 'не задано';
  const colorRaw = String(params.color || '').trim().toLowerCase();
  const colorInUrl =
    colorRaw && colorRaw in WB_COLOR_TO_F14177449
      ? `${WB_COLOR_FACET_PARAM}=${WB_COLOR_TO_F14177449[colorRaw]}, meta_charcs=true`
      : colorRaw
        ? `нет в таблице id (добавьте "${params.color}" в WB_COLOR_TO_F14177449 в parser_wb.js по URL с сайта)`
        : 'не задано';
  return [
    { Параметр: 'Площадка', Значение: 'Wildberries' },
    { Параметр: 'Сортировка WB в URL', Значение: 'sort=priceup (всегда)' },
    { Параметр: 'URL поиска (как открывал парсер)', Значение: u },
    { Параметр: 'Поисковый запрос', Значение: params.query || '' },
    { Параметр: 'Доп. слова (фильтр по названию)', Значение: (params.extraKeywords || '').trim() || '—' },
    {
      Параметр: 'Мин. цена (фильтр по карточке, не в URL)',
      Значение: Number.isFinite(params.minPrice) && params.minPrice > 0 ? String(params.minPrice) : 'не задано',
    },
    {
      Параметр: 'Макс. цена (фильтр по карточке, не в URL; скролл раньше останавливается при sort=priceup)',
      Значение: Number.isFinite(params.maxPrice) && params.maxPrice > 0 ? String(params.maxPrice) : 'не задано',
    },
    {
      Параметр: 'Память, ГБ (фильтр + в URL при известном id)',
      Значение: String(params.memory || '').trim() || 'любая',
    },
    { Параметр: 'Память в строке запроса WB', Значение: memInUrl },
    {
      Параметр: 'Цвет (фильтр WB в URL при известном id)',
      Значение: String(params.color || '').trim() || 'любой',
    },
    { Параметр: 'Цвет в строке запроса WB', Значение: colorInUrl },
  ];
}

module.exports = {
  buildWbSearchUrl,
  parseWbMemoryGb,
  WB_MEMORY_FACET_PARAM,
  WB_MEMORY_GB_TO_F4424,
  WB_COLOR_FACET_PARAM,
  WB_COLOR_TO_F14177449,
  normalizeWbSearchUrlSingleFeed,
  parseWbSerpPageIndexFromUrl,
  hasWbEmptySerpMessage,
  looksLikeWbSkeletonNoItems,
  parseWbListings,
  filterWbListings,
  wbListingsFilterOpts,
  buildWbSearchParamsExportRows,
};
