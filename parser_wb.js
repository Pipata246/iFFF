/**
 * Wildberries: URL поиска, парсинг карточек выдачи, фильтрация.
 * Вёрстка WB часто меняется — селекторы с запасными вариантами внутри page.evaluate.
 */

const { parsePriceNumber } = require('./utils');

/**
 * @typedef {Object} WbSearchParams
 * @property {string} query
 * @property {string} [extraKeywords]
 * @property {string} [memory] — строка из консоли (например 128), для URL f4424
 * @property {number} minPrice
 * @property {number} maxPrice
 * @property {number} [page] — 1-based для пагинации в URL
 */

/**
 * Фильтр «встроенная память» в поиске WB: в URL добавляются `f4424=<id>` и `meta_charcs=true`.
 * Пример (пользователь): 128 ГБ → f4424=25425.
 * Другие объёмы: на wildberries.ru выберите фильтр памяти, скопируйте число после f4424 из адреса и добавьте строку в объект ниже.
 */
const WB_MEMORY_FACET_PARAM = 'f4424';
const WB_MEMORY_GB_TO_F4424 = {
  128: 25425,
};

/**
 * @param {string} memoryRaw
 * @returns {number|null} объём в ГБ для карты WB_MEMORY_GB_TO_F4424
 */
function parseWbMemoryGb(memoryRaw) {
  const s = String(memoryRaw || '')
    .trim()
    .replace(/\s+/g, '')
    .replace(/gb|гб/gi, '');
  const digits = s.replace(/\D/g, '');
  if (!digits) return null;
  const n = parseInt(digits, 10);
  return Number.isFinite(n) && n > 0 ? n : null;
}

/**
 * На WB: priceU (копейки), sort=popular, page, при памяти — f4424 + meta_charcs=true.
 * Если сайт проигнорирует часть параметров, filterWbListings всё равно подрежет по названию.
 * @param {WbSearchParams} p
 * @returns {string}
 */
function buildWbSearchUrl(p) {
  const extraRaw = (p.extraKeywords || '').trim();
  const fullQ = [String(p.query || '').trim(), extraRaw].filter(Boolean).join(' ').trim();
  const u = new URL('https://www.wildberries.ru/catalog/0/search.aspx');

  const pageNum = Number.isFinite(p.page) && p.page >= 1 ? Math.floor(p.page) : 1;
  u.searchParams.set('page', String(pageNum));
  u.searchParams.set('sort', 'popular');
  u.searchParams.set('search', fullQ);

  const minRub = Number.isFinite(p.minPrice) && p.minPrice > 0 ? Math.floor(p.minPrice) : 0;
  const maxRub = Number.isFinite(p.maxPrice) && p.maxPrice > 0 ? Math.floor(p.maxPrice) : 0;
  if (minRub > 0 || maxRub > 0) {
    const minK = minRub > 0 ? minRub * 100 : 0;
    const maxK = maxRub > 0 ? maxRub * 100 : 999_999_999;
    u.searchParams.set('priceU', `${minK};${maxK}`);
  }

  const memGb = parseWbMemoryGb(p.memory || '');
  if (memGb != null && memGb in WB_MEMORY_GB_TO_F4424) {
    const v = WB_MEMORY_GB_TO_F4424[memGb];
    u.searchParams.set(WB_MEMORY_FACET_PARAM, String(v));
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

    function pickPrice(root) {
      const el =
        root.querySelector('ins.price') ||
        root.querySelector('.price') ||
        root.querySelector('[class*="price"]') ||
        root.querySelector('[class*="product-card__price"]');
      return el ? (el.textContent || '').trim().replace(/\s+/g, ' ') : '';
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

    function parseRatingBlock(text) {
      const raw = (text || '').replace(/\s+/g, ' ');
      const m = raw.match(/(\d+[.,]\d+)\s*[·•]\s*(\d+)\s*оцен/i);
      if (m) {
        const r = parseFloat(m[1].replace(',', '.'));
        const c = parseInt(m[2], 10);
        return {
          rating: Number.isFinite(r) && r >= 0 && r <= 5 ? r : null,
          reviews: Number.isFinite(c) ? c : null,
        };
      }
      const m2 = raw.match(/(\d+[.,]\d+)/);
      if (m2) {
        const r = parseFloat(m2[1].replace(',', '.'));
        return { rating: Number.isFinite(r) && r >= 0 && r <= 5 ? r : null, reviews: null };
      }
      return { rating: null, reviews: null };
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
      const specs = pickSpecsBlock(root);
      const memoryLabel = pickMemoryFromText(`${title} ${specs}`);
      const blockText = (root.textContent || '').replace(/\s+/g, ' ');
      const { rating, reviews } = parseRatingBlock(blockText);
      const brand = pickBrand(root, title);
      const colorHint = '';

      out.push({
        title,
        priceText,
        href,
        city: '—',
        memoryLabel,
        sellerName: brand || 'Wildberries',
        publishedLabel: '—',
        sellerKind: 'company',
        rating,
        reviewsCount: reviews,
        colorHint,
      });
    }

    return out;
  });

  return items.map((it) => ({
    ...it,
    priceNum: parsePriceNumber(it.priceText),
  }));
}

/**
 * @param {Awaited<ReturnType<typeof parseWbListings>>} items
 * @param {object} opts
 * @param {string} opts.extraKeywords
 * @param {string} opts.memory
 * @param {string} opts.color — подстрока в названии (рус/лат)
 * @param {number} opts.minPrice
 * @param {number} opts.maxPrice
 * @param {'any'|'with'|'without'} opts.ratingMode
 * @param {number} opts.limit
 */
function filterWbListings(items, opts) {
  const kw = String(opts.extraKeywords || '')
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .map((k) => k.toLowerCase());
  const mem = String(opts.memory || '').trim();
  const memNorm = mem.replace(/\D/g, '');
  const colorRaw = String(opts.color || '').trim().toLowerCase();

  let out = items.slice();

  out = out.filter((it) => {
    const t = (it.title || '').toLowerCase();
    for (const k of kw) {
      if (!t.includes(k)) return false;
    }
    return true;
  });

  if (memNorm) {
    out = out.filter((it) => {
      const t = `${it.title || ''} ${it.memoryLabel || ''}`;
      const re = new RegExp(`\\b${memNorm}\\s*(gb|гб|гиг|tb|тб)?\\b`, 'i');
      return re.test(t) || t.includes(memNorm);
    });
  }

  if (colorRaw) {
    out = out.filter((it) => (it.title || '').toLowerCase().includes(colorRaw));
  }

  out = out.filter((it) => {
    const n = it.priceNum;
    if (n == null) return true;
    if (Number.isFinite(opts.minPrice) && opts.minPrice > 0 && n < opts.minPrice) return false;
    if (Number.isFinite(opts.maxPrice) && opts.maxPrice > 0 && n > opts.maxPrice) return false;
    return true;
  });

  const mode = opts.ratingMode || 'any';
  if (mode === 'with') {
    out = out.filter((it) => it.rating != null && Number.isFinite(it.rating));
  } else if (mode === 'without') {
    out = out.filter((it) => it.rating == null || !Number.isFinite(it.rating));
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
    extraKeywords: params.extraKeywords || '',
    memory: params.memory || '',
    color: params.color || '',
    minPrice: params.minPrice,
    maxPrice: params.maxPrice,
    ratingMode: params.wbRatingMode || 'any',
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
    page: 1,
  });
  const memGb = parseWbMemoryGb(params.memory || '');
  const memInUrl =
    memGb != null && memGb in WB_MEMORY_GB_TO_F4424
      ? `${WB_MEMORY_FACET_PARAM}=${WB_MEMORY_GB_TO_F4424[memGb]}, meta_charcs=true`
      : memGb != null
        ? `нет в таблице id (добавьте ${memGb} ГБ в WB_MEMORY_GB_TO_F4424 в parser_wb.js по URL с сайта)`
        : 'не задано';
  const rm = params.wbRatingMode || 'any';
  const ratingLabel =
    rm === 'with' ? 'Только с рейтингом на карточке' : rm === 'without' ? 'Только без рейтинга' : 'Не важно';
  return [
    { Параметр: 'Площадка', Значение: 'Wildberries' },
    { Параметр: 'URL поиска (как открывал парсер)', Значение: u },
    { Параметр: 'Поисковый запрос', Значение: params.query || '' },
    { Параметр: 'Доп. слова (фильтр по названию)', Значение: (params.extraKeywords || '').trim() || '—' },
    {
      Параметр: 'Мин. цена (фильтр + в URL priceU)',
      Значение: Number.isFinite(params.minPrice) && params.minPrice > 0 ? String(params.minPrice) : 'не задано',
    },
    {
      Параметр: 'Макс. цена (фильтр + в URL priceU)',
      Значение: Number.isFinite(params.maxPrice) && params.maxPrice > 0 ? String(params.maxPrice) : 'не задано',
    },
    {
      Параметр: 'Память, ГБ (фильтр + в URL при известном id)',
      Значение: String(params.memory || '').trim() || 'любая',
    },
    { Параметр: 'Память в строке запроса WB', Значение: memInUrl },
    {
      Параметр: 'Цвет (подстрока в названии)',
      Значение: String(params.color || '').trim() || 'любой',
    },
    { Параметр: 'Рейтинг на карточке', Значение: ratingLabel },
  ];
}

module.exports = {
  buildWbSearchUrl,
  parseWbMemoryGb,
  WB_MEMORY_FACET_PARAM,
  WB_MEMORY_GB_TO_F4424,
  parseWbSerpPageIndexFromUrl,
  hasWbEmptySerpMessage,
  looksLikeWbSkeletonNoItems,
  parseWbListings,
  filterWbListings,
  wbListingsFilterOpts,
  buildWbSearchParamsExportRows,
};
