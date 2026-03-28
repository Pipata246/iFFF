/**
 * Сбор URL поиска Avito, парсинг карточек, фильтрация, Excel (xlsx).
 */

const XLSX = require('xlsx');
const { cityToSlug, parsePriceNumber, dedupeByHref, resultsPath, logSuccess } = require('./utils');

/**
 * @typedef {Object} SearchParams
 * @property {string} query
 * @property {string} [extraKeywords]
 * @property {string} city
 * @property {number} minPrice
 * @property {number} maxPrice
 * @property {string} sellerType — 'private' | 'company' | 'any'
 */

/**
 * Запрос про iPhone / айфон — открываем раздел «Телефоны» (рабочий путь на Avito).
 * @param {string} query
 */
function isIphoneFamilyQuery(query) {
  const t = (query || '').toLowerCase();
  return t.includes('iphone') || t.includes('айфон');
}

/**
 * Если на карточке нет явной подписи «частное лицо» / «компания», угадываем по строке имени.
 * @param {string} name
 * @returns {'private'|'company'|'unknown'}
 */
function inferSellerKindFromSellerName(name) {
  const n = (name || '').trim();
  if (!n) return 'unknown';
  const low = n.toLowerCase();
  const pad = ` ${low.replace(/[^\dA-Za-zА-ЯЁа-яё]+/g, ' ')} `;

  const legal = ['ооо', 'оао', 'зао', 'пао', 'нко', 'ип', 'чп', 'ао'];
  if (legal.some((w) => pad.includes(` ${w} `))) return 'company';
  if (/[«»"]/.test(n) || /\(.*\)/.test(n)) return 'company';
  if (/\b(ltd\.?|llc|inc\.?|gmbh)\b/i.test(n)) return 'company';

  if (
    /магазин|салон|сервис|центр|опт|склад|поставк|дистриб|холдинг|group|store|shop|market|digital|trade|сеть\s|компани/i.test(
      low
    )
  ) {
    return 'company';
  }

  if (n.length > 55) return 'company';

  const words = n.split(/\s+/).filter(Boolean);
  if (words.length >= 4) return 'company';

  const cyrWord = /^[А-ЯЁA-Zа-яё][а-яё\-']*$/;
  const latWord = /^[A-Za-z][a-z\-']*$/;
  if (
    words.length >= 1 &&
    words.length <= 3 &&
    words.every((w) => cyrWord.test(w) || latWord.test(w))
  ) {
    return 'private';
  }

  return 'unknown';
}

/**
 * Явные метки в тексте карточки важнее догадки по имени.
 * @param {'private'|'company'|'unknown'} badgeKind
 * @param {string} sellerName
 * @returns {'private'|'company'|'unknown'}
 */
function mergeSellerKind(badgeKind, sellerName) {
  if (badgeKind === 'private' || badgeKind === 'company') return badgeKind;
  return inferSellerKindFromSellerName(sellerName);
}

/**
 * По тексту с карточки («3 часа назад», «сегодня», …): считаем «за сегодня» для фильтра.
 * Не опираемся на часовой пояс — только типичные формулировки Avito.
 * @param {string} text
 * @returns {boolean}
 */
function isListingPublishedTodayByCardText(text) {
  const raw = (text || '').trim().toLowerCase();
  if (!raw) return false;

  if (
    /вчера|позавчера|\d+\s+(день|дня|дней|дню)\s+назад|\d+\s+недел|\d+\s+месяц|\d+\s+лет\s+назад/u.test(
      raw
    )
  ) {
    return false;
  }

  const pad = ` ${raw.replace(/[^0-9a-zа-яё]+/gi, ' ')} `;
  if (pad.includes(' сегодня ')) return true;
  if (/только что|несколько\s+секунд/u.test(raw)) return true;
  if (/\d+\s*(минуту|минуты|минут)\s+назад/u.test(raw)) return true;
  if (/\d+\s*(час|часа|часов)\s+назад/u.test(raw)) return true;

  return false;
}

/**
 * Собрать URL поиска: город, цена, q = основной запрос + доп. слова (всё в параметре q).
 * Глубокие пути вида .../apple/iphone_15 без ID каталога (-ASgBAg...) на Avito дают 404.
 * Для iPhone + непустые доп. слова: .../{город}/telefony?q=... — выдача в категории телефонов.
 * @param {SearchParams} p
 * @returns {string}
 */
function buildSearchUrl(p) {
  const slug = cityToSlug(p.city);
  const extraRaw = (p.extraKeywords || '').trim();
  const fullQ = [p.query.trim(), extraRaw].filter(Boolean).join(' ').trim();

  const params = new URLSearchParams();
  params.set('q', fullQ);
  if (Number.isFinite(p.minPrice) && p.minPrice > 0) params.set('pmin', String(Math.floor(p.minPrice)));
  if (Number.isFinite(p.maxPrice) && p.maxPrice > 0) params.set('pmax', String(Math.floor(p.maxPrice)));

  let pathBase = `https://www.avito.ru/${slug}`;
  if (extraRaw && isIphoneFamilyQuery(p.query)) {
    pathBase = `https://www.avito.ru/${slug}/telefony`;
  }

  return `${pathBase}?${params.toString()}`;
}

/**
 * Проверка текста страницы на явную блокировку / капчу Avito.
 * Не используем короткие вроде «капч» / «captcha» — они дают ложные срабатывания в обычной вёрстке.
 * @param {import('playwright').Page} page
 * @returns {Promise<boolean>}
 */
async function detectBlock(page) {
  const body = await page.locator('body').innerText().catch(() => '');
  const low = body.toLowerCase();
  const phrases = [
    'доступ ограничен',
    'доступ временно ограничен',
    'подтвердите что вы человек',
    'подтвердите, что вы не робот',
    'слишком много запросов',
    'доступ с вашего ip временно ограничен',
    'yandex smartcaptcha',
    'smartcaptcha',
    'пройдите проверку',
  ];
  if (phrases.some((p) => low.includes(p))) return true;
  if (/\bкапча\b/i.test(body)) return true;
  return false;
}

/**
 * Явное сообщение Avito о пустой выдаче (не путать с «голым» HTML).
 * @param {import('playwright').Page} page
 */
async function hasAvitoEmptySerpMessage(page) {
  const body = await page.locator('body').innerText().catch(() => '');
  const low = body.toLowerCase();
  return (
    /ничего не найдено|по вашему запросу ничего|объявлений не найдено|подходящих объявлений нет|0\s+объявлен/i.test(
      low
    )
  );
}

/**
 * Выдача открылась без JS/CSS: футер/баннеры есть, карточек [data-marker=item] нет.
 * @param {import('playwright').Page} page
 */
async function looksLikeAvitoSkeletonNoItems(page) {
  return page.evaluate(() => {
    if (document.querySelectorAll('[data-marker="item"]').length > 0) return false;
    const h = (location.hostname || '').toLowerCase();
    if (!h.includes('avito')) return false;
    const t = document.body ? document.body.innerText || '' : '';
    if (t.length < 80) return false;
    const ios = /как дальше пользоваться авито/i.test(t);
    const foot = /для бизнеса/i.test(t) && /карьера в авито/i.test(t);
    const catalogs = /каталоги|#япомогаю/i.test(t);
    const thin = t.length < 5200 && /помощь/i.test(t);
    return ios || foot || (catalogs && t.length < 9000) || thin;
  });
}

/**
 * Спарсить объявления со страницы (несколько вариантов селекторов под вёрстку Avito).
 * @param {import('playwright').Page} page
 * @returns {Promise<Array<{ title: string, priceText: string, priceNum: number|null, href: string, city: string, memoryLabel: string, sellerName: string, publishedLabel: string, sellerKind: string, rating: number|null }>>}
 */
async function parseListings(page) {
  const items = await page.evaluate(() => {
    const base = window.location.origin;

    /**
     * Объявление: /item/ID или SEO-путь ..._1234567890 (без /item/ в URL).
     * @param {string} pathname
     */
    function isListingPathname(pathname) {
      const p = pathname.toLowerCase();
      if (p.includes('/item/')) return true;
      if (
        p.includes('/brands/') ||
        p.includes('/profile/') ||
        p.includes('/account/') ||
        p.includes('/favorites') ||
        p.includes('/comparison')
      ) {
        return false;
      }
      return /_\d{8,}(?:\/|\?|$)/.test(p) || /-\d{8,}(?:\/|\?|$)/.test(p);
    }

    /**
     * @param {HTMLAnchorElement} a
     */
    function hrefFromListingAnchor(a) {
      const raw = a.getAttribute('href') || '';
      if (!raw || raw.startsWith('javascript:') || raw === '#') return '';
      try {
        const u = new URL(raw, base);
        const host = u.hostname.toLowerCase();
        if (host !== 'avito.ru' && !host.endsWith('.avito.ru')) return '';
        if (!isListingPathname(u.pathname)) return '';
        return u.href.split('#')[0];
      } catch {
        return '';
      }
    }

    /**
     * Главная ссылка на объявление внутри карточки.
     * @param {Element} root
     * @returns {HTMLAnchorElement | null}
     */
    function findListingAnchor(root) {
      const prefer =
        root.querySelector('a[data-marker="item-title"]') ||
        root.querySelector('[data-marker="item-title"] a[href]') ||
        root.querySelector('a[data-marker="item-link"]') ||
        root.querySelector('[data-marker="item-line/title"] a[href]');
      if (prefer && 'href' in prefer && hrefFromListingAnchor(/** @type {HTMLAnchorElement} */ (prefer))) {
        return /** @type {HTMLAnchorElement} */ (prefer);
      }
      const links = root.querySelectorAll('a[href]');
      for (const a of links) {
        if (hrefFromListingAnchor(/** @type {HTMLAnchorElement} */ (a))) return /** @type {HTMLAnchorElement} */ (a);
      }
      return null;
    }

    /**
     * @param {Element} root
     * @param {HTMLAnchorElement | null} mainA
     */
    function pickTitle(root, mainA) {
      const el =
        root.querySelector('[data-marker="item-title"]') ||
        root.querySelector('[data-marker="item-line/title"]') ||
        root.querySelector('[itemprop="name"]');
      const fromEl = el?.textContent?.trim();
      if (fromEl) return fromEl;
      if (mainA?.textContent?.trim()) return mainA.textContent.trim();
      const h3a = root.querySelector('h3 a');
      if (h3a?.textContent?.trim()) return h3a.textContent.trim();
      return '';
    }

    /**
     * @param {HTMLAnchorElement | null} mainA
     */
    function pickHref(mainA) {
      return mainA ? hrefFromListingAnchor(mainA) : '';
    }

    /**
     * @param {Element} root
     */
    function pickPrice(root) {
      const el =
        root.querySelector('[data-marker="item-price"]') ||
        root.querySelector('[itemprop="price"]') ||
        root.querySelector('[data-marker*="price"]') ||
        root.querySelector('[class*="price"]');
      return el ? el.textContent?.trim() || '' : '';
    }

    /**
     * @param {Element} root
     */
    function pickGeo(root) {
      const el =
        root.querySelector('[data-marker="item-address"]') ||
        root.querySelector('[data-marker="item-line/geo"]') ||
        root.querySelector('[class*="geo"]');
      return el ? el.textContent?.trim() || '' : '';
    }

    /**
     * Объём памяти из строк параметров карточки или из названия (ГБ/ТБ).
     * @param {Element} root
     * @param {string} title
     */
    function pickMemoryLabel(root, title) {
      const chunks = [];
      root.querySelectorAll('[data-marker*="item-line"]').forEach((el) => {
        const t = (el.textContent || '').trim().replace(/\s+/g, ' ');
        if (t) chunks.push(t);
      });
      const hay = `${chunks.join(' ')} ${title || ''}`.replace(/\s+/g, ' ');
      const m = hay.match(/(\d{2,4})\s*(gb|гб|гиг(?:абайт)?|tb|тб)/i);
      if (m) {
        const n = m[1];
        const u = m[2].toLowerCase();
        return /tb|тб/.test(u) ? `${n} ТБ` : `${n} ГБ`;
      }
      const m2 = (title || '').match(/\b(64|128|256|512|1024)\s*(gb|гб)?\b/i);
      if (m2) {
        if (m2[2] && /gb/i.test(m2[2])) return `${m2[1]} ГБ`;
        return `${m2[1]} ГБ`;
      }
      return '';
    }

    /**
     * Убрать хвост с рейтингом («4,9 · 81 отзыв») и лишние пробелы.
     * @param {string} raw
     */
    function cleanSellerNameLabel(raw) {
      if (!raw) return '';
      let t = raw.replace(/\s+/g, ' ').trim();
      t = t.replace(/^продавец\s+/i, '').trim();
      const m = t.match(/^(.+?)\s+\d+[.,]\d+\s*[·•]/u);
      if (m) t = m[1].trim();
      if (t.length > 100) t = t.slice(0, 100).trim();
      return t;
    }

    /**
     * Имя/название продавца с карточки выдачи (на странице объявления — блок справа; в ленте — своя вёрстка).
     * @param {Element} root
     */
    function pickSellerName(root) {
      const trySelectors = [
        '[data-marker="item-line/seller-name"]',
        '[data-marker*="seller-name"]',
        '[data-marker*="seller-title"]',
        '[data-marker*="seller-info"] a[href]',
        '[data-marker*="seller-info"]',
        '[data-marker="item-seller-info"]',
        '[data-marker*="item-seller"]',
        '[data-marker*="seller"]',
      ];
      for (const sel of trySelectors) {
        const el = root.querySelector(sel);
        if (!el) continue;
        const t = cleanSellerNameLabel(el.textContent || '');
        if (!t || t.length < 2) continue;
        if (/^[\d\s₽]+$/u.test(t)) continue;
        return t;
      }
      const prof = root.querySelector(
        'a[href*="/user/"], a[href*="/brands/"], a[href*="/profile/"], a[href*="/seller/"]'
      );
      if (prof) {
        const t = cleanSellerNameLabel(prof.textContent || '');
        if (t && t.length >= 2 && t.length < 100) return t;
      }
      const skipMarker = /geo|title|price|address|iva|date|metro|delivery/i;
      const lines = root.querySelectorAll('[data-marker^="item-line"]');
      for (const el of lines) {
        const marker = el.getAttribute('data-marker') || '';
        if (skipMarker.test(marker)) continue;
        const t = cleanSellerNameLabel(el.textContent || '');
        if (!t || t.length < 2 || t.length > 90) continue;
        if (/^\d{1,2}\.\d{1,2}\.\d{2,4}$/u.test(t)) continue;
        if (/^\d+\s*(час|дн|мин|сек)|сегодня|вчера/u.test(t)) continue;
        if (/^[\d\s₽·•]+$/u.test(t)) continue;
        return t;
      }
      return '';
    }

    /**
     * Подпись времени публикации («3 часа назад», «сегодня», …).
     * @param {Element} root
     */
    function pickPublishedLabel(root) {
      const hints = /назад|сегодня|вчера|только что|несколько\s+секунд|минут|часов|часа|\bчас\b/i;
      const markers = [
        '[data-marker*="date"]',
        '[data-marker*="time"]',
        '[data-marker*="item-line/date"]',
        '[data-marker*="datetime"]',
        '[data-marker*="published"]',
      ];
      for (const sel of markers) {
        const el = root.querySelector(sel);
        if (!el) continue;
        const t = (el.textContent || '').replace(/\s+/g, ' ').trim();
        if (t && hints.test(t) && t.length < 120) return t;
      }
      const lines = root.querySelectorAll('[data-marker^="item-line"]');
      const candidates = [];
      for (const el of lines) {
        const t = (el.textContent || '').replace(/\s+/g, ' ').trim();
        if (!t || t.length > 120) continue;
        if (!hints.test(t)) continue;
        let score = 0;
        if (/назад/i.test(t)) score += 4;
        if (/сегодня|вчера|только что/i.test(t)) score += 3;
        if (/час|минут/i.test(t)) score += 2;
        candidates.push({ t, score });
      }
      candidates.sort((a, b) => b.score - a.score);
      if (candidates.length) return candidates[0].t;
      const body = (root.textContent || '').replace(/\s+/g, ' ');
      const m = body.match(
        /\d+\s*(?:минуту|минуты|минут|час|часа|часов)\s+назад|сегодня(?:\s+в\s+\d{1,2}[.:]\d{2})?|вчера(?:\s+в\s+\d{1,2}[.:]\d{2})?|только что|несколько секунд/iu
      );
      return m ? m[0].trim().slice(0, 120) : '';
    }

    const roots = Array.from(document.querySelectorAll('[data-marker="item"]'));

    const unique = [];
    const seen = new Set();
    for (const root of roots) {
      const mainA = findListingAnchor(root);
      const href = pickHref(mainA);
      if (!href || seen.has(href)) continue;
      seen.add(href);
      const title = pickTitle(root, mainA);
      const priceText = pickPrice(root);
      const city = pickGeo(root);
      const memoryLabel = pickMemoryLabel(root, title);
      const sellerName = pickSellerName(root);
      const publishedLabel = pickPublishedLabel(root);
      const blockText = (root.textContent || '').toLowerCase();
      let badgeKind = 'unknown';
      if (blockText.includes('частн') || blockText.includes('частное лицо')) badgeKind = 'private';
      else if (blockText.includes('компани') || blockText.includes('магазин')) badgeKind = 'company';

      let rating = null;
      const ratingEl = root.querySelector('[data-marker*="rating"], [class*="rating"]');
      const rt = ratingEl?.textContent || root.textContent || '';
      const rm = rt.match(/(\d+[.,]\d+)/);
      if (rm) {
        const n = parseFloat(rm[1].replace(',', '.'));
        if (Number.isFinite(n) && n >= 0 && n <= 5) rating = n;
      }

      unique.push({
        title,
        priceText,
        href,
        city,
        memoryLabel,
        sellerName,
        publishedLabel,
        badgeKind,
        rating,
      });
    }

    return unique;
  });

  return items.map((it) => {
    const { badgeKind, ...rest } = it;
    return {
      ...rest,
      priceNum: parsePriceNumber(it.priceText),
      sellerKind: mergeSellerKind(badgeKind, it.sellerName),
    };
  });
}

/**
 * Фильтрация: ключевые слова, память, цена, тип продавца, рейтинг.
 * @param {Awaited<ReturnType<typeof parseListings>>} items
 * @param {object} opts
 * @param {string} opts.extraKeywords
 * @param {string} opts.memory
 * @param {number} opts.minPrice
 * @param {number} opts.maxPrice
 * @param {string} opts.sellerType
 * @param {number} opts.minRating
 * @param {boolean} [opts.onlyToday] — только объявления с подписью «сегодня» / N часов(минут) назад
 * @param {number} opts.limit — 0 = не ограничивать количество сохранённых строк
 */
function filterListings(items, opts) {
  const kw = opts.extraKeywords
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .map((k) => k.toLowerCase());
  const mem = String(opts.memory || '').trim();
  const memNorm = mem.replace(/\D/g, '');

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
      const t = it.title || '';
      const re = new RegExp(`\\b${memNorm}\\s*(gb|гб|гиг|tb|тб)?\\b`, 'i');
      return re.test(t) || t.includes(memNorm);
    });
  }

  out = out.filter((it) => {
    const n = it.priceNum;
    if (n == null) return true;
    if (Number.isFinite(opts.minPrice) && opts.minPrice > 0 && n < opts.minPrice) return false;
    if (Number.isFinite(opts.maxPrice) && opts.maxPrice > 0 && n > opts.maxPrice) return false;
    return true;
  });

  if (opts.sellerType === 'private') {
    out = out.filter((it) => it.sellerKind !== 'company');
  } else if (opts.sellerType === 'company') {
    out = out.filter((it) => it.sellerKind !== 'private');
  }

  if (Number.isFinite(opts.minRating) && opts.minRating > 0) {
    out = out.filter((it) => {
      if (it.rating == null) return true; // нет данных — не отсекаем жёстко
      return it.rating >= opts.minRating;
    });
  }

  if (opts.onlyToday) {
    out = out.filter((it) => isListingPublishedTodayByCardText(it.publishedLabel || ''));
  }

  if (opts.limit > 0) out = out.slice(0, opts.limit);
  return out;
}

/**
 * @param {string} kind
 * @returns {string}
 */
function sellerKindToRu(kind) {
  if (kind === 'private') return 'Частное лицо';
  if (kind === 'company') return 'Компания';
  return 'Не определено';
}

/**
 * Строки для листа «Фильтры запуска» (что вводили при старте парсера).
 * @param {SearchParams & { memory?: string, minRating?: number }} params
 * @returns {Array<{ Параметр: string, Значение: string }>}
 */
function buildSearchParamsExportRows(params) {
  const st = params.sellerType || 'any';
  const sellerFilter =
    st === 'private'
      ? 'Только частные лица (private)'
      : st === 'company'
        ? 'Только компании (company)'
        : 'Любой (any)';
  const searchUrl = buildSearchUrl({
    query: params.query,
    extraKeywords: params.extraKeywords,
    city: params.city,
    minPrice: params.minPrice,
    maxPrice: params.maxPrice,
    sellerType: params.sellerType,
  });
  return [
    { Параметр: 'Площадка', Значение: 'Avito' },
    { Параметр: 'URL поиска (как открывал парсер)', Значение: searchUrl },
    { Параметр: 'Поисковый запрос', Значение: params.query || '' },
    { Параметр: 'Доп. слова (фильтр по названию)', Значение: (params.extraKeywords || '').trim() || '—' },
    { Параметр: 'Город', Значение: params.city || '' },
    {
      Параметр: 'Мин. цена (фильтр + в URL)',
      Значение: Number.isFinite(params.minPrice) && params.minPrice > 0 ? String(params.minPrice) : 'не задано',
    },
    {
      Параметр: 'Макс. цена (фильтр + в URL)',
      Значение: Number.isFinite(params.maxPrice) && params.maxPrice > 0 ? String(params.maxPrice) : 'не задано',
    },
    {
      Параметр: 'Память, ГБ (фильтр по названию)',
      Значение: String(params.memory || '').trim() || 'любая',
    },
    { Параметр: 'Тип продавца (фильтр)', Значение: sellerFilter },
    {
      Параметр: 'Мин. рейтинг (фильтр)',
      Значение:
        Number.isFinite(params.minRating) && params.minRating > 0 ? String(params.minRating) : 'не задано',
    },
    {
      Параметр: 'Только «за сегодня» по подписи на карточке',
      Значение: params.onlyToday ? 'да (минуты/часы назад, сегодня, только что)' : 'нет, все даты',
    },
  ];
}

const EXCEL_LIST_HEADERS = [
  'Название',
  'Цена',
  'Память',
  'Имя продавца',
  'Тип продавца',
  'Рейтинг',
  'Опубликовано',
  'Город',
  'Ссылка',
];

/**
 * Сохранить xlsx: лист объявлений (поля с карточки) и при передаче searchParams — лист фильтров запуска.
 * @param {Array<{ title: string, priceText: string, priceNum?: number|null, href: string, city: string, memoryLabel?: string, sellerName?: string, sellerKind?: string, rating?: number|null, publishedLabel?: string, marketplace?: string }>} rows
 * @param {{ checkpoint?: string, searchParams?: SearchParams & { memory?: string, minRating?: number, onlyToday?: boolean }, filterExportRows?: Array<{ Параметр: string, Значение: string }> }} [meta]
 */
function saveToExcel(rows, meta) {
  const m = meta && typeof meta === 'object' ? meta : {};
  const showMarketplace = rows.some((r) => r && typeof r.marketplace === 'string' && r.marketplace !== '');
  const sheetRows = rows.map((r) => {
    const row = {
      Название: r.title,
      Цена: r.priceText || (r.priceNum != null ? String(r.priceNum) : ''),
      Память: r.memoryLabel != null && String(r.memoryLabel).trim() !== '' ? r.memoryLabel : '—',
      'Имя продавца': r.sellerName != null && String(r.sellerName).trim() !== '' ? r.sellerName : '—',
      'Тип продавца': sellerKindToRu(r.sellerKind || 'unknown'),
      Рейтинг: r.rating != null && Number.isFinite(r.rating) ? r.rating : '—',
      Опубликовано:
        r.publishedLabel != null && String(r.publishedLabel).trim() !== '' ? r.publishedLabel : '—',
      Город: r.city,
      Ссылка: r.href,
    };
    if (showMarketplace) {
      row['Площадка'] = r.marketplace && String(r.marketplace).trim() !== '' ? r.marketplace : '—';
    }
    return row;
  });
  const ws = sheetRows.length
    ? XLSX.utils.json_to_sheet(sheetRows)
    : XLSX.utils.aoa_to_sheet([EXCEL_LIST_HEADERS]);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Объявления');
  const filterRows =
    Array.isArray(m.filterExportRows) && m.filterExportRows.length > 0
      ? m.filterExportRows
      : m.searchParams
        ? buildSearchParamsExportRows(m.searchParams)
        : null;
  if (filterRows) {
    const wsFilters = XLSX.utils.json_to_sheet(filterRows);
    XLSX.utils.book_append_sheet(wb, wsFilters, 'Фильтры запуска');
  }
  const fp = resultsPath();
  XLSX.writeFile(wb, fp);
  if (m.checkpoint) {
    logSuccess(`промежуточно — ${m.checkpoint}: ${fp}, строк: ${sheetRows.length}`);
  } else {
    logSuccess(`файл ${fp}, строк: ${sheetRows.length}`);
  }
}

module.exports = {
  buildSearchUrl,
  detectBlock,
  hasAvitoEmptySerpMessage,
  looksLikeAvitoSkeletonNoItems,
  parseListings,
  filterListings,
  saveToExcel,
  buildSearchParamsExportRows,
  isListingPublishedTodayByCardText,
};
