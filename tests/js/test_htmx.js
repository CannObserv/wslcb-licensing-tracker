/**
 * test_htmx.js — Behaviour tests that run the vendored htmx 2.0.4 under jsdom (#232).
 *
 * Pages come from tests/js/fixtures/, rendered from the real templates by
 * tests/htmx_fixtures.py (regenerate with `uv run python -m tests.htmx_fixtures`).
 * See htmx_harness.js for the server, the jsdom shims and how a reload is observed.
 */

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const H = require('./htmx_harness');

const ENTITIES_START = '/entities?type=organization&subtype=tribe&sort=name';

const SEARCH_1 = {
  q: 'cafe',
  section_type: 'new_application',
  outcome_status: 'pending',
  application_type: 'NEW APPLICATION',
  state: 'WA',
  city: 'SEATTLE',
  endorsement: ['CANNABIS RETAILER'],
  date_from: '2026-01-01',
  date_to: '2026-06-30',
};
const SEARCH_2 = {
  q: 'bar',
  section_type: 'approved', // hides the outcome filter and clears it
  application_type: 'ASSUMPTION',
  state: 'OR',
  city: 'PORTLAND',
  endorsement: ['BEER/WINE GIFT DELIVERY'],
  date_from: '2026-02-01',
  date_to: '',
};

/** Live-search the /entities box for `lo`, then `lodge`, as a typist pausing between them. */
async function liveSearchEntities(page) {
  const input = page.doc.querySelector('input[name="q"]');
  await H.swapped(page, 'entities-results', () => H.type(page, input, 'lo'));
  await H.swapped(page, 'entities-results', () => H.type(page, input, 'lodge'));
  return input;
}

/** Set every /search control the way a user would, then submit through htmx. */
async function submitSearch(page, want) {
  const doc = page.doc;
  doc.querySelector('input[name="q"]').value = want.q;
  H.choose(page, doc.getElementById('section_type'), want.section_type);
  if (want.outcome_status !== undefined) {
    H.choose(page, doc.getElementById('outcome_status'), want.outcome_status);
  }
  H.choose(page, doc.getElementById('application_type'), want.application_type);
  H.choose(page, doc.getElementById('state'), want.state);
  const city = doc.getElementById('city');
  await H.until(() => [...city.options].some((o) => o.value === want.city), `${want.state} cities`);
  H.choose(page, city, want.city);
  for (const cb of doc.querySelectorAll('input[name="endorsement"]')) {
    if (cb.checked !== want.endorsement.includes(cb.value)) cb.click();
  }
  doc.getElementById('from_date').value = want.date_from;
  doc.getElementById('to_date').value = want.date_to;
  await H.swapped(page, 'results', () => doc.getElementById('search-form').requestSubmit());
}

/**
 * Press Back and return the page the user then sees: the same page if htmx
 * restored a snapshot, or the URL loaded fresh if htmx asked for a reload.
 */
async function pageAfterBack(server, page, opened) {
  const result = await H.back(page);
  if (!result.reloaded) return page;
  const fresh = await H.openPage(server, result.reloaded);
  opened.push(fresh);
  return fresh;
}

describe('htmx under jsdom (#232)', () => {
  let server;
  const opened = [];

  before(async () => {
    server = await H.startServer();
  });

  after(async () => {
    opened.forEach((p) => p.close());
    await server.close();
  });

  it('serves the vendored htmx 2.0.4 and processes the page', async () => {
    const page = await H.openPage(server, ENTITIES_START);
    opened.push(page);
    assert.equal(page.win.htmx.version, '2.0.4');
    assert.deepEqual(page.errors, []);
  });

  describe('/entities live search swaps the filter links out of band (#215)', () => {
    let page;
    let input;

    before(async () => {
      page = await H.openPage(server, ENTITIES_START);
      opened.push(page);
      input = await liveSearchEntities(page);
    });

    it('sends the hidden filters with each keystroke pause', () => {
      for (const q of ['lo', 'lodge']) {
        const want = server.routeKey('/entities', `?q=${q}&type=organization&subtype=tribe&sort=name`, true);
        assert.ok(server.requests.includes(want), `missing request ${want}`);
      }
      assert.deepEqual(server.misses, []);
    });

    it('pushes the typed query into the URL', () => {
      assert.equal(page.win.location.search, '?q=lodge&type=organization&subtype=tribe&sort=name');
    });

    it('gives every filter link the typed q', () => {
      const links = page.doc.querySelectorAll('#filter-form a, #entities-sort a, #entities-kinds a');
      assert.ok(links.length >= 13, `expected the type, sort and kind links, got ${links.length}`);
      for (const a of links) {
        assert.equal(new URL(a.href).searchParams.get('q'), 'lodge', a.getAttribute('href'));
      }
    });

    it('keeps the hidden filter inputs', () => {
      assert.deepEqual(H.controlState(page.doc.getElementById('filter-form')), {
        type: 'organization',
        subtype: 'tribe',
        sort: 'name',
      });
    });

    it('swaps each region in place: one of each id, no leftover hx-swap-oob', () => {
      for (const id of ['filter-form', 'entities-sort', 'entities-kinds']) {
        assert.equal(page.doc.querySelectorAll(`#${id}`).length, 1, id);
      }
      assert.equal(page.doc.querySelectorAll('[hx-swap-oob]').length, 0);
    });

    it('never replaces the search box: same node, focus and caret kept', () => {
      assert.equal(page.doc.querySelector('input[name="q"]'), input);
      assert.equal(page.doc.activeElement, input);
      assert.equal(input.selectionStart, 5);
      assert.equal(input.value, 'lodge');
    });

    it('shows the results for the typed query', () => {
      const names = [...page.doc.querySelectorAll('#entities-results a[href^="/entity/"]')]
        .map((a) => a.textContent.trim());
      assert.deepEqual(names, ['LODGE HALL TRIBE']);
      assert.deepEqual(page.errors, []);
    });
  });

  describe('Back shows the URL\'s query in every control (#231)', () => {
    it('/entities: the search box matches q on each Back', async () => {
      const page = await H.openPage(server, ENTITIES_START);
      opened.push(page);
      await liveSearchEntities(page);

      for (const want of ['lo', '']) {
        const shown = await pageAfterBack(server, page, opened);
        assert.equal(new URLSearchParams(shown.win.location.search).get('q') ?? '', want);
        const state = H.controlState(shown.doc);
        assert.deepEqual(state, H.urlState(shown.win.location.search, state));
        assert.deepEqual(shown.errors, []);
      }
    });

    it('/search: every filter, and what it drives, matches the URL on each Back', async () => {
      const page = await H.openPage(server, '/search');
      opened.push(page);
      await submitSearch(page, SEARCH_1);
      await submitSearch(page, SEARCH_2);

      for (const want of [SEARCH_1, null]) {
        const shown = await pageAfterBack(server, page, opened);
        const doc = shown.doc;
        const search = shown.win.location.search;
        assert.equal(new URLSearchParams(search).get('q') ?? '', want ? want.q : '');

        const state = H.controlState(doc.getElementById('search-form'));
        assert.deepEqual(state, H.urlState(search, state));

        // The results are the URL's: their export link echoes the same filters.
        const exp = doc.querySelector('#results a[href^="/api/v1/export"]');
        const expState = H.urlState(new URL(exp.href).search, state);
        assert.deepEqual(expState, H.urlState(search, state));

        // UI that search.js derives from the controls.
        const sectionNew = state.section_type === 'new_application';
        assert.equal(doc.getElementById('outcome-filter-wrapper').style.display, sectionNew ? '' : 'none');
        assert.equal(doc.getElementById('city-filter-wrapper').style.display, state.state ? '' : 'none');
        if (want) {
          const cities = [...doc.getElementById('city').options].map((o) => o.value).filter(Boolean);
          assert.deepEqual(cities, ['SEATTLE', 'SPOKANE']);
        }
        const label = doc.getElementById('endorsement-label').textContent;
        assert.equal(label, want ? want.endorsement[0] : 'All');
        assert.deepEqual(shown.errors, []);
      }
    });

    it('never restores a cached snapshot, and caches none', async () => {
      const page = await H.openPage(server, ENTITIES_START);
      opened.push(page);
      await liveSearchEntities(page);
      await H.back(page);
      await H.back(page);
      // A restore re-runs every <script> in <body> (#231): it would redeclare
      // search.html's const and stack document listeners. A reload can't.
      assert.deepEqual(page.restores, []);
      assert.equal(page.reloads.length, 2);
      assert.equal(page.win.localStorage.getItem('htmx-history-cache'), null);
    });
  });
});
