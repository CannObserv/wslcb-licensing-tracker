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

/** Live-search the /entities box for `lo`, then `lodge`, as a typist pausing between them. */
async function liveSearchEntities(page) {
  const input = page.doc.querySelector('input[name="q"]');
  await H.swapped(page, 'entities-results', () => H.type(page, input, 'lo'));
  await H.swapped(page, 'entities-results', () => H.type(page, input, 'lodge'));
  return input;
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
});
