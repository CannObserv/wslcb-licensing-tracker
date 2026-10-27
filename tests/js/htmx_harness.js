/**
 * htmx_harness.js — Run the vendored htmx in jsdom against real rendered pages (#232).
 *
 * A local HTTP server serves the fixtures that tests/htmx_fixtures.py renders
 * from the templates (full pages, HX-Request partials, API JSON) plus the
 * repo's static/. jsdom loads a page with its own <script> tags, so htmx,
 * search.js and base.html's inline script all run as they would in a browser.
 *
 * jsdom has no cross-document navigation: location.reload() is reported as a
 * "Not implemented: navigation" error. The harness records it as a reload of
 * the current URL; a test then opens that URL in a fresh page to stand in for it.
 */

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { JSDOM, VirtualConsole } = require('jsdom');

const ROOT = path.resolve(__dirname, '../..');
const STATIC = path.join(ROOT, 'static');
const FIXTURES = path.join(__dirname, 'fixtures');
const MIME = { '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml' };

/** Order-insensitive request key: htmx and the fixtures may order params differently. */
function routeKey(pathname, search, hx) {
  const params = [...new URLSearchParams(search)].map(([k, v]) => `${k}=${v}`).sort();
  return `${hx ? 'HX ' : ''}${pathname}?${params.join('&')}`;
}

/**
 * Serve the fixtures and static/. Unknown routes answer 404 and are listed in
 * `misses`, so a test that wanders off the rendered set fails loudly.
 */
async function startServer() {
  const manifest = JSON.parse(fs.readFileSync(path.join(FIXTURES, 'manifest.json'), 'utf-8'));
  const routes = new Map(manifest.map((m) => {
    const u = new URL(m.url, 'http://fixture');
    return [routeKey(u.pathname, u.search, m.hx), m];
  }));
  const requests = [];
  const misses = [];

  const server = http.createServer((req, res) => {
    const u = new URL(req.url, 'http://fixture');
    if (u.pathname.startsWith('/static/')) {
      const file = path.join(STATIC, u.pathname.slice('/static/'.length));
      if (!file.startsWith(STATIC + path.sep) || !fs.existsSync(file)) {
        misses.push(u.pathname);
        res.writeHead(404).end();
        return;
      }
      res.writeHead(200, { 'content-type': MIME[path.extname(file)] || 'application/octet-stream' });
      res.end(fs.readFileSync(file));
      return;
    }
    const key = routeKey(u.pathname, u.search, req.headers['hx-request'] === 'true');
    requests.push(key);
    const m = routes.get(key);
    if (!m) {
      misses.push(key);
      res.writeHead(404, { 'content-type': 'text/plain' }).end(`no fixture for ${key}`);
      return;
    }
    res.writeHead(200, { 'content-type': m.content_type });
    res.end(fs.readFileSync(path.join(FIXTURES, m.file)));
  });

  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  return {
    base: `http://127.0.0.1:${server.address().port}`,
    requests,
    misses,
    routeKey,
    close() {
      server.closeAllConnections();
      return new Promise((resolve) => server.close(resolve));
    },
  };
}

/**
 * jsdom 24's XPathExpression.evaluate requires a result type; htmx 2.0.4's
 * hx-on scan omits it (browsers default to ANY_TYPE). Without this, htmx's
 * history restore throws in jsdom.
 */
function shimXPath(win) {
  const create = win.XPathEvaluator.prototype.createExpression;
  win.XPathEvaluator.prototype.createExpression = function (...args) {
    const expr = create.apply(this, args);
    const evaluate = expr.evaluate;
    expr.evaluate = function (node, type = 0, result = null) {
      return evaluate.call(this, node, type, result);
    };
    return expr;
  };
}

/**
 * jsdom 24 has no fetch; search.js loads cities with it. Lend the window
 * node's own, resolving relative URLs against the page.
 */
function shimFetch(win) {
  win.fetch = (input, init) => fetch(new URL(String(input), win.location.href), init);
}

function beforeParse(win) {
  shimXPath(win);
  shimFetch(win);
}

/** Resolve once `test()` holds, polling; reject after `ms`. */
async function until(test, what, ms = 3000) {
  const end = Date.now() + ms;
  while (!test()) {
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

/**
 * Load `url` from the server with scripts and subresources, and wait for the
 * window's load event (external scripts have run).
 *
 * The returned page collects uncaught script errors and console.error output
 * in `errors`, reload requests in `reloads`, and every htmx history restore
 * in `restores`.
 */
async function openPage(server, url) {
  const errors = [];
  const reloads = [];
  const restores = [];
  let dom;
  const virtualConsole = new VirtualConsole();
  virtualConsole.on('jsdomError', (err) => {
    if (/^Not implemented: navigation/.test(err.message)) {
      reloads.push(dom.window.location.pathname + dom.window.location.search);
    } else {
      errors.push(err.message);
    }
  });
  virtualConsole.on('error', (...args) => errors.push(args.map(String).join(' ')));

  dom = await JSDOM.fromURL(server.base + url, {
    runScripts: 'dangerously',
    resources: 'usable',
    pretendToBeVisual: true,
    virtualConsole,
    beforeParse,
  });
  const win = dom.window;
  if (win.document.readyState !== 'complete') {
    await new Promise((resolve) => win.addEventListener('load', resolve, { once: true }));
  }
  win.document.body.addEventListener('htmx:historyRestore', (e) => restores.push(e.detail.path));
  return {
    server,
    win,
    doc: win.document,
    errors,
    reloads,
    restores,
    close: () => win.close(),
  };
}

/**
 * Run `action`, then wait until htmx has swapped and settled `#targetId`
 * (OOB swaps settle in the same pass). A failed request (e.g. a 404 for a
 * URL with no fixture) throws at once instead of timing out.
 */
async function swapped(page, targetId, action, ms = 3000) {
  let done = false;
  let failed = null;
  const onSettle = (e) => { if (e.target.id === targetId) done = true; };
  const onError = (e) => { failed = `${e.type}: ${e.detail.error || ''}`; };
  const body = page.doc.body;
  body.addEventListener('htmx:afterSettle', onSettle);
  body.addEventListener('htmx:responseError', onError);
  body.addEventListener('htmx:sendError', onError);
  try {
    action();
    await until(() => done || failed, `htmx to settle #${targetId}`, ms);
    if (failed) throw new Error(`${failed} (server misses: ${JSON.stringify(page.server.misses)})`);
  } finally {
    body.removeEventListener('htmx:afterSettle', onSettle);
    body.removeEventListener('htmx:responseError', onError);
    body.removeEventListener('htmx:sendError', onError);
  }
}

/** Type `text` into `input` as a user would: value, caret at the end, an input event. */
function type(page, input, text) {
  input.focus();
  input.value = text;
  input.setSelectionRange(text.length, text.length);
  input.dispatchEvent(new page.win.Event('input', { bubbles: true }));
}

/** Set a control's value and fire `change`, as picking from a <select> does. */
function choose(page, control, value) {
  control.value = value;
  control.dispatchEvent(new page.win.Event('change', { bubbles: true }));
}

/**
 * Press Back. Resolves with `{restored}` (htmx put back a cached snapshot) or
 * `{reloaded}` (htmx asked for a full reload: the URL to load fresh).
 */
async function back(page) {
  const restores = page.restores.length;
  const reloads = page.reloads.length;
  page.win.history.back();
  await until(
    () => page.restores.length > restores || page.reloads.length > reloads,
    'Back to restore or reload',
  );
  if (page.reloads.length > reloads) return { reloaded: page.reloads[page.reloads.length - 1] };
  return { restored: page.restores[page.restores.length - 1] };
}

/**
 * What the named controls under `root` show: text-like inputs and selects by
 * value, checkboxes as the list of checked values.
 */
function controlState(root) {
  const state = {};
  for (const el of root.querySelectorAll('input[name], select[name]')) {
    if (el.type === 'checkbox') {
      state[el.name] = state[el.name] || [];
      if (el.checked) state[el.name].push(el.value);
    } else {
      state[el.name] = el.value;
    }
  }
  return state;
}

/** The same shape as controlState, read from a URL's query (absent = empty). */
function urlState(search, like) {
  const params = new URLSearchParams(search);
  const state = {};
  for (const [name, shown] of Object.entries(like)) {
    state[name] = Array.isArray(shown) ? params.getAll(name) : (params.get(name) ?? '');
  }
  return state;
}

module.exports = {
  startServer,
  openPage,
  swapped,
  type,
  choose,
  back,
  until,
  controlState,
  urlState,
};
