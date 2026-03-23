/**
 * test_detail.js — Unit tests for static/js/detail.js (source viewer toggle).
 *
 * Uses Node built-in test runner + jsdom for lightweight DOM mocking.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const { JSDOM } = require('jsdom');
const fs = require('node:fs');
const path = require('node:path');

const detailSrc = fs.readFileSync(
  path.resolve(__dirname, '../../static/js/detail.js'),
  'utf-8'
);

/**
 * Build a minimal DOM with a source-viewer div and N badge buttons,
 * then evaluate detail.js in that window context.
 */
function setup(badgeCount = 2) {
  const badges = Array.from({ length: badgeCount }, (_, i) =>
    `<button data-source-id="s${i + 1}" aria-pressed="false">Source ${i + 1}</button>`
  ).join('\n');

  const html = `<!DOCTYPE html><html><body>
    <div id="source-viewer"></div>
    ${badges}
  </body></html>`;

  const dom = new JSDOM(html, { runScripts: 'dangerously' });
  const scriptEl = dom.window.document.createElement('script');
  scriptEl.textContent = detailSrc;
  dom.window.document.body.appendChild(scriptEl);
  return dom.window;
}

describe('toggleSourceViewer', () => {
  let win;

  beforeEach(() => {
    win = setup(2);
  });

  it('opens viewer and sets aria-pressed="true" on the clicked badge', () => {
    const btn = win.document.querySelector('[data-source-id="s1"]');
    const result = win.toggleSourceViewer(btn, 's1');

    assert.equal(result, true);
    assert.equal(btn.getAttribute('aria-pressed'), 'true');
    assert.equal(
      win.document.getElementById('source-viewer').dataset.activeSource,
      's1'
    );
  });

  it('closes viewer when clicking the same open badge', () => {
    const btn = win.document.querySelector('[data-source-id="s1"]');
    // Open first
    win.toggleSourceViewer(btn, 's1');
    // Click again — should close
    const result = win.toggleSourceViewer(btn, 's1');

    assert.equal(result, false);
    assert.equal(btn.getAttribute('aria-pressed'), 'false');
    assert.equal(
      win.document.getElementById('source-viewer').dataset.activeSource,
      undefined
    );
  });

  it('switches to a different badge when one is already open', () => {
    const btn1 = win.document.querySelector('[data-source-id="s1"]');
    const btn2 = win.document.querySelector('[data-source-id="s2"]');

    win.toggleSourceViewer(btn1, 's1');
    const result = win.toggleSourceViewer(btn2, 's2');

    assert.equal(result, true);
    // Old badge reset
    assert.equal(btn1.getAttribute('aria-pressed'), 'false');
    // New badge active
    assert.equal(btn2.getAttribute('aria-pressed'), 'true');
    assert.equal(
      win.document.getElementById('source-viewer').dataset.activeSource,
      's2'
    );
  });
});

describe('closeSourceViewer', () => {
  let win;

  beforeEach(() => {
    win = setup(2);
  });

  it('clears data-active-source and resets all aria-pressed', () => {
    const btn1 = win.document.querySelector('[data-source-id="s1"]');
    const btn2 = win.document.querySelector('[data-source-id="s2"]');
    const viewer = win.document.getElementById('source-viewer');

    // Open a badge
    win.toggleSourceViewer(btn1, 's1');
    // Manually set btn2 aria-pressed to simulate stale state
    btn2.setAttribute('aria-pressed', 'true');

    win.closeSourceViewer();

    assert.equal(viewer.dataset.activeSource, undefined);
    assert.equal(viewer.innerHTML, '');
    assert.equal(btn1.getAttribute('aria-pressed'), 'false');
    assert.equal(btn2.getAttribute('aria-pressed'), 'false');
  });

  it('is safe to call when no viewer is active', () => {
    const viewer = win.document.getElementById('source-viewer');
    // Should not throw
    win.closeSourceViewer();

    assert.equal(viewer.dataset.activeSource, undefined);
    assert.equal(viewer.innerHTML, '');
  });
});
