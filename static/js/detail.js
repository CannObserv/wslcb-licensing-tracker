/**
 * detail.js — Source viewer toggle logic for the record detail page.
 *
 * Globals: toggleSourceViewer(btn, sid), closeSourceViewer()
 */

/**
 * Toggle the source viewer open or closed.
 *
 * Called via hx-on::before-request on each source badge button.
 * Returns false when the viewer was just closed so the caller can
 * cancel the pending HTMX request with event.preventDefault().
 */
function toggleSourceViewer(btn, sid) {
    var v = document.getElementById('source-viewer');
    if (v.dataset.activeSource === sid) {
        closeSourceViewer();
        return false;
    }
    // Reset aria-pressed on any previously active badge.
    document.querySelectorAll('[data-source-id]').forEach(function (b) {
        b.setAttribute('aria-pressed', 'false');
    });
    btn.setAttribute('aria-pressed', 'true');
    v.dataset.activeSource = sid;
    return true;
}

/**
 * Close the source viewer and clear all toggle state.
 *
 * Called by the ✕ button rendered inside source_viewer.html.
 */
function closeSourceViewer() {
    var v = document.getElementById('source-viewer');
    v.innerHTML = '';
    delete v.dataset.activeSource;
    document.querySelectorAll('[data-source-id]').forEach(function (b) {
        b.setAttribute('aria-pressed', 'false');
    });
}
