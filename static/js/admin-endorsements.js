// ── Endorsement list filter ────────────────────────────────────────────────
function filterEndorsements() {
  const q = document.getElementById('endo-search').value.toLowerCase();
  const statusVal = document.getElementById('endo-status-filter').value;
  document.querySelectorAll('.endo-row').forEach(row => {
    const nameMatch = !q || row.dataset.name.includes(q);
    const statusMatch = !statusVal || row.dataset.status === statusVal;
    row.style.display = (nameMatch && statusMatch) ? '' : 'none';
  });
}

// ── Alias checkbox machinery ───────────────────────────────────────────────
function rebuildVariantHiddens(checked, canonId) {
  const hiddenContainer = document.getElementById('alias-variant-hiddens');
  hiddenContainer.innerHTML = '';
  checked.filter(cb => cb.dataset.id !== String(canonId)).forEach(cb => {
    const inp = document.createElement('input');
    inp.type = 'hidden';
    inp.name = 'variant_ids';
    inp.value = cb.dataset.id;
    hiddenContainer.appendChild(inp);
  });
}

function updateAliasPanel() {
  const checked = [...document.querySelectorAll('.endo-check:checked')];
  const listEl = document.getElementById('alias-selected-list');
  const canonSelect = document.getElementById('alias-canonical-select');
  const submitBtn = document.getElementById('alias-submit-btn');

  if (checked.length < 2) {
    listEl.textContent = checked.length === 1
      ? `1 selected — check at least one more.`
      : 'No endorsements selected yet.';
    canonSelect.innerHTML = '<option value="">— pick from checked —</option>';
    rebuildVariantHiddens([], null);
    submitBtn.disabled = true;
    return;
  }

  // Rebuild canonical dropdown with checked items
  const prevCanon = canonSelect.value;
  canonSelect.innerHTML = '<option value="">— choose canonical —</option>';
  checked.forEach(cb => {
    const opt = document.createElement('option');
    opt.value = cb.dataset.id;
    opt.textContent = cb.dataset.name;
    canonSelect.appendChild(opt);
  });
  // Restore previous selection if still valid
  if (prevCanon && [...canonSelect.options].some(o => o.value === prevCanon)) {
    canonSelect.value = prevCanon;
  }

  listEl.innerHTML = `<strong>${checked.length} selected:</strong> ` +
    checked.map(cb => `<em>${cb.dataset.name}</em>`).join(', ');

  submitBtn.disabled = false;

  // Eagerly populate variant hiddens with whatever canonical is currently selected
  // (fixes the race where submit fires before onchange).
  rebuildVariantHiddens(checked, canonSelect.value);

  canonSelect.onchange = function() {
    rebuildVariantHiddens(checked, this.value);
  };
}

// Guard on the alias form submit: re-sync hiddens and validate canonical is chosen.
document.addEventListener('DOMContentLoaded', () => {
  const aliasForm = document.querySelector('#alias-form-details form');
  if (aliasForm) {
    aliasForm.addEventListener('submit', function(e) {
      const checked = [...document.querySelectorAll('.endo-check:checked')];
      const canonSelect = document.getElementById('alias-canonical-select');
      if (!canonSelect.value) {
        e.preventDefault();
        alert('Please choose which endorsement is canonical before submitting.');
        return;
      }
      // Final re-sync of hidden inputs in case onchange was skipped
      rebuildVariantHiddens(checked, canonSelect.value);
      if (!document.querySelectorAll('#alias-variant-hiddens input').length) {
        e.preventDefault();
        alert('At least one variant must differ from the canonical.');
      }
    });
  }
});

document.querySelectorAll('.endo-check').forEach(cb =>
  cb.addEventListener('change', updateAliasPanel)
);

// ── Suggestion accept helper ───────────────────────────────────────────────
function sugAccept(radio, canonicalId, variantId) {
  const form = radio.closest('form');
  form.querySelector('.sug-canonical-id').value = canonicalId;
  form.querySelector('.sug-variant-id').value = variantId;
}

// ── Code list filter ───────────────────────────────────────────────────────
function filterCodes() {
  const q = document.getElementById('code-search').value.toLowerCase();
  document.querySelectorAll('.code-row').forEach(row => {
    const match = !q || row.dataset.code.includes(q) || (row.dataset.names || '').includes(q);
    row.style.display = match ? '' : 'none';
  });
}
