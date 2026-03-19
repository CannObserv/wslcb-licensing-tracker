// Show/hide outcome filter based on section type
document.getElementById('section_type').addEventListener('change', function() {
    const wrapper = document.getElementById('outcome-filter-wrapper');
    const select = document.getElementById('outcome_status');
    if (this.value === 'new_application') {
        wrapper.style.display = '';
    } else {
        wrapper.style.display = 'none';
        select.value = '';
    }
});

document.getElementById('state').addEventListener('change', async function() {
    const state = this.value;
    const wrapper = document.getElementById('city-filter-wrapper');
    const citySelect = document.getElementById('city');

    // Reset city selection
    citySelect.innerHTML = '<option value="">All Cities</option>';

    if (!state) {
        wrapper.style.display = 'none';
        return;
    }

    // Fetch cities for the selected state
    try {
        const resp = await fetch('/api/v1/cities?state=' + encodeURIComponent(state));
        const json = await resp.json();
        for (const c of json.data) {
            const opt = document.createElement('option');
            opt.value = c;
            opt.textContent = c;
            citySelect.appendChild(opt);
        }
    } catch (e) {
        console.error('Failed to load cities:', e);
    }

    wrapper.style.display = '';
});

// ── Multi-select endorsement dropdown ────────────────────────────────────
(function () {
    const toggle = document.getElementById('endorsement-toggle');
    const panel  = document.getElementById('endorsement-panel');
    const label  = document.getElementById('endorsement-label');
    const search = document.getElementById('endorsement-search');
    const clearBtn = document.getElementById('endorsement-clear');
    const substanceSelect = document.getElementById('regulated_substance');

    function getChecked() {
        return Array.from(document.querySelectorAll('input[name="endorsement"]:checked'));
    }

    function updateLabel() {
        const checked = getChecked();
        if (checked.length === 0) {
            label.textContent = 'All';
        } else if (checked.length === 1) {
            label.textContent = checked[0].value;
        } else {
            label.textContent = checked.length + ' selected';
        }
    }

    // Auto-detect substance match and update the substance dropdown.
    function syncSubstanceDropdown() {
        const checked = new Set(getChecked().map(cb => cb.value));
        let matched = '';
        if (checked.size > 0) {
            for (const s of SUBSTANCE_ENDORSEMENTS) {
                const sSet = new Set(s.endorsements);
                if (sSet.size === checked.size && [...checked].every(v => sSet.has(v))) {
                    matched = String(s.id);
                    break;
                }
            }
        }
        substanceSelect.value = matched;
    }

    // Toggle panel open/closed.
    toggle.addEventListener('click', function (e) {
        e.stopPropagation();
        const open = !panel.classList.contains('hidden');
        panel.classList.toggle('hidden', open);
        toggle.setAttribute('aria-expanded', String(!open));
        if (!open) search.focus();
    });

    // Close when clicking outside.
    document.addEventListener('click', function (e) {
        if (!document.getElementById('endorsement-filter').contains(e.target)) {
            panel.classList.add('hidden');
            toggle.setAttribute('aria-expanded', 'false');
        }
    });

    // Filter visible options.
    search.addEventListener('input', function () {
        const q = this.value.toLowerCase();
        document.querySelectorAll('.endorsement-option').forEach(function (el) {
            el.style.display = el.querySelector('span').textContent.toLowerCase().includes(q) ? '' : 'none';
        });
    });

    // Clear button.
    clearBtn.addEventListener('click', function () {
        document.querySelectorAll('input[name="endorsement"]').forEach(cb => cb.checked = false);
        updateLabel();
        substanceSelect.value = '';
    });

    // Update label when any checkbox changes; also sync substance dropdown.
    document.querySelectorAll('input[name="endorsement"]').forEach(function (cb) {
        cb.addEventListener('change', function () {
            updateLabel();
            syncSubstanceDropdown();
        });
    });

    // Regulated Substance shortcut: pre-check matching endorsements.
    substanceSelect.addEventListener('change', function () {
        const substanceId = parseInt(this.value);
        const all = document.querySelectorAll('input[name="endorsement"]');
        all.forEach(cb => cb.checked = false);
        if (substanceId) {
            const substance = SUBSTANCE_ENDORSEMENTS.find(s => s.id === substanceId);
            if (substance) {
                const set = new Set(substance.endorsements);
                all.forEach(cb => { if (set.has(cb.value)) cb.checked = true; });
            }
        }
        updateLabel();
    });

    // Initialise label on page load.
    updateLabel();
    syncSubstanceDropdown();
}());
