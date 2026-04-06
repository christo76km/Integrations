async function loadPlays(btn, mode) {
  const panel = document.getElementById(btn.dataset.target);
  if (!panel) return;

  panel.innerHTML = `<div class="plays-loading">Loading…</div>`;

  const params = new URLSearchParams({
    artist: btn.dataset.artist || "",
    album:  btn.dataset.album || "",
    track:  btn.dataset.track || "",
    tz:     btn.dataset.tz || "local",
    mode:   mode
  });

  // include date filters only if present (server ignores if mode=all anyway)
  if (btn.dataset.dateFrom) params.set("date_from", btn.dataset.dateFrom);
  if (btn.dataset.dateTo)   params.set("date_to", btn.dataset.dateTo);

  const resp = await fetch("/api/plays?" + params.toString());
  const data = await resp.json();

  const hasDate = btn.dataset.hasDate === "1";

  // Controls (only when date filter exists)
  let controls = "";
  if (hasDate) {
    controls = `
      <div class="plays-controls">
        <button type="button" class="plays-mode" data-mode="filtered">Filtered</button>
        <button type="button" class="plays-mode" data-mode="all">All history</button>
        <span class="plays-meta">Showing: <b>${data.mode}</b> (${data.count})</span>
      </div>
    `;
  }

  const ul = document.createElement("ul");
  ul.className = "plays-list";
  data.plays.forEach(p => {
    const li = document.createElement("li");
    li.textContent = p.played_at;
    ul.appendChild(li);
  });

  panel.innerHTML = controls;
  panel.appendChild(ul);

  // Store current mode so we don't re-fetch unnecessarily
  btn.dataset.mode = data.mode;
  btn.dataset.loaded = "1";
}

document.addEventListener("dblclick", function (e) {
    if (!e.target.classList.contains("editable")) return;

    const cell = e.target;
    const oldValue = cell.innerText.trim();
    const scrobbleId = cell.dataset.id;
    const recordType = cell.dataset.type;

    const scope = {
        artist: cell.dataset.artist || "",
        album: cell.dataset.album || "",
        track: cell.dataset.track || ""
    };

    const newValue = prompt(`Update ${recordType}:`, oldValue);
    if (!newValue || newValue === oldValue) return;

    const applyAll = confirm("Apply this change to ALL matching entries in this group?");

    if (applyAll) {
        fetch("/api/bulk_update", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({
                record_type: recordType,
                updated_value: newValue,
                scope: scope
            })
        })
            .then(r => r.json())
            .then(resp => {
                alert(`Updated ${resp.inserted} entries.`);
                // Important: groups may merge/split after rename -> reload
                window.location.reload();
            })
            .catch(err => {
                alert("Bulk update failed");
                console.error(err);
            });

    } else {
        fetch("/api/update", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({
                scrobble_id: scrobbleId,
                record_type: recordType,
                updated_value: newValue
            })
        })
            .then(r => {
                if (!r.ok) throw new Error("Request failed");
                cell.innerText = newValue;
            })
            .catch(err => {
                alert("Update failed");
                console.error(err);
            });
    }
});

document.addEventListener("click", async function (e) {
  const btn = e.target.closest(".toggle-plays");
  if (!btn) return;

  const panel = document.getElementById(btn.dataset.target);
  if (!panel) return;

  const isHidden = panel.hasAttribute("hidden");

  if (isHidden) {
    panel.removeAttribute("hidden");
    btn.textContent = "Hide";
    btn.setAttribute("aria-expanded", "true");

    const hasDate = btn.dataset.hasDate === "1";
    const defaultMode = hasDate ? "filtered" : "all";

    // load if never loaded or if mode not set yet
    if (btn.dataset.loaded !== "1" || !btn.dataset.mode) {
      await loadPlays(btn, defaultMode);
    }
  } else {
    panel.setAttribute("hidden", "");
    btn.textContent = btn.dataset.label || "Show";
    btn.setAttribute("aria-expanded", "false");
  }
});

document.addEventListener("click", async function (e) {
  const modeBtn = e.target.closest(".plays-mode");
  if (!modeBtn) return;

  const panel = modeBtn.closest(".plays-panel");
  if (!panel) return;

  const btnId = panel.dataset.btn;
  const ownerBtn = document.getElementById(btnId);
  if (!ownerBtn) return;

  const newMode = modeBtn.dataset.mode;

  // Avoid reloading if already in that mode
  if (ownerBtn.dataset.mode === newMode) return;

  await loadPlays(ownerBtn, newMode);
});