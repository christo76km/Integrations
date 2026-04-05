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

    // Lazy load only once
    if (btn.dataset.loaded === "0") {
      const params = new URLSearchParams({
        artist: btn.dataset.artist || "",
        album:  btn.dataset.album || "",
        track:  btn.dataset.track || ""
      });

      const resp = await fetch("/api/plays?" + params.toString());
      const plays = await resp.json();

      const ul = document.createElement("ul");
      ul.className = "plays-list";
      plays.forEach(p => {
        const li = document.createElement("li");
        li.textContent = p.played_at;
        ul.appendChild(li);
      });

      panel.innerHTML = "";
      panel.appendChild(ul);
      btn.dataset.loaded = "1";
    }
  } else {
    panel.setAttribute("hidden", "");
    btn.textContent = btn.dataset.label || "Show";
    btn.setAttribute("aria-expanded", "false");
  }
});