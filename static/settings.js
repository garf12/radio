// --- Utilities ---

function escapeHtml(str) {
    const div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
}

// --- Streams ---

async function loadStreamsSettings() {
    const container = document.getElementById("streams-list");
    if (!container) return;
    try {
        const resp = await fetch("/api/streams");
        const data = await resp.json();
        const streams = data.streams || [];
        if (streams.length === 0) {
            container.innerHTML = '<div class="empty-state">No streams configured</div>';
            return;
        }
        container.innerHTML = streams.map(s => `
            <div class="stream-row" data-id="${s.id}">
                <span class="stream-color-dot" style="background:${s.color}"></span>
                <span class="stream-row-name">${escapeHtml(s.name)}</span>
                <span class="stream-row-url">${escapeHtml(s.url)}</span>
                <label class="stream-toggle">
                    <input type="checkbox" ${s.enabled ? "checked" : ""} onchange="toggleStream('${s.id}', this.checked)">
                    <span class="toggle-slider"></span>
                </label>
                <button class="btn-delete" onclick="removeStream('${s.id}')" title="Remove stream">&#10007;</button>
            </div>
        `).join("");
    } catch (e) {
        console.error("Failed to load streams settings:", e);
    }
}

async function addStream() {
    const id = document.getElementById("stream-add-id").value.trim().toLowerCase();
    const name = document.getElementById("stream-add-name").value.trim();
    const url = document.getElementById("stream-add-url").value.trim();
    const color = document.getElementById("stream-add-color").value;
    if (!id || !name || !url) return alert("ID, Name, and URL are required");
    try {
        const resp = await fetch("/api/streams", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ id, name, url, color, enabled: true }),
        });
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({}));
            return alert(err.detail || "Failed to add stream");
        }
        document.getElementById("stream-add-id").value = "";
        document.getElementById("stream-add-name").value = "";
        document.getElementById("stream-add-url").value = "";
        await loadStreamsSettings();
    } catch (e) {
        console.error("Failed to add stream:", e);
    }
}

async function toggleStream(streamId, enabled) {
    try {
        await fetch(`/api/streams/${streamId}`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ enabled }),
        });
    } catch (e) {
        console.error("Failed to toggle stream:", e);
    }
}

async function removeStream(streamId) {
    if (!confirm(`Delete stream "${streamId}"? Historical data will be preserved.`)) return;
    try {
        const resp = await fetch(`/api/streams/${streamId}`, { method: "DELETE" });
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({}));
            return alert(err.detail || "Failed to delete stream");
        }
        await loadStreamsSettings();
    } catch (e) {
        console.error("Failed to remove stream:", e);
    }
}

// --- Config ---

async function loadConfig() {
    try {
        const resp = await fetch("/api/config");
        const cfg = await resp.json();
        document.getElementById("cfg-whisper").value = cfg.whisper_model || "base";
        document.getElementById("cfg-sensitivity").value = cfg.alert_sensitivity || "medium";
        document.getElementById("cfg-system-prompt").value = cfg.system_prompt || "";
        document.getElementById("cfg-custom-instructions").value = cfg.custom_instructions || "";
        document.getElementById("cfg-map-lat").value = cfg.map_default_lat || "";
        document.getElementById("cfg-map-lng").value = cfg.map_default_lng || "";
        document.getElementById("cfg-webhook-url").value = cfg.webhook_url || "";
        document.getElementById("cfg-vad-threshold").value = cfg.vad_threshold ?? 0.5;
        document.getElementById("cfg-vad-grace").value = cfg.vad_grace_period_s ?? 1.5;
        document.getElementById("cfg-vad-pre-roll").value = cfg.vad_pre_roll_s ?? 1.0;
        document.getElementById("cfg-vad-min-chunk").value = cfg.min_chunk_duration_s ?? 1.5;
        document.getElementById("cfg-vad-max-chunk").value = cfg.max_chunk_duration_s ?? 60;

        // Load models
        const mResp = await fetch("/api/models");
        const mData = await mResp.json();
        const select = document.getElementById("cfg-model");
        select.innerHTML = "";
        if (mData.models.length === 0) {
            select.innerHTML = '<option value="">No models (check API key)</option>';
        } else {
            mData.models.forEach((m) => {
                const opt = document.createElement("option");
                opt.value = m.id;
                opt.textContent = m.name;
                if (m.id === cfg.analysis_model) opt.selected = true;
                select.appendChild(opt);
            });
        }
    } catch (e) {
        console.error("Failed to load config:", e);
    }
}

async function saveConfig() {
    const body = {
        analysis_model: document.getElementById("cfg-model").value,
        whisper_model: document.getElementById("cfg-whisper").value,
        alert_sensitivity: document.getElementById("cfg-sensitivity").value,
        custom_instructions: document.getElementById("cfg-custom-instructions").value,
    };
    const apiKey = document.getElementById("cfg-api-key").value;
    if (apiKey) body.openrouter_api_key = apiKey;
    const mapsApiKey = document.getElementById("cfg-maps-api-key").value;
    if (mapsApiKey) body.google_maps_api_key = mapsApiKey;
    const mapLat = document.getElementById("cfg-map-lat").value;
    if (mapLat) body.map_default_lat = parseFloat(mapLat);
    const mapLng = document.getElementById("cfg-map-lng").value;
    if (mapLng) body.map_default_lng = parseFloat(mapLng);
    body.webhook_url = document.getElementById("cfg-webhook-url").value;
    const vadThreshold = document.getElementById("cfg-vad-threshold").value;
    if (vadThreshold) body.vad_threshold = parseFloat(vadThreshold);
    const vadGrace = document.getElementById("cfg-vad-grace").value;
    if (vadGrace) body.vad_grace_period_s = parseFloat(vadGrace);
    const vadPreRoll = document.getElementById("cfg-vad-pre-roll").value;
    if (vadPreRoll) body.vad_pre_roll_s = parseFloat(vadPreRoll);
    const vadMinChunk = document.getElementById("cfg-vad-min-chunk").value;
    if (vadMinChunk) body.min_chunk_duration_s = parseFloat(vadMinChunk);
    const vadMaxChunk = document.getElementById("cfg-vad-max-chunk").value;
    if (vadMaxChunk) body.max_chunk_duration_s = parseFloat(vadMaxChunk);

    try {
        await fetch("/api/config", {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
        });
        const msg = document.getElementById("save-msg");
        msg.classList.add("show");
        setTimeout(() => msg.classList.remove("show"), 2000);
    } catch (e) {
        console.error("Failed to save config:", e);
    }
}

// --- Dictionary ---

async function loadDictionary() {
    try {
        const resp = await fetch("/api/dictionary");
        const data = await resp.json();
        const tbody = document.getElementById("dictionary-tbody");
        if (!tbody) return;
        tbody.innerHTML = "";
        (data.entries || []).forEach(entry => {
            const tr = document.createElement("tr");
            tr.innerHTML = `
                <td>${escapeHtml(entry.term)}</td>
                <td>${escapeHtml(entry.replacement)}</td>
                <td><span class="dict-category-badge">${entry.category}</span></td>
                <td><button class="btn-delete" onclick="deleteDictionaryEntry(${entry.id})">&#10007;</button></td>
            `;
            tbody.appendChild(tr);
        });
    } catch (e) {
        console.error("Failed to load dictionary:", e);
    }
}

async function addDictionaryEntry() {
    const term = document.getElementById("dict-term").value.trim();
    const replacement = document.getElementById("dict-replacement").value.trim();
    const category = document.getElementById("dict-category").value;
    if (!term || !replacement) return;
    try {
        await fetch("/api/dictionary", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ term, replacement, category }),
        });
        document.getElementById("dict-term").value = "";
        document.getElementById("dict-replacement").value = "";
        await loadDictionary();
    } catch (e) {
        console.error("Failed to add dictionary entry:", e);
    }
}

async function deleteDictionaryEntry(entryId) {
    try {
        await fetch(`/api/dictionary/${entryId}`, { method: "DELETE" });
        await loadDictionary();
    } catch (e) {
        console.error("Failed to delete dictionary entry:", e);
    }
}

// --- Init ---

loadConfig();
loadStreamsSettings();
loadDictionary();
