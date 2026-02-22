// State
let ws = null;
let transcriptCount = 0;
let alertCount = 0;
let historyOffset = 0;
const HISTORY_PAGE = 50;

// --- WebSocket ---

function connectWS() {
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    ws = new WebSocket(`${proto}//${location.host}/ws`);

    ws.onopen = () => {
        document.getElementById("ws-status").innerHTML =
            '<span class="status-dot connected"></span>Connected';
    };

    ws.onclose = () => {
        document.getElementById("ws-status").innerHTML =
            '<span class="status-dot disconnected"></span>Disconnected';
        setTimeout(connectWS, 3000);
    };

    ws.onerror = () => ws.close();

    ws.onmessage = (e) => {
        const msg = JSON.parse(e.data);
        if (msg.type === "transcription") {
            addTranscription(msg.data);
        } else if (msg.type === "alert") {
            addAlert(msg.data);
        } else if (msg.type === "status") {
            updateChunkStatus(msg.data);
        }
    };
}

// --- Transcriptions ---

function addTranscription(data) {
    const list = document.getElementById("transcript-list");
    const empty = list.querySelector(".empty-state");
    if (empty) empty.remove();

    const el = document.createElement("div");
    el.className = "transcript-item";
    el.innerHTML = `
        <div class="time">${formatTime(data.timestamp)}</div>
        <div class="text">${escapeHtml(data.text)}</div>
    `;
    list.prepend(el);

    // Keep max 100 items in live feed
    while (list.children.length > 100) {
        list.removeChild(list.lastChild);
    }

    transcriptCount++;
    updateCounts();
}

// --- Alerts ---

function addAlert(data) {
    const list = document.getElementById("alert-list");
    const empty = list.querySelector(".empty-state");
    if (empty) empty.remove();

    const el = document.createElement("div");
    el.className = `alert-item ${data.severity}`;
    el.innerHTML = `
        <div class="alert-header">
            <span class="alert-badge ${data.severity}">${data.severity}</span>
            <span class="time">${formatTime(data.timestamp)}</span>
        </div>
        <div class="summary">${escapeHtml(data.summary)}</div>
        <div class="category">${data.category}</div>
    `;
    list.prepend(el);

    alertCount++;
    updateCounts();

    // Browser notification
    if (Notification.permission === "granted") {
        new Notification(`Scanner Alert [${data.severity.toUpperCase()}]`, {
            body: data.summary,
            tag: `alert-${data.id}`,
        });
    }
}

// --- Tabs ---

document.querySelectorAll(".tab").forEach((tab) => {
    tab.addEventListener("click", () => {
        document.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
        document.querySelectorAll(".panel").forEach((p) => p.classList.remove("active"));
        tab.classList.add("active");
        document.getElementById(`panel-${tab.dataset.tab}`).classList.add("active");

        if (tab.dataset.tab === "config") loadConfig();
        if (tab.dataset.tab === "history" && historyOffset === 0) loadHistory();
    });
});

// --- Config ---

async function loadConfig() {
    try {
        const resp = await fetch("/api/config");
        const cfg = await resp.json();
        document.getElementById("cfg-stream-url").value = cfg.stream_url || "";
        document.getElementById("cfg-whisper").value = cfg.whisper_model || "base";
        document.getElementById("cfg-sensitivity").value = cfg.alert_sensitivity || "medium";

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
        stream_url: document.getElementById("cfg-stream-url").value,
        analysis_model: document.getElementById("cfg-model").value,
        whisper_model: document.getElementById("cfg-whisper").value,
        alert_sensitivity: document.getElementById("cfg-sensitivity").value,
    };
    const apiKey = document.getElementById("cfg-api-key").value;
    if (apiKey) body.openrouter_api_key = apiKey;

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

// --- History ---

async function loadHistory() {
    try {
        const resp = await fetch(`/api/transcriptions?limit=${HISTORY_PAGE}&offset=${historyOffset}`);
        const data = await resp.json();
        const list = document.getElementById("history-list");

        if (data.transcriptions.length === 0 && historyOffset === 0) {
            list.innerHTML = '<div class="empty-state">No transcription history</div>';
            return;
        }

        data.transcriptions.forEach((t) => {
            const el = document.createElement("div");
            el.className = "transcript-item";
            el.innerHTML = `
                <div class="time">${formatTime(t.timestamp)}</div>
                <div class="text">${escapeHtml(t.text)}</div>
            `;
            list.appendChild(el);
        });

        historyOffset += data.transcriptions.length;
        document.getElementById("load-more-btn").style.display =
            data.transcriptions.length < HISTORY_PAGE ? "none" : "";
    } catch (e) {
        console.error("Failed to load history:", e);
    }
}

// --- Status polling ---

function updateChunkStatus(data) {
    const el = document.getElementById("chunk-status");
    if (el) {
        el.textContent = `Chunks: ${data.chunks_processed} (${data.silent_chunks} silent) | Last: ${formatTime(data.last_chunk)}`;
    }
}

async function pollStatus() {
    try {
        const resp = await fetch("/api/status");
        const data = await resp.json();
        const pip = data.pipeline;
        const dot = pip.running ? "running" : "disconnected";
        const label = pip.running ? "Pipeline running" : pip.error ? `Error: ${pip.error}` : "Pipeline idle";
        document.getElementById("pipeline-status").innerHTML =
            `<span class="status-dot ${dot}"></span>${label}`;

        transcriptCount = data.counts.transcriptions;
        alertCount = data.counts.alerts;
        updateCounts();

        if (pip.chunks_processed !== undefined) {
            updateChunkStatus({
                chunks_processed: pip.chunks_processed,
                silent_chunks: pip.silent_chunks,
                last_chunk: pip.last_chunk,
            });
        }
    } catch (e) {
        // ignore
    }
}

function updateCounts() {
    document.getElementById("counts").textContent =
        `${transcriptCount} transcriptions | ${alertCount} alerts`;
}

// --- Helpers ---

function formatTime(iso) {
    if (!iso) return "";
    const d = new Date(iso);
    return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function escapeHtml(str) {
    const div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
}

// --- Init ---

if ("Notification" in window && Notification.permission === "default") {
    Notification.requestPermission();
}

connectWS();
pollStatus();
setInterval(pollStatus, 15000);
