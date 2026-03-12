// State
let ws = null;
let transcriptCount = 0;
let alertCount = 0;
let eventCount = 0;
let eventsActive = 0;
let liveFeedOffset = 0;
let liveFeedLoading = false;
let liveFeedDone = false;
const FEED_PAGE = 50;

// Stream state
let selectedStreamId = "";  // empty = "All Streams"
let streamsData = [];       // array from API
let streamColorMap = {};    // stream_id -> color

// Audio playback state
let currentPlayBtn = null;
const sharedAudio = document.getElementById("shared-audio");

// Live audio state
let liveAudioPlaying = false;
const liveAudio = document.getElementById("live-audio");

// SVG icons
const PLAY_SVG = '<svg viewBox="0 0 16 16"><polygon points="4,2 14,8 4,14"/></svg>';
const PAUSE_SVG = '<svg viewBox="0 0 16 16"><rect x="3" y="2" width="3.5" height="12"/><rect x="9.5" y="2" width="3.5" height="12"/></svg>';

// Audio meter state
let peakHoldValue = 0;
let peakDecayTimer = null;

// Summaries state (no longer needed but keep section marker)

// Map state
let map = null;
let mapMarkers = {};  // event_id -> Marker
let mapInfoWindow = null;
let mapLoaded = false;
let mapApiLoaded = false;
let mapConfig = null;

// --- Audio Playback ---

function playAudio(transcriptionId, btn) {
    // If same button is playing, pause it
    if (currentPlayBtn === btn) {
        sharedAudio.pause();
        btn.innerHTML = PLAY_SVG + "Play";
        btn.classList.remove("playing");
        currentPlayBtn = null;
        return;
    }

    // Stop previous
    if (currentPlayBtn) {
        sharedAudio.pause();
        currentPlayBtn.innerHTML = PLAY_SVG + "Play";
        currentPlayBtn.classList.remove("playing");
    }

    // Stop live audio if playing
    if (liveAudioPlaying) {
        stopLiveAudio();
    }

    // Play new
    currentPlayBtn = btn;
    btn.innerHTML = PAUSE_SVG + "Stop";
    btn.classList.add("playing");
    sharedAudio.src = `/api/audio/${transcriptionId}`;
    sharedAudio.load();
    sharedAudio.play().catch((err) => {
        console.error("Audio playback failed:", err);
        btn.innerHTML = PLAY_SVG + "Play";
        btn.classList.remove("playing");
        currentPlayBtn = null;
    });
}

sharedAudio.addEventListener("ended", () => {
    if (currentPlayBtn) {
        currentPlayBtn.innerHTML = PLAY_SVG + "Play";
        currentPlayBtn.classList.remove("playing");
        currentPlayBtn = null;
    }
});

sharedAudio.addEventListener("error", () => {
    if (currentPlayBtn) {
        currentPlayBtn.innerHTML = PLAY_SVG + "Play";
        currentPlayBtn.classList.remove("playing");
        currentPlayBtn = null;
    }
});

// --- Live Audio ---

const LIVE_PLAY_SVG = '<polygon points="4,2 14,8 4,14"/>';
const LIVE_PAUSE_SVG = '<rect x="3" y="2" width="3.5" height="12"/><rect x="9.5" y="2" width="3.5" height="12"/>';

function toggleLiveAudio() {
    if (liveAudioPlaying) {
        stopLiveAudio();
    } else {
        // Stop transcription playback if active
        if (currentPlayBtn) {
            sharedAudio.pause();
            currentPlayBtn.innerHTML = PLAY_SVG + "Play";
            currentPlayBtn.classList.remove("playing");
            currentPlayBtn = null;
        }

        const btn = document.getElementById("live-listen-btn");
        const icon = document.getElementById("live-listen-icon");
        liveAudio.src = selectedStreamId ? `/api/stream?stream_id=${selectedStreamId}` : "/api/stream";
        liveAudio.play().then(() => {
            liveAudioPlaying = true;
            btn.classList.add("listening");
            icon.innerHTML = LIVE_PAUSE_SVG;
        }).catch(() => {
            liveAudio.src = "";
            liveAudioPlaying = false;
            btn.classList.remove("listening");
            icon.innerHTML = LIVE_PLAY_SVG;
        });
    }
}

function stopLiveAudio() {
    liveAudio.pause();
    liveAudio.src = "";
    liveAudioPlaying = false;
    const btn = document.getElementById("live-listen-btn");
    const icon = document.getElementById("live-listen-icon");
    btn.classList.remove("listening");
    icon.innerHTML = LIVE_PLAY_SVG;
}

liveAudio.addEventListener("error", () => {
    stopLiveAudio();
});

// --- Audio Meter ---

function updateAudioMeter(data) {
    const rms = data.rms || 0;
    const peak = data.peak || 0;

    // Convert to percentage (0-100), using a log scale for better visual response
    // RMS of 0.01 ~ -40dB, 1.0 ~ 0dB
    const rmsDb = rms > 0 ? 20 * Math.log10(rms) : -60;
    const peakDb = peak > 0 ? 20 * Math.log10(peak) : -60;

    // Map -60dB..0dB to 0..100%
    const rmsPct = Math.max(0, Math.min(100, ((rmsDb + 60) / 60) * 100));
    const peakPct = Math.max(0, Math.min(100, ((peakDb + 60) / 60) * 100));

    document.getElementById("audio-meter-fill").style.width = rmsPct + "%";

    // Peak hold with decay
    if (peakPct > peakHoldValue) {
        peakHoldValue = peakPct;
        clearTimeout(peakDecayTimer);
        peakDecayTimer = setTimeout(() => {
            const decay = setInterval(() => {
                peakHoldValue = Math.max(0, peakHoldValue - 2);
                document.getElementById("audio-meter-peak").style.left = peakHoldValue + "%";
                if (peakHoldValue <= 0) clearInterval(decay);
            }, 50);
        }, 500);
    }
    document.getElementById("audio-meter-peak").style.left = peakHoldValue + "%";

    // dB readout
    const dbText = rmsDb > -60 ? Math.round(rmsDb) + "dB" : "--dB";
    document.getElementById("audio-meter-db").textContent = dbText;
}

// --- VAD Indicator ---

function updateVadIndicator(state) {
    const el = document.getElementById("vad-indicator");
    if (!el) return;
    el.classList.remove("vad-waiting", "vad-recording", "vad-grace");
    if (state === "recording" || state === "grace_period") {
        el.textContent = "RECEIVING";
        el.classList.add(state === "recording" ? "vad-recording" : "vad-grace");
    } else {
        el.textContent = "STANDBY";
        el.classList.add("vad-waiting");
    }
}

// --- Stream Functions ---

function getStreamName(streamId) {
    if (!streamId) return "";
    const s = streamsData.find(s => s.id === streamId);
    return s ? s.name : streamId;
}

function getStreamColor(streamId) {
    return streamColorMap[streamId] || "#00e89d";
}

function streamTagHtml(streamId) {
    if (!streamId || streamsData.length <= 1) return "";
    const name = getStreamName(streamId);
    const color = getStreamColor(streamId);
    return `<span class="stream-tag" style="--stream-color: ${color}">${escapeHtml(name)}</span>`;
}

async function loadStreams() {
    try {
        const resp = await fetch("/api/streams");
        const data = await resp.json();
        streamsData = data.streams || [];
        streamColorMap = {};
        streamsData.forEach(s => { streamColorMap[s.id] = s.color || "#00e89d"; });

        const select = document.getElementById("stream-select");
        // Preserve current selection
        const prev = select.value;
        select.innerHTML = '<option value="">All Streams</option>';
        streamsData.forEach(s => {
            const opt = document.createElement("option");
            opt.value = s.id;
            opt.textContent = s.name + (s.enabled ? "" : " (disabled)");
            opt.style.color = s.color;
            select.appendChild(opt);
        });
        // Hide selector if only one stream
        select.parentElement.style.display = streamsData.length <= 1 ? "none" : "";
        if (prev && streamsData.some(s => s.id === prev)) {
            select.value = prev;
        }
    } catch (e) {
        console.error("Failed to load streams:", e);
    }
}

function onStreamChange() {
    const prev = selectedStreamId;
    selectedStreamId = document.getElementById("stream-select").value;
    // Re-filter visible items
    filterFeedByStream();
    // Reset and reload live feed if stream changed
    if (prev !== selectedStreamId) {
        liveFeedOffset = 0;
        liveFeedDone = false;
        document.getElementById("transcript-list").innerHTML = '<div class="empty-state">Loading...</div>';
        document.getElementById("event-list").innerHTML = '<div class="empty-state">Loading...</div>';
        eventsPanelInitialized = false;
        loadLiveFeed();
        // Update live audio source
        if (liveAudioPlaying) {
            stopLiveAudio();
        }
    }
    // Reload map if visible
    if (mapLoaded) loadMapEvents();
}

function filterFeedByStream() {
    // Filter transcript items
    const tList = document.getElementById("transcript-list");
    tList.querySelectorAll(".transcript-item").forEach(el => {
        if (!selectedStreamId || el.dataset.streamId === selectedStreamId) {
            el.style.display = "";
        } else {
            el.style.display = "none";
        }
    });
    // Filter event cards in sidebar
    const eList = document.getElementById("event-list");
    eList.querySelectorAll(".event-card").forEach(el => {
        if (!selectedStreamId || el.dataset.streamId === selectedStreamId) {
            el.style.display = "";
        } else {
            el.style.display = "none";
        }
    });
}

// --- Stream Management (Settings) ---


// --- Play button HTML helper ---

function playBtnHtml(transcriptionId) {
    if (!transcriptionId) return "";
    return `<button class="play-btn" onclick="playAudio(${transcriptionId}, this)">${PLAY_SVG}Play</button>`;
}

// --- Export Video ---

const VIDEO_SVG = '<svg viewBox="0 0 16 16"><path d="M2 3a1 1 0 0 1 1-1h10a1 1 0 0 1 1 1v10a1 1 0 0 1-1 1H3a1 1 0 0 1-1-1V3zm9.5 5L8 5.5v5L11.5 8z"/></svg>';
let exportTranscriptionId = null;

function exportBtnHtml(transcriptionId) {
    if (!transcriptionId) return "";
    return `<button class="export-btn" onclick="openExportModal(${transcriptionId})">${VIDEO_SVG}Video</button>`;
}

function openExportModal(transcriptionId) {
    exportTranscriptionId = transcriptionId;
    const modal = document.getElementById("export-modal");
    document.getElementById("export-text").value = "";
    document.getElementById("export-bg").value = "";
    const picker = document.getElementById("bg-picker");
    picker.classList.remove("has-file");
    document.getElementById("bg-picker-label").textContent = "Click to select image...";
    const status = document.getElementById("export-status");
    status.style.display = "none";
    status.className = "export-status";
    document.getElementById("export-generate-btn").disabled = false;
    modal.style.display = "";
}

function onBgFileChange(input) {
    const picker = document.getElementById("bg-picker");
    const label = document.getElementById("bg-picker-label");
    if (input.files && input.files[0]) {
        picker.classList.add("has-file");
        label.textContent = input.files[0].name;
    } else {
        picker.classList.remove("has-file");
        label.textContent = "Click to select image...";
    }
}

function closeExportModal() {
    document.getElementById("export-modal").style.display = "none";
    exportTranscriptionId = null;
}

document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeExportModal();
});

async function generateVideo() {
    if (!exportTranscriptionId) return;

    const btn = document.getElementById("export-generate-btn");
    const status = document.getElementById("export-status");
    const spinner = document.getElementById("export-spinner");
    const statusText = document.getElementById("export-status-text");

    btn.disabled = true;
    status.style.display = "flex";
    status.className = "export-status";
    spinner.style.display = "";
    statusText.textContent = "Generating video...";

    const text = document.getElementById("export-text").value;
    const bgFile = document.getElementById("export-bg").files[0];

    const formData = new FormData();
    formData.append("text", text);
    if (bgFile) formData.append("background", bgFile);

    try {
        const resp = await fetch(`/api/video/${exportTranscriptionId}`, {
            method: "POST",
            body: formData,
        });

        if (!resp.ok) {
            const err = await resp.json().catch(() => ({ detail: "Video generation failed" }));
            throw new Error(err.detail || "Video generation failed");
        }

        const blob = await resp.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `scanner_${exportTranscriptionId}.mp4`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);

        spinner.style.display = "none";
        status.className = "export-status success";
        statusText.textContent = "Video downloaded!";
        btn.disabled = false;
    } catch (e) {
        spinner.style.display = "none";
        status.className = "export-status error";
        statusText.textContent = e.message || "Failed to generate video";
        btn.disabled = false;
    }
}

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
        } else if (msg.type === "event") {
            handleEvent(msg.data);
            if (!selectedStreamId || (msg.data && msg.data.stream_id === selectedStreamId)) {
                updateMapFromEvent(msg.data);
            }
        } else if (msg.type === "status") {
            updateChunkStatus(msg.data);
        } else if (msg.type === "audio_level") {
            if (!selectedStreamId || msg.data.stream_id === selectedStreamId) {
                updateAudioMeter(msg.data);
            }
        } else if (msg.type === "vad_state") {
            if (!selectedStreamId || msg.data.stream_id === selectedStreamId) {
                updateVadIndicator(msg.data.state);
            }
        } else if (msg.type === "summary") {
            addSummaryToFeed(msg.data);
        } else if (msg.type === "feedback") {
            // Update feedback state in UI if visible
            const fbContainer = document.getElementById(`fb-${msg.data.alert_id}`);
            if (fbContainer) {
                const label = msg.data.feedback_type === "correct" ? "Confirmed" : msg.data.feedback_type === "false_positive" ? "Marked FP" : "Corrected";
                fbContainer.innerHTML = `<span class="feedback-done ${msg.data.feedback_type}">${label}</span>`;
            }
        }
    };
}

// --- Transcriptions ---

function addTranscription(data) {
    const list = document.getElementById("transcript-list");

    // Dedup: skip if already rendered
    if (data.id && document.getElementById(`t-${data.id}`)) return;

    // Filter: hide if doesn't match selected stream
    const visible = !selectedStreamId || data.stream_id === selectedStreamId;

    const empty = list.querySelector(".empty-state");
    if (empty) empty.remove();

    const el = document.createElement("div");
    el.className = "transcript-item";
    if (data.id) el.id = `t-${data.id}`;
    if (data.stream_id) el.dataset.streamId = data.stream_id;
    if (!visible) el.style.display = "none";

    const confidencePct = data.confidence != null ? Math.round(data.confidence * 100) : null;
    const confidenceBadge = confidencePct != null
        ? `<span class="confidence-badge ${confidencePct > 60 ? 'good' : confidencePct > 30 ? 'warn' : 'poor'}">${confidencePct}%</span>`
        : "";
    const reviewBadge = data.needs_review ? '<span class="review-badge">Review</span>' : "";
    const sTag = streamTagHtml(data.stream_id);

    el.innerHTML = `
        <div class="item-header">
            <span class="time">${formatTime(data.timestamp)}${sTag}</span>
            <span class="transcript-badges">${confidenceBadge}${reviewBadge}${data.audio_file ? playBtnHtml(data.id) + exportBtnHtml(data.id) : ""}</span>
        </div>
        <div class="text">${escapeHtml(data.text)}</div>
    `;
    list.prepend(el);

    transcriptCount++;
    updateCounts();
}

// --- Alerts ---

function addAlert(data) {
    alertCount++;
    updateCounts();

    // Browser notification only — sidebar is now driven by events
    if (Notification.permission === "granted") {
        new Notification(`Scanner Alert [${data.severity.toUpperCase()}]`, {
            body: data.summary,
            tag: `alert-${data.id}`,
        });
    }
}

// --- Events ---

let eventsFilter = 'all';
let eventsPanelInitialized = false;

function handleEvent(eventData) {
    renderEventToContainer(eventData, "event-list", "event-");
    renderEventToContainer(eventData, "events-panel-list", "ep-event-");
    applyEventsFilter();
    // Apply stream filter to sidebar
    if (selectedStreamId && eventData.stream_id !== selectedStreamId) {
        const sidebarCard = document.getElementById("event-" + eventData.id);
        if (sidebarCard) sidebarCard.style.display = "none";
    }
}

function renderEventToContainer(eventData, containerId, idPrefix) {
    const list = document.getElementById(containerId);
    if (!list) return;
    const empty = list.querySelector(".empty-state");
    if (empty) empty.remove();

    const existingCard = document.getElementById(idPrefix + eventData.id);
    if (existingCard) {
        const wasExpanded = existingCard.classList.contains("expanded");
        const newCard = createEventCard(eventData, idPrefix);
        if (wasExpanded) newCard.classList.add("expanded");
        existingCard.replaceWith(newCard);
        list.prepend(newCard);
        newCard.classList.add("pulse");
        setTimeout(() => newCard.classList.remove("pulse"), 800);
    } else {
        const card = createEventCard(eventData, idPrefix);
        list.prepend(card);
        card.classList.add("pulse");
        setTimeout(() => card.classList.remove("pulse"), 800);
    }
}

function createEventCard(ev, idPrefix) {
    idPrefix = idPrefix || "event-";
    const card = document.createElement("div");
    card.className = `event-card ${ev.severity}`;
    card.id = idPrefix + ev.id;
    card.dataset.status = ev.status;
    if (ev.stream_id) card.dataset.streamId = ev.stream_id;

    const cardElementId = idPrefix + ev.id;
    const alertCount = ev.alerts ? ev.alerts.length : (ev.alert_count || 0);
    const timeRange = ev.alerts && ev.alerts.length > 0
        ? `${formatTime(ev.alerts[0].timestamp)} – ${formatTime(ev.alerts[ev.alerts.length - 1].timestamp)}`
        : formatTime(ev.created_at);
    const sTag = streamTagHtml(ev.stream_id);

    const audioBtn = ev.audio_transcription_id
        ? playBtnHtml(ev.audio_transcription_id)
        : "";

    card.innerHTML = `
        <div class="event-header" onclick="toggleEvent('${cardElementId}', ${ev.id})">
            <div class="event-header-top">
                <div class="event-header-badges">
                    <span class="alert-badge ${ev.severity}">${ev.severity}</span>
                    <span class="event-status ${ev.status}">${ev.status}</span>
                    ${sTag}
                </div>
                <span class="event-expand-arrow">&#9660;</span>
            </div>
            <div class="event-title">${escapeHtml(ev.title)}</div>
            <div class="event-meta">
                <span>${ev.category}</span>
                <span>${alertCount} update${alertCount !== 1 ? 's' : ''}</span>
                <span>${timeRange}</span>
                ${audioBtn}
            </div>
        </div>
        <div class="event-alerts">
            ${ev.alerts ? renderEventAlerts(ev.alerts) : '<div class="empty-state" style="padding:12px">Loading...</div>'}
        </div>
    `;
    return card;
}

function toggleEvent(cardElementId, eventId) {
    const card = document.getElementById(cardElementId);
    if (!card) return;
    const isExpanded = card.classList.contains("expanded");
    if (isExpanded) {
        card.classList.remove("expanded");
    } else {
        card.classList.add("expanded");
        const alertsContainer = card.querySelector(".event-alerts");
        const emptyState = alertsContainer.querySelector(".empty-state");
        if (emptyState) {
            fetch(`/api/events/${eventId}`)
                .then(r => r.json())
                .then(data => {
                    alertsContainer.innerHTML = renderEventAlerts(data.alerts || []);
                })
                .catch(() => {
                    alertsContainer.innerHTML = '<div class="empty-state" style="padding:12px">Failed to load alerts</div>';
                });
        }
    }
}

// --- Events panel filter ---

function applyEventsFilter() {
    const list = document.getElementById("events-panel-list");
    if (!list) return;
    const cards = list.querySelectorAll(".event-card");
    cards.forEach(card => {
        const statusMatch = eventsFilter === 'all' || card.dataset.status === eventsFilter;
        const streamMatch = !selectedStreamId || card.dataset.streamId === selectedStreamId;
        card.style.display = (statusMatch && streamMatch) ? '' : 'none';
    });
}

document.querySelectorAll(".events-filter-btn").forEach(btn => {
    btn.addEventListener("click", () => {
        document.querySelectorAll(".events-filter-btn").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        eventsFilter = btn.dataset.filter;
        applyEventsFilter();
    });
});

async function initEventsPanel() {
    if (eventsPanelInitialized) return;
    eventsPanelInitialized = true;
    try {
        let url = "/api/events?limit=50";
        if (selectedStreamId) url += `&stream_id=${selectedStreamId}`;
        const resp = await fetch(url);
        const data = await resp.json();
        const events = data.events || [];
        // Render oldest first so newest ends up on top
        events.slice().reverse().forEach(ev => {
            if (!document.getElementById("ep-event-" + ev.id)) {
                renderEventToContainer(ev, "events-panel-list", "ep-event-");
            }
        });
        applyEventsFilter();
    } catch (e) {
        console.error("Failed to init events panel:", e);
    }
}

function renderEventAlerts(alerts) {
    if (!alerts || alerts.length === 0) {
        return '<div class="empty-state" style="padding:12px">No updates yet</div>';
    }
    return alerts.map(a => `
        <div class="event-alert-item" id="alert-item-${a.id}">
            <div class="alert-header">
                <span class="alert-badge ${a.severity}">${a.severity}</span>
                <span class="time">${formatTime(a.timestamp)}</span>
            </div>
            <div class="summary">${escapeHtml(a.summary)}</div>
            <div class="alert-footer">
                ${a.transcription_id ? playBtnHtml(a.transcription_id) + exportBtnHtml(a.transcription_id) : ""}
                <div class="feedback-buttons" id="fb-${a.id}">
                    <button class="feedback-btn correct" onclick="feedbackAlert(${a.id}, 'correct', this)" title="Correct alert">&#10003;</button>
                    <button class="feedback-btn false-positive" onclick="feedbackAlert(${a.id}, 'false_positive', this)" title="False positive">&#10007;</button>
                </div>
            </div>
        </div>
    `).join("");
}

// --- Tabs ---

document.querySelectorAll(".tab[data-tab]").forEach((tab) => {
    tab.addEventListener("click", () => {
        document.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
        document.querySelectorAll(".panel").forEach((p) => p.classList.remove("active"));
        tab.classList.add("active");
        document.getElementById(`panel-${tab.dataset.tab}`).classList.add("active");

        if (tab.dataset.tab === "map") initMapPanel();
        if (tab.dataset.tab === "events") initEventsPanel();
        if (tab.dataset.tab === "summaries") loadSummaries();
    });
});


// --- Infinite scroll: load older transcriptions ---

async function loadOlderTranscriptions() {
    if (liveFeedLoading || liveFeedDone) return;
    liveFeedLoading = true;
    try {
        let url = `/api/transcriptions?limit=${FEED_PAGE}&offset=${liveFeedOffset}`;
        if (selectedStreamId) url += `&stream_id=${selectedStreamId}`;
        const resp = await fetch(url);
        const data = await resp.json();
        const list = document.getElementById("transcript-list");

        if (data.transcriptions.length === 0) {
            liveFeedDone = true;
            return;
        }

        // API returns newest-first, append to bottom (older items)
        data.transcriptions.forEach((t) => {
            if (t.id && document.getElementById(`t-${t.id}`)) return;
            const el = document.createElement("div");
            el.className = "transcript-item";
            if (t.id) el.id = `t-${t.id}`;
            if (t.stream_id) el.dataset.streamId = t.stream_id;
            const cPct = t.confidence != null ? Math.round(t.confidence * 100) : null;
            const cBadge = cPct != null ? `<span class="confidence-badge ${cPct > 60 ? 'good' : cPct > 30 ? 'warn' : 'poor'}">${cPct}%</span>` : "";
            const rBadge = t.needs_review ? '<span class="review-badge">Review</span>' : "";
            const sTag = streamTagHtml(t.stream_id);
            el.innerHTML = `
                <div class="item-header">
                    <span class="time">${formatTime(t.timestamp)}${sTag}</span>
                    <span class="transcript-badges">${cBadge}${rBadge}${t.audio_file ? playBtnHtml(t.id) + exportBtnHtml(t.id) : ""}</span>
                </div>
                <div class="text">${escapeHtml(t.text)}</div>
            `;
            list.appendChild(el);
        });

        liveFeedOffset += data.transcriptions.length;
        if (data.transcriptions.length < FEED_PAGE) liveFeedDone = true;
    } catch (e) {
        console.error("Failed to load older transcriptions:", e);
    } finally {
        liveFeedLoading = false;
    }
}

document.getElementById("feed").addEventListener("scroll", (e) => {
    const el = e.target;
    if (el.scrollTop + el.clientHeight >= el.scrollHeight - 200) {
        loadOlderTranscriptions();
    }
});

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
        const isRunning = pip.running;
        const dot = isRunning ? "running" : "disconnected";
        const streamStates = pip.streams || {};
        const runningCount = Object.values(streamStates).filter(s => s.running).length;
        const label = isRunning ? `${runningCount} stream${runningCount !== 1 ? "s" : ""} running` : "Pipeline idle";
        document.getElementById("pipeline-status").innerHTML =
            `<span class="status-dot ${dot}"></span>${label}`;

        transcriptCount = data.counts.transcriptions;
        alertCount = data.counts.alerts;
        eventCount = data.counts.events || 0;
        eventsActive = data.counts.events_active || 0;
        updateCounts();

        // Aggregate chunk status across streams
        let totalChunks = 0, totalSilent = 0, lastChunk = null;
        Object.values(streamStates).forEach(s => {
            totalChunks += s.chunks_processed || 0;
            totalSilent += s.silent_chunks || 0;
            if (s.last_chunk && (!lastChunk || s.last_chunk > lastChunk)) lastChunk = s.last_chunk;
        });
        if (totalChunks > 0) {
            updateChunkStatus({
                chunks_processed: totalChunks,
                silent_chunks: totalSilent,
                last_chunk: lastChunk,
            });
        }
    } catch (e) {
        // ignore
    }
}

function updateCounts() {
    document.getElementById("counts").textContent =
        `${transcriptCount} transcriptions | ${alertCount} alerts | ${eventsActive} active events`;
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

// --- Summaries ---

function createSummaryCard(s) {
    const card = document.createElement("div");
    card.className = `summary-card ${s.activity_level || 'moderate'}`;
    card.id = `summary-${s.id}`;
    if (s.stream_id) card.dataset.streamId = s.stream_id;

    const timeRange = `${formatTime(s.period_start)} – ${formatTime(s.period_end)}`;
    const themes = (s.key_themes || []).map(t => `<span class="summary-theme">${escapeHtml(t)}</span>`).join("");
    const sTag = streamTagHtml(s.stream_id);

    card.innerHTML = `
        <div class="summary-header">
            <span class="summary-time-range">${timeRange}${sTag}</span>
            <span class="activity-badge ${s.activity_level || 'moderate'}">${s.activity_level || 'moderate'}</span>
        </div>
        <div class="summary-text">${escapeHtml(s.summary_text)}</div>
        <div class="summary-meta">
            <span>${s.transcription_count || 0} transcriptions</span>
            ${themes}
        </div>
    `;
    return card;
}

function renderSummaryToSlot(slotId, data) {
    const slot = document.getElementById(slotId);
    if (!slot) return;
    slot.innerHTML = "";
    const card = createSummaryCard(data);
    slot.appendChild(card);
    card.classList.add("pulse");
    setTimeout(() => card.classList.remove("pulse"), 800);
}

function addSummaryToFeed(data) {
    const summaryType = data.summary_type || "10min";
    const slotId = summaryType === "hourly" ? "summary-hourly" : "summary-recent";
    renderSummaryToSlot(slotId, data);
}

async function loadSummaries() {
    try {
        const resp = await fetch("/api/summaries/current");
        const data = await resp.json();

        if (data.recent) {
            renderSummaryToSlot("summary-recent", data.recent);
        }
        if (data.hourly) {
            renderSummaryToSlot("summary-hourly", data.hourly);
        }
    } catch (e) {
        console.error("Failed to load summaries:", e);
    }
}

// --- Map ---

const SEVERITY_COLORS = {
    critical: { fill: "#ff3b3b", stroke: "#cc2020" },
    high:     { fill: "#ff8c22", stroke: "#cc6a10" },
    medium:   { fill: "#ffb800", stroke: "#cc9300" },
    low:      { fill: "#4a90e2", stroke: "#2c6cb5" },
};

async function loadMapApi() {
    if (mapApiLoaded) return true;

    try {
        const resp = await fetch("/api/config/maps");
        mapConfig = await resp.json();
    } catch (e) {
        console.error("Failed to load map config:", e);
        return false;
    }

    if (!mapConfig.google_maps_api_key) {
        document.getElementById("map-container").innerHTML =
            '<div class="map-empty-state">' +
            '<span style="font-size:32px">&#128506;</span>' +
            '<span>Google Maps API key not configured.</span>' +
            '<span>Add it in Settings to enable the map view.</span>' +
            '</div>';
        return false;
    }

    return new Promise((resolve) => {
        window._initGoogleMap = () => {
            mapApiLoaded = true;
            resolve(true);
        };
        const script = document.createElement("script");
        script.src = `https://maps.googleapis.com/maps/api/js?key=${mapConfig.google_maps_api_key}&callback=_initGoogleMap`;
        script.async = true;
        script.defer = true;
        document.head.appendChild(script);
    });
}

const MAP_DARK_STYLES = [
    { elementType: "geometry", stylers: [{ color: "#131b2e" }] },
    { elementType: "labels.text.stroke", stylers: [{ color: "#131b2e" }] },
    { elementType: "labels.text.fill", stylers: [{ color: "#7a8ba6" }] },
    { featureType: "administrative", elementType: "geometry.stroke", stylers: [{ color: "#1a2744" }] },
    { featureType: "road", elementType: "geometry", stylers: [{ color: "#192440" }] },
    { featureType: "road", elementType: "geometry.stroke", stylers: [{ color: "#1a2744" }] },
    { featureType: "road.highway", elementType: "geometry", stylers: [{ color: "#1c2742" }] },
    { featureType: "water", elementType: "geometry", stylers: [{ color: "#080d18" }] },
    { featureType: "poi", elementType: "geometry", stylers: [{ color: "#192440" }] },
    { featureType: "transit", elementType: "geometry", stylers: [{ color: "#192440" }] },
];

async function initMapPanel() {
    if (mapLoaded) return;

    const ready = await loadMapApi();
    if (!ready) return;

    map = new google.maps.Map(document.getElementById("map-container"), {
        center: { lat: mapConfig.map_default_lat, lng: mapConfig.map_default_lng },
        zoom: 13,
        styles: MAP_DARK_STYLES,
        mapTypeControl: false,
        streetViewControl: false,
        fullscreenControl: true,
    });

    mapInfoWindow = new google.maps.InfoWindow();
    mapLoaded = true;
    await loadMapEvents();
}

async function loadMapEvents() {
    if (!mapLoaded) return;

    const status = document.getElementById("map-filter-status").value;
    const hours = document.getElementById("map-filter-time").value;
    let url = "/api/events/map";
    const params = [];
    if (status) params.push(`status=${status}`);
    if (hours) params.push(`hours=${hours}`);
    if (selectedStreamId) params.push(`stream_id=${selectedStreamId}`);
    if (params.length) url += "?" + params.join("&");

    try {
        const resp = await fetch(url);
        const data = await resp.json();

        // Clear existing markers
        Object.values(mapMarkers).forEach((m) => m.setMap(null));
        mapMarkers = {};

        const events = data.events || [];
        events.forEach((ev) => addOrUpdateMapMarker(ev));

        document.getElementById("map-stats").textContent =
            `${events.length} event${events.length !== 1 ? "s" : ""} on map`;
    } catch (e) {
        console.error("Failed to load map events:", e);
    }
}

function filterMapMarkers() {
    loadMapEvents();
}

function addOrUpdateMapMarker(ev) {
    if (!mapLoaded || ev.latitude == null || ev.longitude == null) return;

    const existing = mapMarkers[ev.id];
    if (existing) {
        existing.setMap(null);
    }

    const colors = SEVERITY_COLORS[ev.severity] || SEVERITY_COLORS.low;
    const isResolved = ev.status === "resolved";

    const marker = new google.maps.Marker({
        position: { lat: ev.latitude, lng: ev.longitude },
        map: map,
        icon: {
            path: google.maps.SymbolPath.CIRCLE,
            scale: 14,
            fillColor: colors.fill,
            fillOpacity: isResolved ? 0.5 : 1.0,
            strokeColor: "#ffffff",
            strokeWeight: 3,
        },
        title: ev.title,
        opacity: isResolved ? 0.7 : 1.0,
        zIndex: isResolved ? 1 : 10,
    });

    marker.addListener("click", () => {
        const infoId = `map-info-${ev.id}`;
        const content = `
            <div class="map-info-window" id="${infoId}">
                <div class="info-title">${escapeHtml(ev.title)}</div>
                <div class="info-badges">
                    <span class="info-badge ${ev.severity}">${ev.severity}</span>
                    <span class="info-badge map-status-${ev.status}">${ev.status}</span>
                </div>
                <div class="info-meta"><strong>Category:</strong> ${escapeHtml(ev.category)}</div>
                ${ev.location_text ? `<div class="info-location">&#128205; ${escapeHtml(ev.location_text)}</div>` : ""}
                <div class="info-meta">${formatTime(ev.created_at)}${ev.updated_at !== ev.created_at ? " – " + formatTime(ev.updated_at) : ""}</div>
                <div class="info-audio-section"><span class="info-audio-loading">Loading audio...</span></div>
            </div>
        `;
        mapInfoWindow.setContent(content);
        mapInfoWindow.open(map, marker);

        fetch(`/api/events/${ev.id}`)
            .then(r => r.json())
            .then(data => {
                const container = document.getElementById(infoId);
                if (!container) return;
                const audioSection = container.querySelector(".info-audio-section");
                if (!audioSection) return;
                const alerts = (data.alerts || []).filter(a => a.transcription_id);
                if (alerts.length === 0) {
                    audioSection.remove();
                    return;
                }
                audioSection.innerHTML = '<div class="info-audio-label">Audio</div>' +
                    '<div class="info-audio-list">' +
                    alerts.map(a => `
                        <div class="info-audio-item">
                            <span class="info-audio-summary">${escapeHtml(a.summary).substring(0, 60)}${a.summary.length > 60 ? '...' : ''}</span>
                            <button class="play-btn info-play-btn" onclick="playAudio(${a.transcription_id}, this)">${PLAY_SVG}Play</button>
                            <button class="export-btn info-play-btn" onclick="openExportModal(${a.transcription_id})">${VIDEO_SVG}Video</button>
                        </div>
                    `).join("") +
                    '</div>';
            })
            .catch(() => {
                const container = document.getElementById(infoId);
                if (!container) return;
                const audioSection = container.querySelector(".info-audio-section");
                if (audioSection) audioSection.remove();
            });
    });

    mapMarkers[ev.id] = marker;
}

function updateMapFromEvent(eventData) {
    if (!mapLoaded || !eventData) return;
    if (eventData.latitude == null || eventData.longitude == null) return;

    // Check if this event matches the current status filter
    const filterStatus = document.getElementById("map-filter-status").value;
    if (filterStatus && eventData.status !== filterStatus) {
        // Remove marker if it no longer matches filter
        if (mapMarkers[eventData.id]) {
            mapMarkers[eventData.id].setMap(null);
            delete mapMarkers[eventData.id];
        }
        return;
    }

    // Check if this event matches the current time filter
    const filterHours = document.getElementById("map-filter-time").value;
    if (filterHours && eventData.updated_at) {
        const cutoff = new Date(Date.now() - parseFloat(filterHours) * 3600000);
        if (new Date(eventData.updated_at) < cutoff) {
            if (mapMarkers[eventData.id]) {
                mapMarkers[eventData.id].setMap(null);
                delete mapMarkers[eventData.id];
            }
            return;
        }
    }

    addOrUpdateMapMarker(eventData);

    // Update stats count
    const count = Object.keys(mapMarkers).length;
    document.getElementById("map-stats").textContent =
        `${count} event${count !== 1 ? "s" : ""} on map`;
}

// --- Load recent items into live feed on startup ---

async function loadLiveFeed() {
    try {
        let tUrl = `/api/transcriptions?limit=${FEED_PAGE}&offset=0`;
        let evUrl = "/api/events?limit=50&offset=0";
        if (selectedStreamId) {
            tUrl += `&stream_id=${selectedStreamId}`;
            evUrl += `&stream_id=${selectedStreamId}`;
        }
        const [tResp, evResp] = await Promise.all([
            fetch(tUrl),
            fetch(evUrl),
        ]);
        const tData = await tResp.json();
        const evData = await evResp.json();

        // Transcriptions come back newest-first from API; render oldest first so newest ends up on top
        const transcriptions = tData.transcriptions.reverse();
        if (transcriptions.length > 0) {
            const list = document.getElementById("transcript-list");
            const empty = list.querySelector(".empty-state");
            if (empty) empty.remove();

            transcriptions.forEach((t) => {
                if (t.id && document.getElementById(`t-${t.id}`)) return;
                const el = document.createElement("div");
                el.className = "transcript-item";
                if (t.id) el.id = `t-${t.id}`;
                if (t.stream_id) el.dataset.streamId = t.stream_id;
                const cPct = t.confidence != null ? Math.round(t.confidence * 100) : null;
                const cBadge = cPct != null ? `<span class="confidence-badge ${cPct > 60 ? 'good' : cPct > 30 ? 'warn' : 'poor'}">${cPct}%</span>` : "";
                const rBadge = t.needs_review ? '<span class="review-badge">Review</span>' : "";
                const sTag = streamTagHtml(t.stream_id);
                el.innerHTML = `
                    <div class="item-header">
                        <span class="time">${formatTime(t.timestamp)}${sTag}</span>
                        <span class="transcript-badges">${cBadge}${rBadge}${t.audio_file ? playBtnHtml(t.id) + exportBtnHtml(t.id) : ""}</span>
                    </div>
                    <div class="text">${escapeHtml(t.text)}</div>
                `;
                list.prepend(el);
            });

            liveFeedOffset = tData.transcriptions.length;
            if (tData.transcriptions.length < FEED_PAGE) liveFeedDone = true;
        }

        // Events: load with alert counts, fetch full alerts on expand
        const events = evData.events || [];
        if (events.length > 0) {
            // Events come back newest-first; render oldest first so newest ends up on top
            const reversed = events.slice().reverse();
            reversed.forEach((ev) => {
                handleEvent(ev);
            });
        }
    } catch (e) {
        console.error("Failed to load live feed:", e);
    }
}

// --- Alert Feedback ---

async function feedbackAlert(alertId, feedbackType, btn) {
    try {
        const resp = await fetch(`/api/alerts/${alertId}/feedback`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ feedback_type: feedbackType }),
        });
        if (!resp.ok) throw new Error("Failed");
        const container = document.getElementById(`fb-${alertId}`);
        if (container) {
            const label = feedbackType === "correct" ? "Confirmed" : "Marked FP";
            container.innerHTML = `<span class="feedback-done ${feedbackType}">${label}</span>`;
        }
    } catch (e) {
        console.error("Feedback submission failed:", e);
    }
}


// --- Init ---

if ("Notification" in window && Notification.permission === "default") {
    Notification.requestPermission();
}

// Load streams first, then connect and load feed
loadStreams().then(() => {
    connectWS();
    loadLiveFeed();
    pollStatus();
    setInterval(pollStatus, 15000);
});
