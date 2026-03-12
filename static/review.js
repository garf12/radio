// --- Utilities ---

function escapeHtml(str) {
    const div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
}

function formatTime(iso) {
    if (!iso) return "";
    const d = new Date(iso);
    return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

// --- Audio Playback ---

const PLAY_SVG = '<svg viewBox="0 0 16 16"><polygon points="4,2 14,8 4,14"/></svg>';
const PAUSE_SVG = '<svg viewBox="0 0 16 16"><rect x="3" y="2" width="3.5" height="12"/><rect x="9.5" y="2" width="3.5" height="12"/></svg>';
const VIDEO_SVG = '<svg viewBox="0 0 16 16"><path d="M2 3a1 1 0 0 1 1-1h10a1 1 0 0 1 1 1v10a1 1 0 0 1-1 1H3a1 1 0 0 1-1-1V3zm9.5 5L8 5.5v5L11.5 8z"/></svg>';

let currentPlayBtn = null;
const sharedAudio = document.getElementById("shared-audio");

function playAudio(transcriptionId, btn) {
    if (currentPlayBtn === btn) {
        sharedAudio.pause();
        btn.innerHTML = PLAY_SVG + "Play";
        btn.classList.remove("playing");
        currentPlayBtn = null;
        return;
    }
    if (currentPlayBtn) {
        sharedAudio.pause();
        currentPlayBtn.innerHTML = PLAY_SVG + "Play";
        currentPlayBtn.classList.remove("playing");
    }
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
    sharedAudio.onended = () => {
        btn.innerHTML = PLAY_SVG + "Play";
        btn.classList.remove("playing");
        currentPlayBtn = null;
    };
}

function playBtnHtml(transcriptionId) {
    if (!transcriptionId) return "";
    return `<button class="play-btn" onclick="playAudio(${transcriptionId}, this)">${PLAY_SVG}Play</button>`;
}

function exportBtnHtml() {
    return "";
}

// --- Review Queue ---

let reviewFilter = "all";

document.querySelectorAll(".review-filter-btn").forEach(btn => {
    btn.addEventListener("click", () => {
        document.querySelectorAll(".review-filter-btn").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        reviewFilter = btn.dataset.filter;
        loadReviewQueue();
    });
});

async function loadReviewStats() {
    try {
        const resp = await fetch("/api/review/stats");
        const stats = await resp.json();
        const el = document.getElementById("review-stats");
        if (el) {
            el.innerHTML = `
                <span class="review-stat">FP Rate: <strong>${stats.false_positive_rate}%</strong></span>
                <span class="review-stat">Pending: <strong>${stats.pending_total}</strong></span>
                <span class="review-stat">Corrections: <strong>${stats.corrections}</strong></span>
                <span class="review-stat">Dictionary: <strong>${stats.dictionary_entries}</strong></span>
            `;
        }
    } catch (e) {
        console.error("Failed to load review stats:", e);
    }
}

async function loadReviewQueue() {
    const list = document.getElementById("review-queue-list");
    if (!list) return;
    try {
        const resp = await fetch(`/api/review/queue?review_type=${reviewFilter}&limit=50`);
        const data = await resp.json();
        const items = data.items || [];
        if (items.length === 0) {
            list.innerHTML = '<div class="empty-state">No items to review</div>';
            return;
        }
        list.innerHTML = "";
        items.forEach(item => {
            if (item.item_type === "transcription") {
                list.appendChild(createTranscriptionReviewCard(item));
            } else if (item.item_type === "alert") {
                list.appendChild(createAlertReviewCard(item));
            }
        });
    } catch (e) {
        console.error("Failed to load review queue:", e);
        list.innerHTML = '<div class="empty-state">Failed to load review queue</div>';
    }
}

function createTranscriptionReviewCard(t) {
    const card = document.createElement("div");
    card.className = "review-card transcription-review";
    card.id = `review-t-${t.id}`;

    const flags = (t.flags || []).map(f => `<span class="flag-badge">${f}</span>`).join("");
    const confidencePct = t.confidence != null ? Math.round(t.confidence * 100) : "?";

    card.innerHTML = `
        <div class="review-card-header">
            <span class="review-type-badge transcription">Transcription</span>
            <span class="confidence-badge ${confidencePct > 60 ? 'good' : confidencePct > 30 ? 'warn' : 'poor'}">${confidencePct}%</span>
            ${flags}
            <span class="time">${formatTime(t.timestamp)}</span>
        </div>
        <div class="review-original">${escapeHtml(t.text)}</div>
        ${t.audio_file ? `<div class="review-audio">${playBtnHtml(t.id)}</div>` : ""}
        <textarea class="review-textarea" id="review-text-${t.id}" rows="3">${escapeHtml(t.text)}</textarea>
        <div class="review-actions">
            <button class="btn btn-sm btn-confirm" onclick="confirmTranscription(${t.id})">Correct as-is</button>
            <button class="btn btn-sm btn-correct" onclick="submitTranscriptionCorrection(${t.id})">Submit Correction</button>
        </div>
    `;
    return card;
}

function createAlertReviewCard(a) {
    const card = document.createElement("div");
    card.className = `review-card alert-review ${a.severity}`;
    card.id = `review-a-${a.id}`;

    card.innerHTML = `
        <div class="review-card-header">
            <span class="review-type-badge alert">Alert</span>
            <span class="alert-badge ${a.severity}">${a.severity}</span>
            <span class="review-category">${a.category}</span>
            <span class="time">${formatTime(a.timestamp)}</span>
        </div>
        <div class="review-summary">${escapeHtml(a.summary)}</div>
        <div class="review-actions">
            <button class="btn btn-sm btn-confirm" onclick="feedbackAlertFromReview(${a.id}, 'correct', this)">Correct</button>
            <button class="btn btn-sm btn-fp" onclick="feedbackAlertFromReview(${a.id}, 'false_positive', this)">False Positive</button>
            <button class="btn btn-sm btn-correct" onclick="toggleAlertCorrectionForm(${a.id})">Correct Details</button>
        </div>
        <div class="alert-correction-form" id="correction-form-${a.id}" style="display:none">
            <div class="correction-row">
                <select id="corr-severity-${a.id}" class="dict-input dict-select">
                    <option value="">Same severity</option>
                    <option value="critical">Critical</option>
                    <option value="high">High</option>
                    <option value="medium">Medium</option>
                    <option value="low">Low</option>
                </select>
                <select id="corr-category-${a.id}" class="dict-input dict-select">
                    <option value="">Same category</option>
                    <option value="shooting">Shooting</option>
                    <option value="pursuit">Pursuit</option>
                    <option value="fire">Fire</option>
                    <option value="accident">Accident</option>
                    <option value="medical">Medical</option>
                    <option value="missing_person">Missing Person</option>
                    <option value="robbery">Robbery</option>
                    <option value="assault">Assault</option>
                    <option value="drug_activity">Drug Activity</option>
                    <option value="hazmat">Hazmat</option>
                    <option value="other">Other</option>
                </select>
            </div>
            <textarea id="corr-summary-${a.id}" class="review-textarea" rows="2" placeholder="Corrected summary (optional)"></textarea>
            <button class="btn btn-sm btn-correct" onclick="submitAlertCorrection(${a.id})">Submit Correction</button>
        </div>
    `;
    return card;
}

function toggleAlertCorrectionForm(alertId) {
    const form = document.getElementById(`correction-form-${alertId}`);
    if (form) form.style.display = form.style.display === "none" ? "block" : "none";
}

async function confirmTranscription(transcriptionId) {
    try {
        await fetch(`/api/transcriptions/${transcriptionId}/confirm`, { method: "POST" });
        const card = document.getElementById(`review-t-${transcriptionId}`);
        if (card) {
            card.classList.add("reviewed");
            card.querySelector(".review-actions").innerHTML = '<span class="feedback-done correct">Confirmed</span>';
        }
    } catch (e) {
        console.error("Failed to confirm transcription:", e);
    }
}

async function submitTranscriptionCorrection(transcriptionId) {
    const textarea = document.getElementById(`review-text-${transcriptionId}`);
    if (!textarea) return;
    try {
        const resp = await fetch(`/api/transcriptions/${transcriptionId}/correct`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ corrected_text: textarea.value }),
        });
        const data = await resp.json();
        const card = document.getElementById(`review-t-${transcriptionId}`);
        if (card) {
            card.classList.add("reviewed");
            card.querySelector(".review-actions").innerHTML = '<span class="feedback-done correction">Corrected</span>';
        }
        if (data.dictionary_suggestions && data.dictionary_suggestions.length > 0) {
            showDictionarySuggestions(data.dictionary_suggestions);
        }
    } catch (e) {
        console.error("Failed to submit correction:", e);
    }
}

function showDictionarySuggestions(suggestions) {
    suggestions.forEach(s => {
        if (confirm(`Add to dictionary?\n"${s.term}" → "${s.replacement}"`)) {
            fetch("/api/dictionary", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ term: s.term, replacement: s.replacement, category: "general" }),
            }).catch(e => console.error("Failed to add dictionary entry:", e));
        }
    });
}

async function feedbackAlertFromReview(alertId, feedbackType, btn) {
    try {
        await fetch(`/api/alerts/${alertId}/feedback`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ feedback_type: feedbackType }),
        });
        const card = document.getElementById(`review-a-${alertId}`);
        if (card) {
            card.classList.add("reviewed");
            const label = feedbackType === "correct" ? "Confirmed" : "Marked FP";
            card.querySelector(".review-actions").innerHTML = `<span class="feedback-done ${feedbackType}">${label}</span>`;
        }
    } catch (e) {
        console.error("Feedback submission failed:", e);
    }
}

async function submitAlertCorrection(alertId) {
    const severity = document.getElementById(`corr-severity-${alertId}`)?.value || null;
    const category = document.getElementById(`corr-category-${alertId}`)?.value || null;
    const summary = document.getElementById(`corr-summary-${alertId}`)?.value || null;
    try {
        await fetch(`/api/alerts/${alertId}/feedback`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                feedback_type: "correction",
                corrected_severity: severity,
                corrected_category: category,
                corrected_summary: summary,
            }),
        });
        const card = document.getElementById(`review-a-${alertId}`);
        if (card) {
            card.classList.add("reviewed");
            card.querySelector(".review-actions").innerHTML = '<span class="feedback-done correction">Corrected</span>';
            const form = document.getElementById(`correction-form-${alertId}`);
            if (form) form.style.display = "none";
        }
    } catch (e) {
        console.error("Failed to submit alert correction:", e);
    }
}

// --- Init ---

loadReviewStats();
loadReviewQueue();
