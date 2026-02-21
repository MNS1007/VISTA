# app.py
from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Optional

import streamlit as st

# -----------------------------
# Page config
# -----------------------------
st.set_page_config(page_title="VESTA Video Q&A", page_icon="🎬", layout="wide")

# -----------------------------
# Premium CSS (big impact)
# -----------------------------
st.markdown(
    """
<style>
/* ---------- Global + Background ---------- */
.stApp {
  background:
    radial-gradient(900px 500px at 15% 0%, rgba(124,58,237,0.18), transparent 60%),
    radial-gradient(900px 500px at 85% 10%, rgba(16,185,129,0.16), transparent 58%),
    radial-gradient(900px 500px at 50% 110%, rgba(236,72,153,0.12), transparent 60%);
}
.block-container { padding-top: 1.2rem; padding-bottom: 1.4rem; }

/* Make Streamlit look less “default” */
div[data-testid="stVerticalBlock"] { gap: 0.85rem; }

/* ---------- Header ---------- */
.hero {
  padding: 18px 18px;
  border-radius: 18px;
  border: 1px solid rgba(255,255,255,0.10);
  background: linear-gradient(135deg, rgba(255,255,255,0.06), rgba(255,255,255,0.02));
  box-shadow: 0 16px 40px rgba(0,0,0,0.22);
}
.hero-title {
  font-size: 1.5rem;
  font-weight: 800;
  letter-spacing: -0.02em;
  margin: 0;
}
.hero-sub {
  margin: 4px 0 0 0;
  color: rgba(255,255,255,0.72);
  font-size: 0.95rem;
  line-height: 1.35;
}
.pill {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 6px 10px;
  border-radius: 999px;
  border: 1px solid rgba(255,255,255,0.10);
  background: rgba(255,255,255,0.05);
  color: rgba(255,255,255,0.82);
  font-size: 0.82rem;
}

/* ---------- Cards ---------- */
.card {
  border-radius: 18px;
  border: 1px solid rgba(255,255,255,0.10);
  background: rgba(255,255,255,0.04);
  box-shadow: 0 16px 40px rgba(0,0,0,0.22);
  padding: 14px 14px;
}
.card h3 {
  margin: 0 0 0.6rem 0;
  font-size: 1.05rem;
  font-weight: 800;
}
.muted { color: rgba(255,255,255,0.70); font-size: 0.9rem; }

/* ---------- Chat bubbles (custom) ---------- */
.chatbox {
  height: 520px;
  overflow-y: auto;
  padding: 12px;
  border-radius: 16px;
  border: 1px solid rgba(255,255,255,0.10);
  background: rgba(0,0,0,0.10);
}
.msg {
  margin: 10px 0;
  display: flex;
}
.msg.user { justify-content: flex-end; }
.bubble {
  max-width: 78%;
  padding: 10px 12px;
  border-radius: 14px;
  border: 1px solid rgba(255,255,255,0.10);
  line-height: 1.35;
  font-size: 0.95rem;
  white-space: pre-wrap;
  word-wrap: break-word;
}
.bubble.user {
  background: linear-gradient(135deg, rgba(99,102,241,0.30), rgba(99,102,241,0.14));
}
.bubble.assistant {
  background: rgba(255,255,255,0.06);
}
.meta {
  margin-top: 6px;
  color: rgba(255,255,255,0.65);
  font-size: 0.82rem;
}

/* ---------- Nice small “action chip” ---------- */
.chip {
  display: inline-block;
  margin-top: 8px;
  padding: 6px 10px;
  border-radius: 999px;
  border: 1px solid rgba(255,255,255,0.12);
  background: rgba(255,255,255,0.06);
  font-size: 0.82rem;
  color: rgba(255,255,255,0.80);
}

/* ---------- Inputs ---------- */
.stTextInput > div > div > input,
.stNumberInput input {
  border-radius: 12px !important;
}
.stButton button {
  border-radius: 12px !important;
  font-weight: 700 !important;
}
</style>
""",
    unsafe_allow_html=True,
)

# -----------------------------
# Session state
# -----------------------------
if "video_bytes" not in st.session_state:
    st.session_state.video_bytes = None
if "video_name" not in st.session_state:
    st.session_state.video_name = None
if "play_from" not in st.session_state:
    st.session_state.play_from = 0.0
if "messages" not in st.session_state:
    st.session_state.messages = []  # list[dict(role, text, ts?)]
if "last_jump" not in st.session_state:
    st.session_state.last_jump = None  # last suggested jump timestamp

# -----------------------------
# Model hook (replace later)
# -----------------------------
@dataclass
class ModelResult:
    answer: str
    timestamp_s: Optional[float] = None


def _parse_timestamp_hint(text: str) -> Optional[float]:
    # 12.3s / 12s
    m = re.search(r"(\d+(?:\.\d+)?)\s*s\b", text.lower())
    if m:
        return float(m.group(1))
    # mm:ss or hh:mm:ss
    m = re.search(r"\b(?:(\d+):)?(\d{1,2}):(\d{2})\b", text)
    if m:
        hh = int(m.group(1)) if m.group(1) else 0
        mm = int(m.group(2))
        ss = int(m.group(3))
        return float(hh * 3600 + mm * 60 + ss)
    return None


def answer_question(video_bytes: bytes | None, question: str) -> ModelResult:
    """
    Replace this with your real model call later.

    Expected later:
      - Your pipeline returns (answer, timestamp_seconds or None)
      - timestamp_s triggers an automatic seek in the video player
    """
    if not video_bytes:
        return ModelResult("Upload a video first, then ask questions about it.", None)

    # Demo: parse timestamp if user writes one
    ts = _parse_timestamp_hint(question)
    if ts is not None:
        return ModelResult(
            f"Jumping to **{ts:.2f}s** (demo). When your model is connected, it will pick this automatically.",
            ts,
        )

    # Placeholder response
    return ModelResult(
        "Got it. Connect your model in `answer_question()` so I can respond using the video and jump to the right moment.",
        None,
    )


def seek_to(timestamp_s: float) -> None:
    st.session_state.play_from = max(0.0, float(timestamp_s))
    st.session_state.last_jump = st.session_state.play_from


# -----------------------------
# Header
# -----------------------------
status = "Video loaded" if st.session_state.video_bytes else "No video yet"
dot = "🟢" if st.session_state.video_bytes else "🟠"

st.markdown(
    f"""
<div class="hero">
  <div style="display:flex; align-items:center; justify-content:space-between; gap:12px;">
    <div>
      <div class="hero-title">🎬 VESTA Video Q&A</div>
      <div class="hero-sub">Upload a video on the left, scrub/jump instantly, and ask questions on the right. Your model can return a timestamp to auto-seek.</div>
    </div>
    <div class="pill">{dot} {status}</div>
  </div>
</div>
""",
    unsafe_allow_html=True,
)

# -----------------------------
# Layout
# -----------------------------
left, right = st.columns([1.05, 0.95], gap="large")

# -----------------------------
# Left Card: Video
# -----------------------------
with left:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### 📼 Video")

    uploaded = st.file_uploader(
        "Upload video",
        type=["mp4", "mov", "m4v", "webm"],
        label_visibility="collapsed",
    )
    if uploaded is not None:
        st.session_state.video_bytes = uploaded.getvalue()
        st.session_state.video_name = uploaded.name
        st.session_state.play_from = 0.0
        st.session_state.last_jump = None

    if not st.session_state.video_bytes:
        st.markdown(
            "<div class='muted'>Drop an MP4/MOV/WebM here to enable seeking and chat-based Q&A.</div>",
            unsafe_allow_html=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.caption(f"Loaded: **{st.session_state.video_name}**")

        # Controls row
        c1, c2, c3, c4 = st.columns([1.0, 1.0, 0.9, 0.9], gap="small")
        with c1:
            start_s = st.number_input(
                "Start (s)",
                min_value=0.0,
                value=float(st.session_state.play_from),
                step=1.0,
                help="Where to start playback (seconds).",
            )
        with c2:
            preview_s = st.number_input(
                "Preview (s)",
                min_value=1.0,
                value=5.0,
                step=1.0,
                help="When your model returns a timestamp, treat this as the preview window length.",
            )
        with c3:
            if st.button("▶ Jump", use_container_width=True):
                seek_to(start_s)
                st.rerun()
        with c4:
            if st.button("⟲ Reset", use_container_width=True):
                seek_to(0.0)
                st.rerun()

        # Quick jump buttons (nice UX)
        q1, q2, q3, q4, q5 = st.columns(5, gap="small")
        for label, delta, col in [
            ("-10s", -10, q1),
            ("-5s", -5, q2),
            ("+5s", +5, q4),
            ("+10s", +10, q5),
        ]:
            with col:
                if st.button(label, use_container_width=True):
                    seek_to(st.session_state.play_from + delta)
                    st.rerun()
        with q3:
            st.markdown(
                "<div class='pill' style='justify-content:center; width:100%;'>⏱️ Controls</div>",
                unsafe_allow_html=True,
            )

        # Player
        st.video(st.session_state.video_bytes, start_time=int(st.session_state.play_from))

        # Show last model jump range
        if st.session_state.last_jump is not None:
            a = float(st.session_state.last_jump)
            b = a + float(preview_s)
            st.markdown(
                f"<div class='chip'>Suggested clip: <b>{a:.2f}s</b> → <b>{b:.2f}s</b></div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                "<div class='muted'>Tip: Ask a question in chat; later your model will return a timestamp and I’ll auto-jump here.</div>",
                unsafe_allow_html=True,
            )

        st.markdown("</div>", unsafe_allow_html=True)

# -----------------------------
# Right Card: Chat
# -----------------------------
with right:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### 💬 Chat")

    # Render custom chat history (more aesthetic than default st.chat_message)
    chat_html = ["<div class='chatbox'>"]
    for msg in st.session_state.messages:
        role = msg.get("role", "assistant")
        text = msg.get("text", "")
        ts = msg.get("timestamp_s", None)

        if role == "user":
            chat_html.append(
                f"<div class='msg user'><div class='bubble user'>{text}</div></div>"
            )
        else:
            extra = ""
            if ts is not None:
                extra = f"<div class='meta'>Suggested jump: {float(ts):.2f}s</div>"
            chat_html.append(
                f"<div class='msg assistant'><div class='bubble assistant'>{text}{extra}</div></div>"
            )

    chat_html.append("</div>")
    st.markdown("\n".join(chat_html), unsafe_allow_html=True)

    # Input row
    ask = st.text_input(
        "Ask something about the video",
        placeholder="e.g., 'When does the worker start climbing the stairs?'",
        label_visibility="collapsed",
    )

    b1, b2 = st.columns([0.82, 0.18], gap="small")
    with b1:
        st.markdown("<div class='muted'>Pro tip: you can type “at 12.5s” to test seeking (demo mode).</div>", unsafe_allow_html=True)
    with b2:
        send = st.button("Send", use_container_width=True)

    if send and ask.strip():
        user_q = ask.strip()
        st.session_state.messages.append({"role": "user", "text": user_q})

        # tiny “thinking” effect (feel nicer)
        with st.spinner("Thinking…"):
            time.sleep(0.2)
            res = answer_question(st.session_state.video_bytes, user_q)

        # Apply seek if timestamp returned
        if res.timestamp_s is not None:
            seek_to(res.timestamp_s)

        st.session_state.messages.append(
            {"role": "assistant", "text": res.answer, "timestamp_s": res.timestamp_s}
        )

        st.rerun()

    # Footer note
    st.markdown(
        "<div class='muted'>Integration: Replace <code>answer_question()</code> to call your model and return <code>timestamp_s</code> (seconds) to auto-jump.</div>",
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)