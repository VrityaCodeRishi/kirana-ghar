import React, { useEffect, useMemo, useRef, useState } from "react";

const API_BASE = "http://localhost:8000";

function threadKey(username) {
  const safe = username ? String(username) : "anon";
  return `support_thread_id:${safe}`;
}

export default function SupportChat({ token, user }) {
  const username = user?.username || "customer";
  const storageKey = useMemo(() => threadKey(username), [username]);
  const [threadId, setThreadId] = useState(() => localStorage.getItem(storageKey) || "");
  const [messages, setMessages] = useState(() => [
    { role: "assistant", content: "Hi! How can I help you today?" },
  ]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const listRef = useRef(null);

  useEffect(() => {
    localStorage.setItem(storageKey, threadId || "");
  }, [storageKey, threadId]);

  useEffect(() => {
    if (!listRef.current) return;
    listRef.current.scrollTop = listRef.current.scrollHeight;
  }, [messages, loading]);

  async function send() {
    const text = input.trim();
    if (!text || loading) return;
    setError(null);
    setLoading(true);
    setInput("");
    setMessages((prev) => [...prev, { role: "user", content: text }]);

    try {
      const res = await fetch(`${API_BASE}/support/chat`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({ message: text, thread_id: threadId || null }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.detail || "Support chat failed");

      if (data.thread_id && data.thread_id !== threadId) setThreadId(data.thread_id);
      setMessages((prev) => [...prev, { role: "assistant", content: data.reply }]);
    } catch (e) {
      setError(e.message || "Support chat failed");
      setMessages((prev) => [
        ...prev,
        { role: "assistant", content: "Sorry — I couldn’t reach support right now. Please try again." },
      ]);
    } finally {
      setLoading(false);
    }
  }

  function onKeyDown(e) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      send();
    }
  }

  return (
    <div className="panel support-chat">
      <h2 style={{ marginTop: 0 }}>Customer Support</h2>
      <p className="muted" style={{ marginBottom: 12 }}>
        Chat with support about refunds, complaints, or general questions.
      </p>

      <div className="chat-window" ref={listRef}>
        {messages.map((m, idx) => (
          <div key={idx} className={`chat-bubble ${m.role === "user" ? "user" : "assistant"}`}>
            {m.content}
          </div>
        ))}
        {loading && <div className="chat-bubble assistant">Typing…</div>}
      </div>

      {error && <p className="muted" style={{ marginTop: 10 }}>{error}</p>}

      <div className="chat-input-row">
        <textarea
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={onKeyDown}
          placeholder="Type your message…"
          rows={2}
        />
        <button className="primary-button inline-button" onClick={send} disabled={loading || !input.trim()}>
          Send
        </button>
      </div>
    </div>
  );
}


