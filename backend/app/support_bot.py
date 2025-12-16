import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, TypedDict

from langchain_chroma import Chroma
from langchain_core.documents import Document
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from typing_extensions import Annotated


class SupportState(TypedDict, total=False):
    messages: Annotated[List, add_messages]
    question: str
    route: Literal["general", "refund", "complaint", "handoff"]
    context: str
    answer: str
    active_flow: Optional[Literal["refund", "complaint"]]
    intake_open: bool
    intake: Dict[str, Any]
    missing_fields: List[str]
    recent_orders: List[Dict[str, Any]]
    just_completed: bool
    last_case_id: Optional[str]


def _extract_order_id(message: str) -> Optional[str]:
    """
    Extract an order id from free text.

    Important: avoid false positives like "order was" / "order very".
    Strategy:
    1) Prefer UUID-like ids anywhere in the text.
    2) Otherwise only accept ids that follow explicit markers: "order id" / "order number".
    """
    if not message:
        return None

    # 1) UUID-like
    m = re.search(
        r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b",
        message,
    )
    if m:
        return m.group(0)

    # 2) Explicit marker + token (require digits in the token to avoid plain words)
    m = re.search(
        r"\border\s*(?:id|number)\s*(?:is|=|:|#)?\s*([A-Za-z0-9][A-Za-z0-9\-]{2,})\b",
        message,
        re.IGNORECASE,
    )
    if m:
        token = m.group(1)
        if re.search(r"\d", token):  # must contain at least one digit
            return token

    return None


def _merge_intake(existing: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(existing or {})
    for k, v in (updates or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out[k] = v
    return out


def _compute_missing(fields: List[str], intake: Dict[str, Any]) -> List[str]:
    missing: List[str] = []
    for f in fields:
        v = intake.get(f)
        if v is None or (isinstance(v, str) and not v.strip()):
            missing.append(f)
    return missing


def _ask_for_next_missing(flow: Literal["refund", "complaint"], missing_fields: List[str]) -> str:
    if not missing_fields:
        return ""
    f = missing_fields[0]
    if flow == "refund":
        prompts = {
            "order_id": "Please share your **order number** (if you have one).",
            "payment_method": "Which **payment method** did you use? (UPI / card / COD / wallet)",
            "what_happened": "Briefly tell me what happened (e.g., refund pending, charged twice, wrong amount).",
        }
    else:
        prompts = {
            "order_id": "Please share your **order number** (if you have one). If you don’t have it, just say “I don’t have the order id”.",
            "issue_type": "What’s the issue type? (damaged / wrong item / missing / late / not delivered / delivery executive issue)",
            "description": "Please describe what went wrong in 1–2 lines.",
        }
    return prompts.get(f, f"Please provide: **{f}**")

def _parse_order_selection(message: str, recent_orders: List[Dict[str, Any]]) -> Optional[str]:
    """
    Let users pick an order in natural ways:
    - "latest", "most recent", "first"
    - "2", "second"
    - pasting the order id
    """
    if not recent_orders:
        return None

    text = (message or "").strip().lower()
    oid = _extract_order_id(message or "")
    if oid:
        return oid

    if any(k in text for k in ["latest", "most recent", "recent", "last one", "last order", "newest", "first"]):
        return recent_orders[0].get("id")

    m = re.search(r"\b([1-9])\b", text)
    if m:
        idx = int(m.group(1)) - 1
        if 0 <= idx < len(recent_orders):
            return recent_orders[idx].get("id")

    if "second" in text and len(recent_orders) >= 2:
        return recent_orders[1].get("id")
    if "third" in text and len(recent_orders) >= 3:
        return recent_orders[2].get("id")

    return None


def _looks_like_order_selection(message: str) -> bool:
    text = (message or "").strip().lower()
    if not text:
        return False
    if _extract_order_id(message or ""):
        return True
    if text in {"1", "2", "3"}:
        return True
    if any(
        k in text
        for k in [
            "latest",
            "most recent",
            "recent",
            "last one",
            "last order",
            "newest",
            "first",
            "second",
            "third",
        ]
    ):
        return True
    return False


def _looks_like_followup(flow: Literal["refund", "complaint"], message: str) -> bool:
    text = (message or "").strip().lower()
    if not text:
        return False
    if _looks_like_order_selection(message):
        return True
    if flow == "refund":
        keywords = [
            "refund",
            "money back",
            "charged",
            "charged twice",
            "double charged",
            "deducted",
            "payment failed",
            "failed but",
            "wrong amount",
            "pending",
            "not received",
            "upi",
            "card",
            "cod",
            "wallet",
        ]
    else:
        keywords = [
            "defective",
            "damaged",
            "broken",
            "leak",
            "leaking",
            "expired",
            "spoiled",
            "wrong",
            "incorrect",
            "different product",
            "missing",
            "late",
            "delayed",
            "not delivered",
            "delivery executive",
            "delivery boy",
            "rude",
            "misbehaved",
        ]
    return any(k in text for k in keywords)


def _should_force_complaint_intake(message: str) -> bool:
    """
    Heuristic: treat as complaint intake (not general FAQ) when user wants to file a complaint
    but lacks order id, or is clearly reporting an issue.
    """
    text = (message or "").strip().lower()
    if not text:
        return False

    # If it's a how-to question, keep it general.
    if any(k in text for k in ["how do i", "how to", "policy", "process", "what is the", "return policy", "refund policy"]):
        return False

    complaint_markers = [
        "complain",
        "complaint",
        "defective",
        "damaged",
        "broken",
        "leak",
        "leaking",
        "wrong item",
        "missing",
        "late",
        "delayed",
        "not delivered",
        "delivery executive",
        "rude",
        "misbehaved",
        "marked delivered",
        "didn't receive",
    ]
    no_order_markers = ["don't have the order", "do not have the order", "dont have the order", "no order id", "no order number"]

    if any(m in text for m in no_order_markers) and any(m in text for m in ["complain", "complaint"]):
        return True

    # If they are describing an issue, treat it as complaint intake.
    if any(m in text for m in complaint_markers) and "order" in text:
        return True

    return False


def _resolve_faq_path() -> str:
    p = os.getenv("SUPPORT_FAQ_PATH")
    if p and os.path.exists(p):
        return p
    candidates = [
        os.path.join(os.getcwd(), "customer-support", "faq_knowledge_base.json"),
        os.path.join(os.getcwd(), "faq_knowledge_base.json"),
        os.path.join(os.path.dirname(__file__), "support_faq_knowledge_base.json"),
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    raise FileNotFoundError(
        "FAQ knowledge base not found. Set SUPPORT_FAQ_PATH or place faq_knowledge_base.json in an expected location."
    )


@dataclass
class SupportBot:
    model: str = os.getenv("SUPPORT_MODEL", "gpt-4o-mini")
    temperature: float = float(os.getenv("SUPPORT_TEMPERATURE", "0"))

    def __post_init__(self) -> None:
        self.llm = ChatOpenAI(model=self.model, temperature=self.temperature)
        self.embeddings = OpenAIEmbeddings()
        self.retriever = self._build_retriever()
        self._memory = MemorySaver()
        self.builder = self._build_graph()

    def _build_retriever(self):
        faq_path = _resolve_faq_path()
        with open(faq_path, "r", encoding="utf-8") as f:
            faq = json.load(f)

        documents: list[Document] = []
        for cat in faq.get("categories", []):
            for qa in cat.get("questions", []):
                documents.append(
                    Document(
                        page_content=f"Q: {qa.get('question','')}\nA: {qa.get('answer','')}",
                        metadata={"category": cat.get("category", "unknown"), "id": qa.get("id")},
                    )
                )

        vectorstore = Chroma.from_documents(documents=documents, embedding=self.embeddings)
        return vectorstore.as_retriever(search_kwargs={"k": 3})

    def _build_graph(self):
        general_prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "You are a customer support assistant for Kirana Ghar.\n"
                    "Use ONLY the FAQ context. If the answer is not in the context, say you don't know.\n\n"
                    "FAQ Context:\n{context}",
                ),
                ("human", "{question}"),
            ]
        )
        general_chain = general_prompt | self.llm

        router_prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "You are a router for Kirana Ghar customer support.\n"
                    "Return EXACTLY one word:\n"
                    "general | refund | complaint | handoff\n\n"
                    "Important distinction:\n"
                    "- If the user asks 'how to' / policy / process (e.g. 'what is refund policy', 'how do I return', 'how to file a complaint'), choose general.\n"
                    "- If the user is reporting a specific case about their recent purchase/order (e.g. 'refund for my last order', 'charged twice', 'delivery executive was rude', 'my order is late/damaged/wrong'), choose refund or complaint.\n"
                    "Choose handoff if they ask for a human or you're unsure.",
                ),
                ("human", "{question}"),
            ]
        )
        router_chain = router_prompt | self.llm

        refund_extract_prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "Extract refund-related details from the user's message.\n"
                    "Return ONLY valid JSON with keys:\n"
                    '{ "order_id": string|null, "payment_method": string|null, "what_happened": string|null }\n'
                    "If missing, set null.",
                ),
                ("human", "Existing: {existing}\nUser: {message}"),
            ]
        )
        refund_extract_chain = refund_extract_prompt | self.llm

        complaint_extract_prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "Extract complaint-related details from the user's message.\n"
                    "Return ONLY valid JSON with keys:\n"
                    '{ "order_id": string|null, "issue_type": string|null, "description": string|null }\n'
                    "If missing, set null.\n"
                    "issue_type should be one of: damaged, wrong_item, missing, late, not_delivered, delivery_exec_issue, other",
                ),
                ("human", "Existing: {existing}\nUser: {message}"),
            ]
        )
        complaint_extract_chain = complaint_extract_prompt | self.llm

        def route_node(state: SupportState) -> SupportState:
            q = state["messages"][-1].content
            state["question"] = q
            # Default: not completing an intake on this turn.
            state["just_completed"] = False

            if state.get("intake_open") and state.get("active_flow") in ("refund", "complaint"):
                state["route"] = state["active_flow"]  # type: ignore
                return state
            if state.get("active_flow") in ("refund", "complaint"):
                af: Literal["refund", "complaint"] = state["active_flow"]  # type: ignore
                if _looks_like_followup(af, q):
                    state["route"] = af  # type: ignore
                    state["intake_open"] = True
                    return state

            decision = router_chain.invoke({"question": q}).content.strip().lower()
            if decision not in ("general", "refund", "complaint", "handoff"):
                decision = "handoff"

            # If user is clearly filing a complaint (esp. without order id), force complaint intake.
            if decision == "general" and _should_force_complaint_intake(q):
                decision = "complaint"

            if decision in ("refund", "complaint") and state.get("active_flow") != decision:
                state["intake"] = {}
                state["missing_fields"] = []
            state["route"] = decision  # type: ignore
            return state

        def rag_node(state: SupportState) -> SupportState:
            q = state["question"]
            docs = self.retriever.invoke(q)
            state["context"] = "\n".join([d.page_content for d in docs])
            return state

        def general_node(state: SupportState) -> SupportState:
            resp = general_chain.invoke({"context": state.get("context", ""), "question": state["question"]})
            state["answer"] = resp.content
            state["messages"].append(AIMessage(content=resp.content))
            return state

        def refund_extract_node(state: SupportState) -> SupportState:
            q = state["question"]
            existing = state.get("intake", {})
            state["active_flow"] = "refund"
            state["intake_open"] = True
            try:
                raw = refund_extract_chain.invoke({"existing": json.dumps(existing), "message": q}).content
                updates = json.loads(raw)
                merged = _merge_intake(existing, updates)
            except Exception:
                merged = dict(existing or {})

            if not merged.get("payment_method"):
                t = (q or "").strip().lower()
                if any(k in t for k in ["upi", "gpay", "google pay", "phonepe", "paytm upi"]):
                    merged["payment_method"] = "upi"
                elif any(k in t for k in ["card", "credit", "debit", "visa", "mastercard", "rupay"]):
                    merged["payment_method"] = "card"
                elif any(k in t for k in ["cod", "cash on delivery", "cash"]):
                    merged["payment_method"] = "cod"
                elif any(k in t for k in ["wallet"]):
                    merged["payment_method"] = "wallet"

            if not merged.get("order_id"):
                oid = _extract_order_id(q)
                if oid:
                    merged["order_id"] = oid
                else:
                    picked = _parse_order_selection(q, state.get("recent_orders", []))
                    if picked:
                        merged["order_id"] = picked
            if not merged.get("what_happened"):
                if not (merged.get("payment_method") and (q or "").strip().lower() in ["upi", "card", "cod", "wallet", "cash"]):
                    merged["what_happened"] = q
            else:
                t = (q or "").strip()
                if t and t.lower() not in {"upi", "card", "cod", "wallet", "cash"} and not _looks_like_order_selection(t):
                    prev = str(merged.get("what_happened") or "")
                    if t not in prev:
                        merged["what_happened"] = (prev + " | " + t).strip(" |")

            state["intake"] = merged
            required = ["order_id", "payment_method", "what_happened"]
            state["missing_fields"] = _compute_missing(required, merged)
            return state

        def refund_next_node(state: SupportState) -> SupportState:
            missing = state.get("missing_fields", [])
            if missing:
                if missing[0] == "order_id":
                    recent = state.get("recent_orders", [])
                    if recent:
                        lines = ["Please choose which order this is about (reply with 1/2/3 or paste the order id):"]
                        for i, o in enumerate(recent[:3], 1):
                            items = o.get("items_summary") or ""
                            lines.append(
                                f"{i}) {o.get('id')} — ₹{o.get('total_amount')} — {o.get('status')} — {items}"
                            )
                        msg = "\n".join(lines)
                    else:
                        msg = _ask_for_next_missing("refund", missing)
                else:
                    msg = _ask_for_next_missing("refund", missing)
                state["answer"] = msg
                state["messages"].append(AIMessage(content=msg))
                return state

            intake = state.get("intake", {})
            summary = (
                "Thanks — I’ve captured the refund issue:\n"
                f"- Order: {intake.get('order_id')}\n"
                f"- Payment: {intake.get('payment_method')}\n"
                f"- Issue: {intake.get('what_happened')}\n\n"
                "I’m redirecting this to **customer support** for resolution."
            )
            state["answer"] = summary
            state["messages"].append(AIMessage(content=summary))
            state["intake_open"] = False
            state["just_completed"] = True
            return state

        def complaint_extract_node(state: SupportState) -> SupportState:
            q = state["question"]
            existing = state.get("intake", {})
            state["active_flow"] = "complaint"
            state["intake_open"] = True
            try:
                raw = complaint_extract_chain.invoke({"existing": json.dumps(existing), "message": q}).content
                updates = json.loads(raw)
                merged = _merge_intake(existing, updates)
            except Exception:
                merged = dict(existing or {})

            text = q.lower()
            if not merged.get("order_id"):
                oid = _extract_order_id(q)
                if oid:
                    merged["order_id"] = oid
                elif "order" in text and any(k in text for k in ["don't have", "do not have", "dont have", "no order"]):
                    merged["order_id"] = "unknown"
                else:
                    picked = _parse_order_selection(q, state.get("recent_orders", []))
                    if picked:
                        merged["order_id"] = picked

            if not merged.get("issue_type"):
                if any(k in text for k in ["defective", "defect", "damaged", "broken", "spoiled", "expired", "leak"]):
                    merged["issue_type"] = "damaged"
                elif any(k in text for k in ["wrong item", "different product", "incorrect item", "not what i ordered"]):
                    merged["issue_type"] = "wrong_item"
                elif any(k in text for k in ["missing item", "missing items", "item missing", "short"]):
                    merged["issue_type"] = "missing"
                elif any(k in text for k in ["late", "delayed"]):
                    merged["issue_type"] = "late"
                elif any(k in text for k in ["not delivered", "didn't deliver", "never arrived"]):
                    merged["issue_type"] = "not_delivered"
                elif any(k in text for k in ["delivery executive", "delivery boy", "rude", "misbehaved"]):
                    merged["issue_type"] = "delivery_exec_issue"
                else:
                    merged["issue_type"] = "other"

            if not merged.get("description"):
                merged["description"] = q
            else:
                t = (q or "").strip()
                prev = str(merged.get("description") or "")
                if t and t not in prev and not _looks_like_order_selection(t):
                    merged["description"] = (prev + " | " + t).strip(" |")

            state["intake"] = merged
            required = ["order_id", "issue_type", "description"]
            state["missing_fields"] = _compute_missing(required, merged)
            return state

        def complaint_next_node(state: SupportState) -> SupportState:
            missing = state.get("missing_fields", [])
            if missing:
                if missing[0] == "order_id":
                    recent = state.get("recent_orders", [])
                    if recent:
                        lines = ["Please choose which order this is about (reply with 1/2/3 or paste the order id):"]
                        for i, o in enumerate(recent[:3], 1):
                            items = o.get("items_summary") or ""
                            lines.append(
                                f"{i}) {o.get('id')} — ₹{o.get('total_amount')} — {o.get('status')} — {items}"
                            )
                        lines.append('If you don’t have the order id, reply: "I don’t have the order id".')
                        msg = "\n".join(lines)
                    else:
                        msg = _ask_for_next_missing("complaint", missing)
                else:
                    msg = _ask_for_next_missing("complaint", missing)
                state["answer"] = msg
                state["messages"].append(AIMessage(content=msg))
                return state

            intake = state.get("intake", {})
            summary = (
                "Thanks — I’ve captured the complaint:\n"
                f"- Order: {intake.get('order_id')}\n"
                f"- Issue type: {intake.get('issue_type')}\n"
                f"- Details: {intake.get('description')}\n\n"
                "I’m redirecting this to **customer support** for resolution."
            )
            state["answer"] = summary
            state["messages"].append(AIMessage(content=summary))
            state["intake_open"] = False
            state["just_completed"] = True
            return state

        def handoff_node(state: SupportState) -> SupportState:
            msg = "Redirecting you to customer support. Please share your order number and a short description."
            state["answer"] = msg
            state["messages"].append(AIMessage(content=msg))
            return state

        def pick_route(state: SupportState) -> str:
            return state.get("route", "handoff")

        graph = StateGraph(SupportState)
        graph.add_node("route", route_node)
        graph.add_node("rag", rag_node)
        graph.add_node("general", general_node)
        graph.add_node("refund_extract", refund_extract_node)
        graph.add_node("refund", refund_next_node)
        graph.add_node("complaint_extract", complaint_extract_node)
        graph.add_node("complaint", complaint_next_node)
        graph.add_node("handoff", handoff_node)

        graph.set_entry_point("route")
        graph.add_conditional_edges(
            "route",
            pick_route,
            {"general": "rag", "refund": "refund_extract", "complaint": "complaint_extract", "handoff": "handoff"},
        )
        graph.add_edge("rag", "general")
        graph.add_edge("refund_extract", "refund")
        graph.add_edge("complaint_extract", "complaint")

        graph.add_edge("general", END)
        graph.add_edge("refund", END)
        graph.add_edge("complaint", END)
        graph.add_edge("handoff", END)

        return graph.compile(checkpointer=self._memory)

    def chat(self, *, thread_id: str, message: str, recent_orders: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        state = self.builder.invoke(
            {"messages": [HumanMessage(content=message)], "recent_orders": recent_orders or []},
            config={"configurable": {"thread_id": thread_id}},
        )
        reply = state["messages"][-1].content if state.get("messages") else ""
        return {
            "thread_id": thread_id,
            "reply": reply,
            "route": state.get("route", "handoff"),
            "intake_open": bool(state.get("intake_open", False)),
            "missing_fields": state.get("missing_fields", []),
            "intake": state.get("intake", {}),
            "just_completed": bool(state.get("just_completed", False)),
        }
