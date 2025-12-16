import asyncio
import os
import logging
import sys
import json
import contextlib
from fastapi import FastAPI, HTTPException, Depends, status, Form
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from sqlalchemy import Table, Column, String, Float, Integer, DateTime, MetaData, ForeignKey, text
from sqlalchemy.sql import select
from databases import Database
from passlib.context import CryptContext
from pydantic import BaseModel
from typing import Optional, List
from elasticsearch import AsyncElasticsearch
from elasticsearch import NotFoundError
from redis import asyncio as redis
from aiokafka import AIOKafkaConsumer
import uuid
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware
from .support_bot import SupportBot

DATABASE_URL_DEFAULT = "postgresql+asyncpg://groceryuser:grocerypass@localhost:5432/grocerydb"
DATABASE_URL_WRITE = os.getenv("DATABASE_URL_WRITE") or os.getenv("DATABASE_URL") or DATABASE_URL_DEFAULT
DATABASE_URL_READ = os.getenv("DATABASE_URL_READ") or os.getenv("DATABASE_URL") or DATABASE_URL_DEFAULT

DB_CONNECT_RETRIES = int(os.getenv("DB_CONNECT_RETRIES", "5"))
DB_CONNECT_DELAY = float(os.getenv("DB_CONNECT_DELAY", "2"))

database_write = Database(DATABASE_URL_WRITE)
database_read = Database(DATABASE_URL_READ)
ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
ES_INDEX_SHOPS = os.getenv("ES_INDEX_SHOPS", "kirana.public.shops")
ES_INDEX_PRODUCTS = os.getenv("ES_INDEX_PRODUCTS", "kirana.public.products")
es: Optional[AsyncElasticsearch] = None
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
redis_client: Optional[redis.Redis] = None
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
KAFKA_CACHE_TOPICS = [
    t for t in os.getenv("KAFKA_CACHE_TOPICS", "kirana.public.shops,kirana.public.products").split(",") if t
]
metadata = MetaData()

pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

logger = logging.getLogger("kirana.db")
if not logger.handlers:
    _h = logging.StreamHandler(stream=sys.stdout)
    _h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)
logger.propagate = False

users = Table(
    "users",
    metadata,
    Column("username", String, primary_key=True),
    Column("hashed_password", String),
    Column("role", String),
    Column("full_name", String),
    Column("address", String),
    Column("mobile", String),
)

shops = Table(
    "shops",
    metadata,
    Column("id", String, primary_key=True),
    Column("name", String),
    Column("owner", String, ForeignKey("users.username")),
    Column("city", String),
)

products = Table(
    "products",
    metadata,
    Column("id", String, primary_key=True),
    Column("shop_id", String, ForeignKey("shops.id")),
    Column("name", String),
    Column("price", Float),
)

orders = Table(
    "orders",
    metadata,
    Column("id", String, primary_key=True),
    Column("customer_username", String, ForeignKey("users.username"), nullable=False),
    Column("shop_id", String, ForeignKey("shops.id"), nullable=False),
    Column("status", String, nullable=False),
    Column("payment_method", String, nullable=False),
    Column("payment_status", String, nullable=False),
    Column("total_amount", Float, nullable=False),
    Column("created_at", DateTime, nullable=False),
    Column("updated_at", DateTime, nullable=False),
)

order_items = Table(
    "order_items",
    metadata,
    Column("id", String, primary_key=True),
    Column("order_id", String, ForeignKey("orders.id"), nullable=False),
    Column("product_id", String, ForeignKey("products.id"), nullable=False),
    Column("product_name_snapshot", String, nullable=False),
    Column("unit_price", Float, nullable=False),
    Column("quantity", Integer, nullable=False),
    Column("line_total", Float, nullable=False),
)

support_cases = Table(
    "support_cases",
    metadata,
    Column("id", String, primary_key=True),
    Column("thread_id", String, nullable=False),
    Column("customer_username", String, ForeignKey("users.username"), nullable=False),
    Column("order_id", String, nullable=True),
    Column("case_type", String, nullable=False),  # refund|complaint
    Column("status", String, nullable=False),  # open|closed
    Column("payload_json", String, nullable=False),
    Column("created_at", DateTime, nullable=False),
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class UserIn(BaseModel):
    username: str
    password: str
    role: str
    full_name: str
    address: str
    mobile: str

class UserOut(BaseModel):
    username: str
    role: str
    full_name: str | None = None
    address: str | None = None
    mobile: str | None = None


class ShopOut(BaseModel):
    id: str
    name: str
    owner: str
    city: str | None = None


class ProductIn(BaseModel):
    name: str
    price: float


class ProductOut(ProductIn):
    id: str
    shop_id: str


class OrderItemIn(BaseModel):
    product_id: str
    quantity: int = 1


class CreateOrderRequest(BaseModel):
    shop_id: str
    items: list[OrderItemIn]
    payment_method: str = "cod"  # cod/upi/card/wallet


class OrderItemOut(BaseModel):
    id: str
    product_id: str
    product_name_snapshot: str
    unit_price: float
    quantity: int
    line_total: float


class OrderOut(BaseModel):
    id: str
    shop_id: str
    customer_username: str
    status: str
    payment_method: str
    payment_status: str
    total_amount: float
    created_at: datetime
    updated_at: datetime


class OrderDetailOut(OrderOut):
    items: list[OrderItemOut]


class UpdateOrderStatusRequest(BaseModel):
    status: str


class SupportChatRequest(BaseModel):
    message: str
    thread_id: str | None = None


class SupportChatResponse(BaseModel):
    thread_id: str
    reply: str
    route: str
    intake_open: bool = False
    missing_fields: list[str] = []
    case_id: str | None = None


async def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


async def get_password_hash(password):
    return pwd_context.hash(password)


async def get_user(username: str, use_master: bool = False):
    query = users.select().where(users.c.username == username)
    db = database_write if use_master else database_read
    logger.info("DB READ from %s: users WHERE username='%s'", "MASTER" if use_master else "REPLICA", username)
    return await db.fetch_one(query)


async def authenticate_user(username: str, password: str):
    user = await get_user(username, use_master=True)
    if user and await verify_password(password, user["hashed_password"]):
        return user
    return False


async def get_shop(shop_id: str, use_master: bool = False):
    query = shops.select().where(shops.c.id == shop_id)
    db = database_write if use_master else database_read
    logger.info("DB READ from %s: shops WHERE id='%s'", "MASTER" if use_master else "REPLICA", shop_id)
    return await db.fetch_one(query)


async def get_product(product_id: str, use_master: bool = False):
    query = products.select().where(products.c.id == product_id)
    db = database_write if use_master else database_read
    logger.info("DB READ from %s: products WHERE id='%s'", "MASTER" if use_master else "REPLICA", product_id)
    return await db.fetch_one(query)


async def connect_with_retry():
    if database_read.is_connected and database_write.is_connected:
        return

    for attempt in range(1, DB_CONNECT_RETRIES + 1):
        try:
            if not database_write.is_connected:
                await database_write.connect()
            if not database_read.is_connected:
                await database_read.connect()
            return
        except Exception:
            if attempt == DB_CONNECT_RETRIES:
                raise
            await asyncio.sleep(DB_CONNECT_DELAY)


async def cache_get(key: str):
    if not redis_client:
        return None
    try:
        raw = await redis_client.get(key)
        if raw is None:
            return None
        return json.loads(raw)
    except Exception as exc:
        logger.warning("Redis get failed for key %s: %s", key, exc)
        return None


async def cache_set(key: str, value, ttl: int = 60):
    if not redis_client:
        return
    try:
        await redis_client.set(key, json.dumps(value), ex=ttl)
    except Exception as exc:
        logger.warning("Redis set failed for key %s: %s", key, exc)


async def cache_del(*keys: str):
    if not redis_client or not keys:
        return
    try:
        await redis_client.delete(*keys)
    except Exception as exc:
        logger.warning("Redis delete failed for keys %s: %s", keys, exc)


async def handle_cache_invalidation_message(topic: str, raw_value: Optional[str]):
    if not redis_client or raw_value is None:
        return
    try:
        data = json.loads(raw_value)
    except Exception as exc:
        logger.warning("Kafka message JSON decode failed: %s", exc)
        return

    if isinstance(data, dict) and "payload" in data:
        payload = data.get("payload") or {}
    else:
        payload = data

    if not isinstance(payload, dict):
        return

    keys_to_del: list[str] = []

    doc_ref = None

    if topic.endswith(".shops"):
        shop_id = payload.get("id")
        if shop_id:
            doc_ref = f"shop_id={shop_id}"
        keys_to_del.append("shops:customer")
        if shop_id:
            keys_to_del.append(f"products:{shop_id}")
    elif topic.endswith(".products"):
        shop_id = payload.get("shop_id")
        if shop_id:
            doc_ref = f"product in shop_id={shop_id}"
        if shop_id:
            keys_to_del.append(f"products:{shop_id}")

    if keys_to_del:
        await cache_del(*keys_to_del)
        logger.info(
            "CDC event from topic %s (%s): invalidated Redis keys %s; corresponding Elasticsearch document is updated by the ES sink connector",
            topic,
            doc_ref or "unknown id",
            keys_to_del,
        )


async def kafka_cache_consumer():
    if not KAFKA_BOOTSTRAP_SERVERS:
        logger.info("KAFKA_BOOTSTRAP_SERVERS not set; skipping Kafka cache consumer")
        return

    loop = asyncio.get_event_loop()
    consumer = AIOKafkaConsumer(
        *KAFKA_CACHE_TOPICS,
        loop=loop,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        value_deserializer=lambda v: v.decode("utf-8") if v is not None else None,
    )

    try:
        await consumer.start()
        logger.info("Kafka cache consumer started for topics: %s", ", ".join(KAFKA_CACHE_TOPICS))
    except Exception as exc:
        logger.warning("Kafka cache consumer failed to start: %s", exc)
        return

    try:
        async for msg in consumer:
            try:
                await handle_cache_invalidation_message(msg.topic, msg.value)
            except Exception as exc:
                logger.warning("Error processing Kafka message from %s: %s", msg.topic, exc)
    except asyncio.CancelledError:
        logger.info("Kafka cache consumer cancelled; shutting down")
    except Exception as exc:
        logger.warning("Kafka cache consumer stopped due to error: %s", exc)
    finally:
        await consumer.stop()


async def ensure_publication():
    stmt = text(
        """
        DO $$
        BEGIN
           IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication') THEN
              CREATE PUBLICATION dbz_publication FOR TABLE public.shops, public.products;
           ELSE
              BEGIN
                 ALTER PUBLICATION dbz_publication SET TABLE public.shops, public.products;
              EXCEPTION WHEN undefined_table THEN
                 NULL;
              END;
           END IF;
        END$$;
        """
    )
    await database_write.execute(stmt)
    logger.info("Ensured Debezium publication 'dbz_publication' exists and is configured")


@app.on_event("startup")
async def startup():
    await connect_with_retry()
    schema_statements = [
        """
        CREATE TABLE IF NOT EXISTS users (
          username VARCHAR PRIMARY KEY,
          hashed_password VARCHAR NOT NULL,
          role VARCHAR NOT NULL,
          full_name VARCHAR,
          address VARCHAR,
          mobile VARCHAR
        );
        """,
        "ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS full_name VARCHAR;",
        "ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS address VARCHAR;",
        "ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS mobile VARCHAR;",
        """
        CREATE TABLE IF NOT EXISTS shops (
          id VARCHAR PRIMARY KEY,
          name VARCHAR NOT NULL,
          owner VARCHAR REFERENCES users(username),
          city VARCHAR
        );
        """,
        "ALTER TABLE IF EXISTS shops ADD COLUMN IF NOT EXISTS city VARCHAR;",
        """
        CREATE TABLE IF NOT EXISTS products (
          id VARCHAR PRIMARY KEY,
          shop_id VARCHAR REFERENCES shops(id),
          name VARCHAR NOT NULL,
          price FLOAT NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS orders (
          id VARCHAR PRIMARY KEY,
          customer_username VARCHAR NOT NULL REFERENCES users(username),
          shop_id VARCHAR NOT NULL REFERENCES shops(id),
          status VARCHAR NOT NULL,
          payment_method VARCHAR NOT NULL,
          payment_status VARCHAR NOT NULL,
          total_amount FLOAT NOT NULL,
          created_at TIMESTAMP NOT NULL,
          updated_at TIMESTAMP NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS order_items (
          id VARCHAR PRIMARY KEY,
          order_id VARCHAR NOT NULL REFERENCES orders(id),
          product_id VARCHAR NOT NULL REFERENCES products(id),
          product_name_snapshot VARCHAR NOT NULL,
          unit_price FLOAT NOT NULL,
          quantity INT NOT NULL,
          line_total FLOAT NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS support_cases (
          id VARCHAR PRIMARY KEY,
          thread_id VARCHAR NOT NULL,
          customer_username VARCHAR NOT NULL REFERENCES users(username),
          order_id VARCHAR,
          case_type VARCHAR NOT NULL,
          status VARCHAR NOT NULL,
          payload_json VARCHAR NOT NULL,
          created_at TIMESTAMP NOT NULL
        );
        """,
        "ALTER TABLE IF EXISTS support_cases ADD COLUMN IF NOT EXISTS thread_id VARCHAR;",
    ]
    for statement in schema_statements:
        await database_write.execute(text(statement))
    await ensure_publication()
    global es, redis_client
    try:
        es = AsyncElasticsearch(ES_URL)
        info = await es.info()
        logger.info("Elasticsearch connected: %s", info.get("version", {}))
    except Exception as exc:
        logger.warning("Elasticsearch not reachable at %s: %s", ES_URL, exc)

    try:
        redis_client = redis.from_url(REDIS_URL)
        await redis_client.ping()
        logger.info("Redis connected at %s", REDIS_URL)
    except Exception as exc:
        logger.warning("Redis not reachable at %s: %s", REDIS_URL, exc)
    try:
        app.state.kafka_task = asyncio.create_task(kafka_cache_consumer())
    except Exception as exc:
        logger.warning("Failed to start Kafka cache consumer task: %s", exc)


@app.on_event("shutdown")
async def shutdown():
    await database_read.disconnect()
    await database_write.disconnect()
    if es:
        await es.close()
    if redis_client:
        await redis_client.close()
    kafka_task = getattr(app.state, "kafka_task", None)
    if kafka_task:
        kafka_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await kafka_task


support_bot: SupportBot | None = None


@app.on_event("startup")
async def init_support_bot():
    # This is intentionally lightweight: FAQ is embedded via env path resolution
    # and Chroma is in-memory. If OPENAI_API_KEY is missing, requests will fail
    # with a clear error.
    global support_bot
    try:
        support_bot = SupportBot()
        logger.info("SupportBot initialized")
    except Exception as exc:
        support_bot = None
        logger.warning("SupportBot failed to initialize: %s", exc)


@app.post("/register", status_code=201)
async def register(user: UserIn):
    user_exist = await get_user(user.username)
    if user_exist:
        raise HTTPException(status_code=400, detail="Username already taken")
    hashed_password = await get_password_hash(user.password)
    if not user.mobile.isdigit():
        raise HTTPException(status_code=400, detail="Mobile must be digits only")
    query = users.insert().values(
        username=user.username,
        hashed_password=hashed_password,
        role=user.role,
        full_name=user.full_name,
        address=user.address,
        mobile=user.mobile,
    )
    await database_write.execute(query)
    logger.info("DB WRITE to MASTER: users INSERT username='%s' role='%s'", user.username, user.role)
    return {"msg": "User registered successfully"}


@app.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = await authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    return {"access_token": user["username"], "token_type": "bearer"}


async def get_current_user(token: str = Depends(oauth2_scheme)):
    user = await get_user(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid authentication")
    return user


@app.get("/me", response_model=UserOut)
async def read_me(current_user=Depends(get_current_user)):
    return {
        "username": current_user["username"],
        "role": current_user["role"],
        "full_name": current_user["full_name"],
        "address": current_user["address"],
        "mobile": current_user["mobile"],
    }


@app.post("/orders", response_model=OrderDetailOut, status_code=201)
async def create_order(payload: CreateOrderRequest, current_user=Depends(get_current_user)):
    if current_user["role"] != "customer":
        raise HTTPException(status_code=403, detail="Only customers can place orders")
    if not payload.items:
        raise HTTPException(status_code=400, detail="Order must have at least one item")
    if any(i.quantity <= 0 for i in payload.items):
        raise HTTPException(status_code=400, detail="Quantity must be >= 1")

    shop = await get_shop(payload.shop_id, use_master=False)
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    product_ids = list({it.product_id for it in payload.items})
    # Fetch product rows
    prod_rows = await database_read.fetch_all(products.select().where(products.c.id.in_(product_ids)))
    prod_map = {r["id"]: r for r in prod_rows}
    if len(prod_map) != len(product_ids):
        raise HTTPException(status_code=400, detail="One or more products not found")
    # Validate all products belong to the shop
    for pid, row in prod_map.items():
        if row["shop_id"] != payload.shop_id:
            raise HTTPException(status_code=400, detail=f"Product {pid} does not belong to this shop")

    now = datetime.utcnow()
    order_id = str(uuid.uuid4())
    status_val = "created"
    payment_method = (payload.payment_method or "cod").lower()
    if payment_method not in ("cod", "upi", "card", "wallet"):
        raise HTTPException(status_code=400, detail="Invalid payment_method")
    payment_status = "pending" if payment_method != "cod" else "pending"

    items_out: list[dict] = []
    total_amount = 0.0

    # Build items + totals from authoritative DB prices
    for it in payload.items:
        p = prod_map[it.product_id]
        unit_price = float(p["price"])
        qty = int(it.quantity)
        line_total = unit_price * qty
        total_amount += line_total
        items_out.append(
            {
                "id": str(uuid.uuid4()),
                "order_id": order_id,
                "product_id": p["id"],
                "product_name_snapshot": p["name"],
                "unit_price": unit_price,
                "quantity": qty,
                "line_total": line_total,
            }
        )

    # Persist order + items
    await database_write.execute(
        orders.insert().values(
            id=order_id,
            customer_username=current_user["username"],
            shop_id=payload.shop_id,
            status=status_val,
            payment_method=payment_method,
            payment_status=payment_status,
            total_amount=total_amount,
            created_at=now,
            updated_at=now,
        )
    )
    await database_write.execute_many(order_items.insert(), items_out)
    logger.info("DB WRITE to MASTER: orders INSERT id='%s' customer='%s' shop='%s'", order_id, current_user["username"], payload.shop_id)

    return {
        "id": order_id,
        "shop_id": payload.shop_id,
        "customer_username": current_user["username"],
        "status": status_val,
        "payment_method": payment_method,
        "payment_status": payment_status,
        "total_amount": total_amount,
        "created_at": now,
        "updated_at": now,
        "items": [
            {
                "id": it["id"],
                "product_id": it["product_id"],
                "product_name_snapshot": it["product_name_snapshot"],
                "unit_price": it["unit_price"],
                "quantity": it["quantity"],
                "line_total": it["line_total"],
            }
            for it in items_out
        ],
    }


@app.get("/orders/me", response_model=list[OrderOut])
async def list_my_orders(current_user=Depends(get_current_user), limit: int = 20):
    if current_user["role"] != "customer":
        raise HTTPException(status_code=403, detail="Only customers can view their orders")
    limit = max(1, min(limit, 50))
    q = (
        orders.select()
        .where(orders.c.customer_username == current_user["username"])
        .order_by(orders.c.created_at.desc())
        .limit(limit)
    )
    rows = await database_read.fetch_all(q)
    return [dict(r) for r in rows]


@app.get("/orders/{order_id}", response_model=OrderDetailOut)
async def get_order_details(order_id: str, current_user=Depends(get_current_user)):
    row = await database_read.fetch_one(orders.select().where(orders.c.id == order_id))
    if not row:
        raise HTTPException(status_code=404, detail="Order not found")

    # Auth: customer can see own; shopowner can see orders for their shop
    if current_user["role"] == "customer":
        if row["customer_username"] != current_user["username"]:
            raise HTTPException(status_code=403, detail="Not authorized to view this order")
    elif current_user["role"] == "shopowner":
        shop = await get_shop(row["shop_id"], use_master=True)
        if not shop or shop["owner"] != current_user["username"]:
            raise HTTPException(status_code=403, detail="Not authorized to view this order")
    else:
        raise HTTPException(status_code=403, detail="Not authorized")

    items = await database_read.fetch_all(order_items.select().where(order_items.c.order_id == order_id))
    return {**dict(row), "items": [dict(i) for i in items]}


@app.post("/orders/{order_id}/cancel", response_model=OrderOut)
async def cancel_order(order_id: str, current_user=Depends(get_current_user)):
    if current_user["role"] != "customer":
        raise HTTPException(status_code=403, detail="Only customers can cancel orders")
    row = await database_read.fetch_one(orders.select().where(orders.c.id == order_id))
    if not row:
        raise HTTPException(status_code=404, detail="Order not found")
    if row["customer_username"] != current_user["username"]:
        raise HTTPException(status_code=403, detail="Not authorized to cancel this order")
    if row["status"] not in ("created", "confirmed"):
        raise HTTPException(status_code=400, detail="Order cannot be cancelled at this stage")

    now = datetime.utcnow()
    await database_write.execute(
        orders.update().where(orders.c.id == order_id).values(status="cancelled", updated_at=now)
    )
    updated = await database_read.fetch_one(orders.select().where(orders.c.id == order_id))
    return dict(updated)


@app.patch("/orders/{order_id}/status", response_model=OrderOut)
async def update_order_status(order_id: str, payload: UpdateOrderStatusRequest, current_user=Depends(get_current_user)):
    if current_user["role"] != "shopowner":
        raise HTTPException(status_code=403, detail="Only shop owners can update order status")
    row = await database_read.fetch_one(orders.select().where(orders.c.id == order_id))
    if not row:
        raise HTTPException(status_code=404, detail="Order not found")
    shop = await get_shop(row["shop_id"], use_master=True)
    if not shop or shop["owner"] != current_user["username"]:
        raise HTTPException(status_code=403, detail="Not authorized to update this order")

    new_status = (payload.status or "").lower()
    allowed = {"confirmed", "packed", "shipped", "delivered", "cancelled"}
    if new_status not in allowed:
        raise HTTPException(status_code=400, detail=f"Invalid status. Allowed: {sorted(allowed)}")

    now = datetime.utcnow()
    await database_write.execute(
        orders.update().where(orders.c.id == order_id).values(status=new_status, updated_at=now)
    )
    updated = await database_read.fetch_one(orders.select().where(orders.c.id == order_id))
    return dict(updated)


@app.post("/support/chat", response_model=SupportChatResponse)
async def support_chat(payload: SupportChatRequest, current_user=Depends(get_current_user)):
    if current_user["role"] != "customer":
        raise HTTPException(status_code=403, detail="Support chatbot is only available to customers")
    if not support_bot:
        raise HTTPException(status_code=503, detail="Support chatbot unavailable")
    msg = (payload.message or "").strip()
    if not msg:
        raise HTTPException(status_code=400, detail="message is required")

    # If thread_id not provided, create a stable per-user thread for the browser session.
    # Frontend should store the returned thread_id and reuse it on subsequent calls.
    thread_id = payload.thread_id or f"{current_user['username']}:{uuid.uuid4()}"

    # Tool-like context: recent orders for this customer (top 3), with item summaries.
    recent_orders: list[dict] = []
    try:
        o_rows = await database_read.fetch_all(
            orders.select()
            .where(orders.c.customer_username == current_user["username"])
            .order_by(orders.c.created_at.desc())
            .limit(3)
        )
        order_ids = [r["id"] for r in o_rows]
        items_by_order: dict[str, list[str]] = {oid: [] for oid in order_ids}
        if order_ids:
            it_rows = await database_read.fetch_all(order_items.select().where(order_items.c.order_id.in_(order_ids)))
            for it in it_rows:
                items_by_order.setdefault(it["order_id"], []).append(
                    f"{it['product_name_snapshot']}×{it['quantity']}"
                )
        for r in o_rows:
            oid = r["id"]
            recent_orders.append(
                {
                    "id": oid,
                    "status": r["status"],
                    "total_amount": float(r["total_amount"]),
                    "items_summary": ", ".join(items_by_order.get(oid, [])[:3]),
                }
            )
        logger.info(
            "Support tools: fetched %d recent orders for customer '%s'",
            len(recent_orders),
            current_user["username"],
        )
    except Exception as exc:
        logger.info("Failed to fetch recent orders for support tools: %s", exc)

    try:
        out = support_bot.chat(thread_id=thread_id, message=msg, recent_orders=recent_orders)
    except Exception as exc:
        logger.warning("SupportBot chat failed: %s", exc)
        raise HTTPException(status_code=503, detail="Support chatbot unavailable")

    logger.info(
        "SupportBot result for customer '%s': route=%s intake_open=%s missing_fields=%s",
        current_user["username"],
        out.get("route", "unknown"),
        bool(out.get("intake_open", False)),
        out.get("missing_fields", []),
    )

    case_id: str | None = None
    # If a refund/complaint intake has completed, persist a support case with order snapshot.
    try:
        route = out.get("route")
        intake_open = bool(out.get("intake_open", False))
        intake = out.get("intake", {}) or {}
        if route in ("refund", "complaint") and bool(out.get("just_completed")) and not intake_open and intake:
            # Dedupe: if we already created an open case for this thread+type, don't create another.
            # Use MASTER here to avoid duplicate case creation due to replica lag.
            existing = await database_write.fetch_one(
                support_cases.select()
                .where(support_cases.c.thread_id == thread_id)
                .where(support_cases.c.case_type == str(route))
                .where(support_cases.c.status == "open")
                .order_by(support_cases.c.created_at.desc())
                .limit(1)
            )
            if existing:
                case_id = existing["id"]
                logger.info(
                    "Support case: skipping duplicate create; existing case_id='%s' thread_id='%s' customer='%s'",
                    case_id,
                    thread_id,
                    current_user["username"],
                )
                # still return case_id; do not overwrite the existing record
                raise StopIteration

            now = datetime.utcnow()
            order_id = intake.get("order_id")

            order_snapshot: dict | None = None
            if isinstance(order_id, str) and order_id and order_id != "unknown":
                logger.info(
                    "Support case: attempting order snapshot fetch for customer '%s' order_id='%s'",
                    current_user["username"],
                    order_id,
                )
                # Use MASTER for order snapshot reads to avoid replica lag right after order creation.
                o = await database_write.fetch_one(
                    orders.select()
                    .where(orders.c.id == order_id)
                    .where(orders.c.customer_username == current_user["username"])
                )
                if o:
                    it_rows = await database_write.fetch_all(
                        order_items.select().where(order_items.c.order_id == order_id)
                    )
                    order_snapshot = {
                        "id": o["id"],
                        "shop_id": o["shop_id"],
                        "status": o["status"],
                        "payment_method": o["payment_method"],
                        "payment_status": o["payment_status"],
                        "total_amount": float(o["total_amount"]),
                        "created_at": (o["created_at"].isoformat() if o["created_at"] else None),
                        "items": [
                            {
                                "product_id": it["product_id"],
                                "name": it["product_name_snapshot"],
                                "unit_price": float(it["unit_price"]),
                                "quantity": int(it["quantity"]),
                                "line_total": float(it["line_total"]),
                            }
                            for it in it_rows
                        ],
                    }
                    logger.info(
                        "Support case: fetched order snapshot for order_id='%s' (items=%d, total=%.2f)",
                        order_id,
                        len(it_rows),
                        float(o["total_amount"]),
                    )
                else:
                    logger.info(
                        "Support case: order snapshot NOT found/authorized for customer '%s' order_id='%s'",
                        current_user["username"],
                        order_id,
                    )

            case_payload = {
                "route": route,
                "user": {"username": current_user["username"]},
                "intake": intake,
                "order": order_snapshot,
                "created_at": now.isoformat(),
            }
            case_id = str(uuid.uuid4())
            await database_write.execute(
                support_cases.insert().values(
                    id=case_id,
                    thread_id=thread_id,
                    customer_username=current_user["username"],
                    order_id=order_id if isinstance(order_id, str) else None,
                    case_type=str(route),
                    status="open",
                    payload_json=json.dumps(case_payload),
                    created_at=now,
                )
            )
            logger.info("Created support_case id='%s' type='%s' customer='%s'", case_id, route, current_user["username"])
    except StopIteration:
        pass
    except Exception as exc:
        logger.warning("Failed to persist support case: %s", exc)

    return {
        "thread_id": out["thread_id"],
        "reply": out["reply"],
        "route": out.get("route", "handoff"),
        "intake_open": bool(out.get("intake_open", False)),
        "missing_fields": out.get("missing_fields", []),
        "case_id": case_id,
    }


@app.post("/shops/", response_model=ShopOut)
async def create_shop(
    name: str = Form(...),
    city: str = Form(...),
    current_user=Depends(get_current_user),
):
    if current_user["role"] != "shopowner":
        raise HTTPException(status_code=403, detail="Only shop owners can create shops")
    shop_id = str(uuid.uuid4())
    query = shops.insert().values(
        id=shop_id, name=name, owner=current_user["username"], city=city
    )
    await database_write.execute(query)
    logger.info("DB WRITE to MASTER: shops INSERT id='%s' name='%s' city='%s'", shop_id, name, city)
    return {
        "id": shop_id,
        "name": name,
        "owner": current_user["username"],
        "city": city,
    }


@app.get("/shops/", response_model=List[ShopOut])
async def list_shops(current_user=Depends(get_current_user)):
    if current_user["role"] == "shopowner":
        query = shops.select().where(shops.c.owner == current_user["username"])
        logger.info("DB READ from MASTER: shops for owner '%s'", current_user["username"]) 
        return await database_write.fetch_all(query)
    else:
        cache_key = "shops:customer"
        cached = await cache_get(cache_key)
        if cached is not None:
            logger.info("Cache HIT for %s", cache_key)
            return cached
        query = shops.select()
        logger.info("Cache MISS for %s; querying DB", cache_key)
        logger.info("DB READ from REPLICA: all shops for customer '%s'", current_user["username"]) 
        rows = await database_read.fetch_all(query)
        await cache_set(cache_key, [dict(r) for r in rows], ttl=60)
        return rows
    
@app.get("/search")
async def search(q: str, type: str = "all", city: Optional[str] = None, size: int = 10, current_user=Depends(get_current_user)):
    if not es:
        raise HTTPException(status_code=503, detail="Search unavailable")

    indices = []
    if type in ("all", "shop"):
        indices.append(ES_INDEX_SHOPS)
    if type in ("all", "product"):
        indices.append(ES_INDEX_PRODUCTS)
    if not indices:
        indices = [ES_INDEX_SHOPS, ES_INDEX_PRODUCTS]

    must_clause = [{
        "multi_match": {
            "query": q,
            "fields": ["name^3", "shop_name^2", "shop_city"],
            "type": "most_fields",
            "fuzziness": "AUTO"
        }
    }]
    filters = []
    if city:
        filters.append({"bool": {"should": [
            {"term": {"city.keyword": city}},
            {"term": {"shop_city.keyword": city}}
        ], "minimum_should_match": 1}})

    body = {"query": {"bool": {"must": must_clause, "filter": filters}}, "size": max(1, min(size, 50))}

    try:
        resp = await es.search(index=",".join(indices), body=body)
    except NotFoundError as exc:
        logger.info("ES index missing during search (%s); returning empty results", exc.info.get("index", "unknown"))
        return {"count": 0, "results": []}
    except Exception as exc:
        logger.warning("ES search failed: %s", exc)
        raise HTTPException(status_code=503, detail="Search unavailable")

    hits = resp.get("hits", {}).get("hits", [])
    results = [
        {
            "_index": h.get("_index"),
            "_id": h.get("_id"),
            "_score": h.get("_score"),
            **(h.get("_source", {}) or {}),
        }
        for h in hits
    ]
    return {"count": len(results), "results": results}


@app.get("/shops/{shop_id}", response_model=ShopOut)
async def get_shop_details(shop_id: str, current_user=Depends(get_current_user)):
    if current_user["role"] == "shopowner":
        row = await get_shop(shop_id, use_master=True)
        if not row:
            raise HTTPException(status_code=404, detail="Shop not found")
        if row["owner"] != current_user["username"]:
            raise HTTPException(status_code=403, detail="Not authorized to view this shop")
        logger.info("DB READ from MASTER: shop '%s' details for owner '%s'", shop_id, current_user["username"]) 
        return row
    else:
        row = await get_shop(shop_id, use_master=False)
        if not row:
            raise HTTPException(status_code=404, detail="Shop not found")
        logger.info("DB READ from REPLICA: shop '%s' details for customer '%s'", shop_id, current_user["username"]) 
        return row
    
@app.get("/shops/{shop_id}/products/", response_model=List[ProductOut])
async def list_products(shop_id: str, current_user=Depends(get_current_user)):
    shop = await get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
    query = products.select().where(products.c.shop_id == shop_id)
    if current_user["role"] == "shopowner":
        if shop["owner"] != current_user["username"]:
            raise HTTPException(status_code=403, detail="Not authorized to view this shop")
        logger.info("DB READ from MASTER: products for shop '%s' (owner '%s')", shop_id, current_user["username"]) 
        return await database_write.fetch_all(query)
    else:
        cache_key = f"products:{shop_id}"
        cached = await cache_get(cache_key)
        if cached is not None:
            logger.info("Cache HIT for %s", cache_key)
            return cached
        logger.info("Cache MISS for %s; querying DB", cache_key)
        logger.info("DB READ from REPLICA: products for shop '%s' (viewer '%s')", shop_id, current_user["username"]) 
        rows = await database_read.fetch_all(query)
        await cache_set(cache_key, [dict(r) for r in rows], ttl=60)
        return rows


@app.get("/debug/db")
async def debug_db(current_user=Depends(get_current_user)):
    read_row = await database_read.fetch_one(
        text(
            "SELECT inet_server_addr()::text AS ip, inet_server_port() AS port, pg_is_in_recovery() AS is_replica"
        )
    )
    write_row = await database_write.fetch_one(
        text(
            "SELECT inet_server_addr()::text AS ip, inet_server_port() AS port, pg_is_in_recovery() AS is_replica"
        )
    )
    return {
        "user": {"username": current_user["username"], "role": current_user["role"]},
        "read_db": {
            "ip": read_row["ip"],
            "port": read_row["port"],
            "is_replica": read_row["is_replica"],
        },
        "write_db": {
            "ip": write_row["ip"],
            "port": write_row["port"],
            "is_replica": write_row["is_replica"],
        },
    }

@app.post("/shops/{shop_id}/products/", response_model=ProductOut, status_code=201)
async def add_product(shop_id: str, product: ProductIn, current_user=Depends(get_current_user)):
    shop = await get_shop(shop_id, use_master=True)
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
    if shop["owner"] != current_user["username"]:
        raise HTTPException(status_code=403, detail="Not authorized to modify this shop")
    product_id = str(uuid.uuid4())
    query = products.insert().values(
        id=product_id,
        shop_id=shop_id,
        name=product.name,
        price=product.price,
    )
    await database_write.execute(query)
    logger.info("DB WRITE to MASTER: products INSERT id='%s' shop_id='%s' name='%s'", product_id, shop_id, product.name)
    return {"id": product_id, "shop_id": shop_id, "name": product.name, "price": product.price}

@app.delete("/shops/{shop_id}/", status_code=204)
async def delete_shop(shop_id: str, current_user=Depends(get_current_user)):
    shop = await get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
    if shop["owner"] != current_user["username"]:
        raise HTTPException(status_code=403, detail="Not authorized to delete this shop")

    await database_write.execute(products.delete().where(products.c.shop_id == shop_id))
    await database_write.execute(shops.delete().where(shops.c.id == shop_id))
    logger.info("DB WRITE to MASTER: deleted shop '%s' and its products", shop_id)
    return


@app.delete("/shops/{shop_id}/products/{product_id}/", status_code=204)
async def delete_product(shop_id: str, product_id: str, current_user=Depends(get_current_user)):
    shop = await get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
    if shop["owner"] != current_user["username"]:
        raise HTTPException(status_code=403, detail="Not authorized to modify this shop")
    product = await get_product(product_id, use_master=True)
    if not product or product["shop_id"] != shop_id:
        raise HTTPException(status_code=404, detail="Product not found in this shop")
    await database_write.execute(products.delete().where(products.c.id == product_id))
    logger.info("DB WRITE to MASTER: deleted product '%s' from shop '%s'", product_id, shop_id)


@app.put("/shops/{shop_id}/products/{product_id}/", response_model=ProductOut)
async def update_product(
    shop_id: str,
    product_id: str,
    payload: ProductIn,
    current_user=Depends(get_current_user),
):
    shop = await get_shop(shop_id)
    if not shop:
        raise HTTPException(status=status.HTTP_404_NOT_FOUND, detail="Shop not found")
    if shop["owner"] != current_user["username"]:
        raise HTTPException(status_code=403, detail="Not authorized to modify this shop")

    product = await get_product(product_id, use_master=True)
    if not product or product["shop_id"] != shop_id:
        raise HTTPException(status=status.HTTP_404_NOT_FOUND, detail="Product not found in this shop")

    update_query = (
        products.update()
        .where(products.c.id == product_id)
        .values(name=payload.name, price=payload.price)
    )
    await database_write.execute(update_query)
    logger.info("DB WRITE to MASTER: updated product '%s' in shop '%s'", product_id, shop_id)
    return {"id": product_id, "shop_id": shop_id, "name": payload.name, "price": payload.price}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", reload=True)
