import asyncio
import os
import logging
import sys
import json
import contextlib
from fastapi import FastAPI, HTTPException, Depends, status, Form
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from sqlalchemy import Table, Column, String, Float, MetaData, ForeignKey, text
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
from fastapi.middleware.cors import CORSMiddleware

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
