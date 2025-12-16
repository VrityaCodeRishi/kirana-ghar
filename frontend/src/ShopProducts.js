import React, { useEffect, useMemo, useState } from "react";
import { useParams, useNavigate, useLocation } from "react-router-dom";

const INR = "₹";

function useSessionCart() {
  const key = "cart";
  const read = () => {
    try {
      const raw = sessionStorage.getItem(key);
      return raw ? JSON.parse(raw) : [];
    } catch (_) {
      return [];
    }
  };
  const [items, setItems] = useState(read);
  useEffect(() => {
    sessionStorage.setItem(key, JSON.stringify(items));
  }, [items]);

  const add = (product, qty = 1) => {
    setItems((prev) => {
      const idx = prev.findIndex((p) => p.id === product.id);
      if (idx >= 0) {
        const next = [...prev];
        next[idx] = { ...next[idx], quantity: next[idx].quantity + qty };
        return next;
      }
      return [...prev, { ...product, quantity: qty }];
    });
  };
  const setQuantity = (productId, qty) => {
    setItems((prev) =>
      prev
        .map((p) => (p.id === productId ? { ...p, quantity: Math.max(1, qty) } : p))
        .filter((p) => p.quantity > 0)
    );
  };
  const remove = (productId) => setItems((prev) => prev.filter((p) => p.id !== productId));
  const clear = () => setItems([]);

  return { items, add, setQuantity, remove, clear };
}

export default function ShopProducts({ token, logout }) {
  const { id: shopId } = useParams();
  const navigate = useNavigate();
  const location = useLocation();
  const [shop, setShop] = useState(() => location.state?.shop || null);
  const [products, setProducts] = useState([]);
  const [loading, setLoading] = useState(true);
  const { items, add, setQuantity, remove, clear } = useSessionCart();

  useEffect(() => {
    const headers = { Authorization: `Bearer ${token}` };
    async function fetchAll() {
      try {
        // Fetch shop details if not passed via state
        if (!shop) {
          const s = await fetch(`http://localhost:8000/shops/${shopId}`, { headers });
          if (s.ok) setShop(await s.json());
        }
        const p = await fetch(`http://localhost:8000/shops/${shopId}/products/`, { headers });
        const pdata = await p.json();
        setProducts(Array.isArray(pdata) ? pdata : []);
      } finally {
        setLoading(false);
      }
    }
    fetchAll();
  }, [shopId, token]);

  const title = useMemo(() => {
    if (!shop) return "Shop";
    return `${shop.name}${shop.city ? ` — ${shop.city}` : ""}`;
  }, [shop]);

  async function placeOrder(targetShopId, orderItems) {
    const headers = { Authorization: `Bearer ${token}`, "Content-Type": "application/json" };
    const res = await fetch("http://localhost:8000/orders", {
      method: "POST",
      headers,
      body: JSON.stringify({
        shop_id: targetShopId,
        items: orderItems.map((it) => ({ product_id: it.id, quantity: it.quantity || 1 })),
        payment_method: "cod",
      }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data?.detail || "Order failed");
    return data;
  }

  const buyNow = async (product, qty = 1) => {
    try {
      const order = await placeOrder(shopId, [{ ...product, quantity: qty }]);
      alert(`Order placed! Order ID: ${order.id}`);
    } catch (e) {
      alert(e.message || "Order failed");
    }
  };

  const buyCart = async () => {
    if (items.length === 0) return alert("Cart is empty");
    try {
      // Global cart can contain items from multiple shops. Create one order per shop.
      const byShop = items.reduce((acc, it) => {
        const sid = it.shop_id || shopId;
        acc[sid] = acc[sid] || [];
        acc[sid].push(it);
        return acc;
      }, {});

      const orderResults = [];
      for (const [sid, shopItems] of Object.entries(byShop)) {
        const order = await placeOrder(sid, shopItems);
        orderResults.push(order);
      }

      clear();
      const summary = orderResults
        .map((o) => `- ${o.id} (₹${Number(o.total_amount).toFixed(2)})`)
        .join("\n");
      alert(`Order(s) placed!\n${summary}`);
    } catch (e) {
      alert(e.message || "Order failed");
    }
  };

  return (
    <div className="dashboard">
      <section className="panel">
        <button className="outline-button" onClick={() => navigate(-1)}>
          ← Back
        </button>
        <h2 className="section-title">{title}</h2>
        {loading ? (
          <p className="muted">Loading products…</p>
        ) : products.length === 0 ? (
          <p className="muted">No products in this shop yet.</p>
        ) : (
          <ul className="products-list list">
            {products.map((p) => (
              <li key={p.id}>
                <span>
                  {p.name}
                  <span className="price-chip">
                    {INR}
                    {Number(p.price).toFixed(2)}
                  </span>
                </span>
                <div className="action-row">
                  <button className="primary-button inline-button buy-now" onClick={() => buyNow(p, 1)}>
                    Buy Now
                  </button>
                  <button className="outline-button" onClick={() => add({ ...p, shop_id: shopId, shop_name: shop?.name }, 1)}>
                    Add to Cart
                  </button>
                </div>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section className="panel">
        <h3>Your Cart</h3>
        {items.length === 0 ? (
          <p className="muted">No items yet. Add some products.</p>
        ) : (
          <ul className="cart-list list">
            {items.map((it) => (
              <li key={it.id}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12 }}>
                  <span>
                    {it.name}
                    {it.shop_name && (
                      <span className="muted" style={{ marginLeft: 8, fontSize: 12 }}>
                        ({it.shop_name})
                      </span>
                    )}
                    <span className="price-chip">
                      {INR}
                      {(it.price * it.quantity).toFixed(2)}
                    </span>
                  </span>
                  <div className="action-row">
                    <button className="outline-button" onClick={() => setQuantity(it.id, Math.max(1, it.quantity - 1))}>
                      -
                    </button>
                    <span style={{ minWidth: 28, textAlign: "center" }}>{it.quantity}</span>
                    <button className="outline-button" onClick={() => setQuantity(it.id, it.quantity + 1)}>
                      +
                    </button>
                    <button className="text-button danger" onClick={() => remove(it.id)}>
                      Remove
                    </button>
                  </div>
                </div>
              </li>
            ))}
          </ul>
        )}
        <div className="action-row" style={{ marginTop: 12 }}>
          <button className="primary-button inline-button" onClick={buyCart}>
            Buy Cart
          </button>
          <button className="outline-button" onClick={clear}>
            Clear Cart
          </button>
        </div>
      </section>
    </div>
  );
}
