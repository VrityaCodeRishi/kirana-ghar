import React, { useState, useEffect } from "react";
import { Link, useNavigate } from "react-router-dom";

const containerStyle = { maxWidth: "900px", margin: "40px auto", padding: "16px" };
const cardStyle = { border: "1px solid #ccc", borderRadius: "8px", padding: "16px", margin: "12px 0", background: "#f9f9f9" };

function CustomerDashboard({ token, logout }) {
  const [shops, setShops] = useState([]);
  const [query, setQuery] = useState("");
  const [searchResults, setSearchResults] = useState([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [searchError, setSearchError] = useState(null);
  const navigate = useNavigate();

  useEffect(() => {
    fetch("http://localhost:8000/shops/", {
      headers: { Authorization: `Bearer ${token}` }
    }).then(res => res.json())
      .then(setShops);
  }, [token]);

  async function runSearch(e) {
    if (e) e.preventDefault();
    const trimmed = query.trim();
    if (!trimmed) {
      setSearchResults([]);
      setSearchError(null);
      return;
    }
    setSearchLoading(true);
    setSearchError(null);
    try {
      const res = await fetch(`http://localhost:8000/search?q=${encodeURIComponent(trimmed)}&type=all&size=20`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      if (!res.ok) throw new Error("Search failed");
      const data = await res.json();
      setSearchResults(Array.isArray(data.results) ? data.results : []);
    } catch (err) {
      setSearchError(err.message || "Search failed");
      setSearchResults([]);
    } finally {
      setSearchLoading(false);
    }
  }

  function goToShop(shopId, shop) {
    if (shop) {
      navigate(`/shop/${shopId}`, { state: { shop } });
    } else {
      navigate(`/shop/${shopId}`);
    }
  }

  return (
    <div style={containerStyle}>
      <h2>Customer Dashboard</h2>
      <button onClick={logout}>Logout</button>
      
      <hr />
      <form onSubmit={runSearch} style={{ marginBottom: "16px", display: "flex", gap: 8, flexWrap: "wrap" }}>
        <input
          placeholder="Search shops or products"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          style={{ flex: 1, minWidth: 220, padding: "8px 10px", borderRadius: 8, border: "1px solid #ccc" }}
        />
        <button type="submit" className="primary-button inline-button" style={{ minWidth: 120 }}>
          {searchLoading ? "Searching..." : "Search"}
        </button>
      </form>
      {searchError && <p className="muted">{searchError}</p>}
      {searchResults.length > 0 && (
        <div style={{ marginBottom: "24px" }}>
          <h3>Search Results</h3>
          {searchResults.map((r) => {
            const isShop = (r._index || "").includes("shops");
            const key = `${r._index}-${r._id}`;
            if (isShop) {
              const shop = { id: r.id || r.ID, name: r.name, city: r.city };
              return (
                <div key={key} style={cardStyle}>
                  <strong>Shop:</strong>{" "}
                  <button
                    style={{ border: "none", background: "none", padding: 0, cursor: "pointer", fontWeight: 600 }}
                    onClick={() => goToShop(shop.id, shop)}
                  >
                    {shop.name}
                    {shop.city ? ` — ${shop.city}` : ""}
                  </button>
                </div>
              );
            }
            const productShopId = r.shop_id || r.SHOP_ID;
            return (
              <div key={key} style={cardStyle}>
                <div>
                  <strong>Product:</strong> {r.name}
                </div>
                {typeof r.price !== "undefined" && (
                  <div style={{ marginTop: 4 }}>Price: ₹{Number(r.price).toFixed(2)}</div>
                )}
                {productShopId && (
                  <button
                    className="outline-button inline-button"
                    style={{ marginTop: 8 }}
                    onClick={() => goToShop(productShopId)}
                  >
                    View shop
                  </button>
                )}
              </div>
            );
          })}
        </div>
      )}

      <h3>All Shops</h3>
      {shops.map(shop => (
        <div key={shop.id} style={cardStyle}>
          <Link to={`/shop/${shop.id}`} state={{ shop }} style={{ textDecoration: "none", fontWeight: "600" }}>
            {shop.name}{shop.city ? ` — ${shop.city}` : ""}
          </Link>
        </div>
      ))}
      {/* Product listing moved to dedicated page /shop/:id */}
    </div>
  );
}

export default CustomerDashboard;
