import React, { useEffect, useState } from "react";

const INR_SYMBOL = "₹";

function ShopOwnerDashboard({ token, logout }) {
  const [shops, setShops] = useState([]);
  const [shopName, setShopName] = useState("");
  const [shopCity, setShopCity] = useState("");
  const [selectedShop, setSelectedShop] = useState(null);
  const [products, setProducts] = useState([]);
  const [productName, setProductName] = useState("");
  const [productPrice, setProductPrice] = useState("");
  const [editingProduct, setEditingProduct] = useState(null);
  const [editName, setEditName] = useState("");
  const [editPrice, setEditPrice] = useState("");

  useEffect(() => {
    fetch("http://localhost:8000/shops/", {
      headers: { Authorization: `Bearer ${token}` },
    })
      .then((res) => res.json())
      .then(setShops);
  }, [token]);

  function createShop() {
    if (!shopName.trim() || !shopCity.trim()) return;
    const formData = new URLSearchParams();
    formData.append("name", shopName.trim());
    formData.append("city", shopCity.trim());
    fetch("http://localhost:8000/shops/", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: formData,
    })
      .then((res) => res.json())
      .then((newShop) => {
        setShops((prev) => [...prev, newShop]);
        setShopName("");
        setShopCity("");
        setSelectedShop(newShop);
      });
  }

  useEffect(() => {
    if (!selectedShop) {
      setProducts([]);
      return;
    }
    fetch(`http://localhost:8000/shops/${selectedShop.id}/products/`, {
      headers: { Authorization: `Bearer ${token}` },
    })
      .then((res) => res.json())
      .then(setProducts);
  }, [selectedShop, token]);

  function addProduct() {
    if (!selectedShop || !productName.trim() || !productPrice) return;

    fetch(`http://localhost:8000/shops/${selectedShop.id}/products/`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        name: productName.trim(),
        price: parseFloat(productPrice),
      }),
    })
      .then((res) => res.json())
      .then((newProduct) => {
        setProducts((prev) => [...prev, newProduct]);
        setProductName("");
        setProductPrice("");
      });
  }

  function deleteShop(shop) {
    if (!window.confirm("Delete this shop and all its products?")) return;
    fetch(`http://localhost:8000/shops/${shop.id}/`, {
      method: "DELETE",
      headers: { Authorization: `Bearer ${token}` },
    }).then((response) => {
      if (response.ok) {
        setShops((prev) => prev.filter((s) => s.id !== shop.id));
        if (selectedShop?.id === shop.id) {
          setSelectedShop(null);
          setProducts([]);
        }
      }
    });
  }

  function startEdit(product) {
    setEditingProduct(product);
    setEditName(product.name);
    setEditPrice(product.price.toString());
  }

  function cancelEdit() {
    setEditingProduct(null);
    setEditName("");
    setEditPrice("");
  }

  function saveProductEdits() {
    if (!editingProduct || !editName.trim() || !editPrice) return;
    fetch(
      `http://localhost:8000/shops/${selectedShop.id}/products/${editingProduct.id}/`,
      {
        method: "PUT",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          name: editName.trim(),
          price: parseFloat(editPrice),
        }),
      }
    )
      .then((res) => res.json())
      .then((updated) => {
        setProducts((prev) => prev.map((p) => (p.id === updated.id ? updated : p)));
        cancelEdit();
      });
  }

  function deleteProduct(product) {
    if (!window.confirm("Remove this product?")) return;
    fetch(
      `http://localhost:8000/shops/${selectedShop.id}/products/${product.id}/`,
      {
        method: "DELETE",
        headers: { Authorization: `Bearer ${token}` },
      }
    ).then((response) => {
      if (response.ok) {
        setProducts((prev) => prev.filter((p) => p.id !== product.id));
        if (editingProduct?.id === product.id) {
          cancelEdit();
        }
      }
    });
  }

  return (
    <div className="dashboard">
      <section className="panel">
        <div className="section-header">
          <h2 className="section-title">Shop Owner Workspace</h2>
          <p className="muted">
            Create shops, add products, and keep inventory updated.
          </p>
        </div>
        <div className="form-inline">
          <input
            placeholder="Give your shop a name"
            value={shopName}
            onChange={(e) => setShopName(e.target.value)}
          />
          <input
            placeholder="City"
            value={shopCity}
            onChange={(e) => setShopCity(e.target.value)}
          />
          <button
            className="primary-button inline-button"
            onClick={createShop}
            disabled={!shopName.trim() || !shopCity.trim()}
          >
            Create Shop
          </button>
          <button className="outline-button" onClick={logout}>
            Logout
          </button>
        </div>
      </section>

      <div className="dashboard-grid">
        <section className="panel">
          <h3>Your Shops</h3>
          {shops.length === 0 ? (
            <p className="muted">No shops yet. Start by creating one above.</p>
          ) : (
            <ul className="list">
              {shops.map((shop) => (
                <li key={shop.id}>
                  <div className="list-row">
                    <button
                      className={
                        selectedShop?.id === shop.id
                          ? "active shop-pill"
                          : "shop-pill"
                      }
                      onClick={() => setSelectedShop(shop)}
                    >
                    {shop.name}
                    {shop.city ? ` — ${shop.city}` : ""}
                  </button>
                    <button
                      className="text-button danger"
                      onClick={() => deleteShop(shop)}
                    >
                      Delete
                    </button>
                  </div>
                </li>
              ))}
            </ul>
          )}
        </section>

        <section className="panel">
          {selectedShop ? (
            <>
              <h3>
                Products in {selectedShop.name}
                {selectedShop.city ? ` at ${selectedShop.city}` : ""}
              </h3>
              {products.length === 0 ? (
                <p className="muted">No products yet. Add your first item.</p>
              ) : (
                <ul className="products-list list">
                  {products.map((product) => (
                    <li key={product.id}>
                      <div className="list-row" style={{ justifyContent: "space-between" }}>
                        <span>
                          {product.name}
                          <span className="price-chip">
                            {INR_SYMBOL}
                            {Number(product.price).toFixed(2)}
                          </span>
                        </span>
                      </div>
                      <div className="action-row">
                        <button
                          className="text-button"
                          onClick={() => startEdit(product)}
                        >
                          Edit
                        </button>
                        <button
                          className="text-button danger"
                          onClick={() => deleteProduct(product)}
                        >
                          Delete
                        </button>
                      </div>
                    </li>
                  ))}
                </ul>
              )}

              <div className="form-inline" style={{ marginTop: "1rem" }}>
                <input
                  placeholder="Product name"
                  value={productName}
                  onChange={(e) => setProductName(e.target.value)}
                />
                <input
                  placeholder="Price in INR"
                  type="number"
                  min="0"
                  value={productPrice}
                  onChange={(e) => setProductPrice(e.target.value)}
                />
                <button
                  className="primary-button inline-button"
                  onClick={addProduct}
                  disabled={!productName.trim() || !productPrice}
                >
                  Add Product
                </button>
              </div>

              {editingProduct && (
                <div className="edit-card">
                  <h4>Editing {editingProduct.name}</h4>
                  <div className="form-inline">
                    <input
                      placeholder="Product name"
                      value={editName}
                      onChange={(e) => setEditName(e.target.value)}
                    />
                    <input
                      placeholder="Price in INR"
                      type="number"
                      min="0"
                      value={editPrice}
                      onChange={(e) => setEditPrice(e.target.value)}
                    />
                    <button
                      className="primary-button inline-button"
                      onClick={saveProductEdits}
                      disabled={!editName.trim() || !editPrice}
                    >
                      Save changes
                    </button>
                    <button className="text-button" onClick={cancelEdit}>
                      Cancel
                    </button>
                  </div>
                </div>
              )}
            </>
          ) : (
            <div>
              <h3>Select a shop</h3>
              <p className="muted">
                Choose one of your shops to manage products and pricing.
              </p>
            </div>
          )}
        </section>
      </div>
    </div>
  );
}

export default ShopOwnerDashboard;
