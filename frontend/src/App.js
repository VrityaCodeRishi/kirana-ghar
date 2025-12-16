import React, { useState, useEffect, useMemo } from "react";
import { BrowserRouter as Router, Routes, Route, Navigate, Link } from "react-router-dom";
import "./App.css";
import Login from "./Login";
import ShopOwnerDashboard from "./ShopOwnerDashboard";
import CustomerDashboard from "./CustomerDashboard";
import ShopProducts from "./ShopProducts";
import Landing from "./Landing";
import Register from "./Register";
import SupportChat from "./SupportChat";

function App() {
  const [token, setToken] = useState(() => localStorage.getItem("token"));
  const [user, setUser] = useState(null); // { username, role }
  const [loadingUser, setLoadingUser] = useState(false);

  useEffect(() => {
    document.title = "Kirana Ghar";
  }, []);

  useEffect(() => {
    const fetchMe = async () => {
      if (!token) {
        setUser(null);
        return;
      }
      setLoadingUser(true);
      try {
        const res = await fetch("http://localhost:8000/me", {
          headers: { Authorization: `Bearer ${token}` },
        });
        if (!res.ok) throw new Error("unauthorized");
        const data = await res.json();
        setUser(data); // { username, role }
      } catch (_) {
        // invalid token; clear session
        localStorage.removeItem("token");
        setToken(null);
        setUser(null);
      } finally {
        setLoadingUser(false);
      }
    };
    fetchMe();
  }, [token]);

  const handleLogout = () => {
    setToken(null);
    localStorage.removeItem("token");
    // Clear any stored support thread id(s) (best-effort).
    try {
      const keys = Object.keys(localStorage);
      keys.forEach((k) => {
        if (k.startsWith("support_thread_id:")) localStorage.removeItem(k);
      });
    } catch (_) {}
  };

  const displayName = useMemo(() => {
    if (!user?.full_name) return null;
    return String(user.full_name).trim();
  }, [user]);

  return (
    <Router>
      <div className="app-shell">
        <header className="app-header">
        <div className="brand">
          <div className="brand-mark">KG</div>
          <div>
            <h1>Kirana Ghar</h1>
            <p>Digital HQ for neighbourhood stores</p>
          </div>
        </div>
        <nav className="nav">
          {displayName && (
            <span className="welcome-text">Welcome {displayName}</span>
          )}
          {token && user?.role === "customer" && (
            <Link className="nav-link" to="/support">
              Support
            </Link>
          )}
          {token && (
            <button className="ghost-button" onClick={handleLogout}>
              Logout
            </button>
          )}
        </nav>
      </header>
      <main className="view-container">
          <Routes>
            <Route
              path="/"
              element={
                loadingUser ? (
                  <div className="panel"><p className="muted">Loading...</p></div>
                ) : token && user?.role === "shopowner" ? (
                  <Navigate to="/shopowner-dashboard" />
                ) : token && user?.role === "customer" ? (
                  <Navigate to="/customer-dashboard" />
                ) : (
                  <Landing />
                )
              }
            />
            <Route
              path="/shopowner-login"
              element={
                token ? (
                  <Navigate to="/" />
                ) : (
                  <div className="auth-wrapper">
                    <Login setToken={setToken} roleType="shopowner" />
                  </div>
                )
              }
            />
            <Route
              path="/customer-login"
              element={
                token ? (
                  <Navigate to="/" />
                ) : (
                  <div className="auth-wrapper">
                    <Login setToken={setToken} roleType="customer" />
                  </div>
                )
              }
            />
            <Route
              path="/shopowner-register"
              element={<Register roleType="shopowner" />}
            />
            <Route
              path="/customer-register"
              element={<Register roleType="customer" />}
            />
          <Route
            path="/shopowner-dashboard"
            element={
              loadingUser ? (
                <div className="panel"><p className="muted">Loading...</p></div>
              ) : token && user?.role === "shopowner" ? (
                <ShopOwnerDashboard token={token} logout={handleLogout} />
              ) : (
                <Navigate to="/shopowner-login" />
              )
            }
          />
          <Route
            path="/customer-dashboard"
            element={
              loadingUser ? (
                <div className="panel"><p className="muted">Loading...</p></div>
              ) : token && user?.role === "customer" ? (
                <CustomerDashboard token={token} logout={handleLogout} />
              ) : (
                <Navigate to="/customer-login" />
              )
            }
          />
          <Route
            path="/shop/:id"
            element={
              loadingUser ? (
                <div className="panel"><p className="muted">Loading...</p></div>
              ) : token && user?.role === "customer" ? (
                <ShopProducts token={token} logout={handleLogout} />
              ) : (
                <Navigate to="/customer-login" />
              )
            }
          />
          <Route
            path="/support"
            element={
              loadingUser ? (
                <div className="panel"><p className="muted">Loading...</p></div>
              ) : token && user?.role === "customer" ? (
                <SupportChat token={token} user={user} />
              ) : (
                <Navigate to="/customer-login" />
              )
            }
          />
          <Route path="*" element={<Navigate to="/" />} />
          </Routes>
      </main>
    </div>
    </Router>
  );
}

export default App;
