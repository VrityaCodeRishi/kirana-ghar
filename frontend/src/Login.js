import React, { useState } from "react";
import { useNavigate } from "react-router-dom";

function Login({ setToken, roleType = "customer" }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();

  const loginUser = async () => {
    if (!username || !password) {
      alert("Enter both username and password");
      return;
    }
    setLoading(true);
    try {
      const response = await fetch("http://localhost:8000/token", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({ username, password }),
      });
      const data = await response.json();
      if (response.ok && data.access_token) {
        localStorage.setItem("token", data.access_token);
        setToken(data.access_token);
        // Ask backend who we are to decide destination securely
        try {
          const meRes = await fetch("http://localhost:8000/me", {
            headers: { Authorization: `Bearer ${data.access_token}` },
          });
          const me = await meRes.json();
          if (me.role === "shopowner") navigate("/shopowner-dashboard");
          else if (me.role === "customer") navigate("/customer-dashboard");
          else navigate("/");
        } catch (_) {
          navigate("/");
        }
      } else {
        alert(data.detail || "Login failed");
      }
    } catch (error) {
      console.error("Login error", error);
      alert("Unable to reach server");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="auth-card">
      <h2>{roleType === "shopowner" ? "Shop Owner Login" : "Customer Login"}</h2>
      <p className="muted">Sign in to continue to Kirana Ghar</p>

      <div className="field-group">
        <label htmlFor="username">Username</label>
        <input
          id="username"
          placeholder="Enter username"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
        />
      </div>

      <div className="field-group">
        <label htmlFor="password">Password</label>
        <input
          id="password"
          placeholder="Enter password"
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
        />
      </div>

      <button
        className="primary-button"
        onClick={loginUser}
        disabled={loading}
      >
        {loading ? "Logging in..." : "Login"}
      </button>
    </div>
  );
}

export default Login;
