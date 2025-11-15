import React, { useState } from "react";
import { useNavigate } from "react-router-dom";

function Register({ roleType = "customer" }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [fullName, setFullName] = useState("");
  const [address, setAddress] = useState("");
  const [mobile, setMobile] = useState("");
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();

  function humanizeError(detail) {
    if (!detail) return "Registration failed";
    if (typeof detail === "string") return detail;
    if (Array.isArray(detail)) {
      // Likely Pydantic validation errors
      return detail
        .map((e) => {
          const loc = Array.isArray(e.loc) ? e.loc[e.loc.length - 1] : e.loc;
          return `${loc}: ${e.msg}`;
        })
        .join("\n");
    }
    if (typeof detail === "object") {
      try {
        return JSON.stringify(detail);
      } catch (_) {
        return "Registration failed";
      }
    }
    return String(detail);
  }

  const handleRegister = async () => {
    if (!username || !password || !fullName || !address || !mobile) {
      alert("Please fill all fields");
      return;
    }
    if (!/^\d{10}$/.test(mobile)) {
      alert("Enter a valid 10-digit mobile number");
      return;
    }
    setLoading(true);
    try {
      const response = await fetch("http://localhost:8000/register", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          username,
          password,
          role: roleType,
          full_name: fullName,
          address,
          mobile,
        }),
      });
      const data = await response.json().catch(() => ({}));
      if (response.ok) {
        // Redirect to correct login after successful registration
        navigate(roleType === "shopowner" ? "/shopowner-login" : "/customer-login");
      } else {
        alert(humanizeError(data.detail));
      }
    } catch (err) {
      console.error("Registration error", err);
      alert("Unable to reach server");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="auth-wrapper">
      <div className="auth-card">
        <h2>{roleType === "shopowner" ? "Register Shop Owner" : "Register Customer"}</h2>
        <p className="muted">Create your account on Kirana Ghar</p>

        <div className="field-group">
          <label htmlFor="full-name">Full name</label>
          <input
            id="full-name"
            placeholder="Your full name"
            value={fullName}
            onChange={(e) => setFullName(e.target.value)}
          />
        </div>

        <div className="field-group">
          <label htmlFor="address">Address</label>
          <input
            id="address"
            placeholder="Street, City, PIN"
            value={address}
            onChange={(e) => setAddress(e.target.value)}
          />
        </div>

        <div className="field-group">
          <label htmlFor="mobile">Mobile number</label>
          <input
            id="mobile"
            placeholder="10-digit mobile"
            value={mobile}
            onChange={(e) => setMobile(e.target.value)}
          />
        </div>

        <div className="field-group">
          <label htmlFor="reg-username">Username</label>
          <input
            id="reg-username"
            placeholder="Choose a username"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
          />
        </div>

        <div className="field-group">
          <label htmlFor="reg-password">Password</label>
          <input
            id="reg-password"
            placeholder="Choose a password"
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
        </div>

        <button
          className="primary-button"
          onClick={handleRegister}
          disabled={loading}
        >
          {loading ? "Registering..." : "Create Account"}
        </button>
      </div>
    </div>
  );
}

export default Register;
