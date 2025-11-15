import React from "react";
import { useNavigate } from "react-router-dom";

function Landing() {
  const navigate = useNavigate();
  return (
    <div className="auth-wrapper">
      <div className="auth-card" style={{ textAlign: "center" }}>
        <h2>Welcome to Kirana Ghar</h2>
        <div className="button-row">
          <button
            className="primary-button inline-button button-lg"
            onClick={() => navigate("/shopowner-login")}
          >
            Shop Owner Login
          </button>
          <button
            className="outline-button inline-button button-lg"
            onClick={() => navigate("/customer-login")}
          >
            Customer Login
          </button>
        </div>
        <div className="button-row" style={{ marginTop: "0.75rem" }}>
          <button
            className="primary-button inline-button button-lg"
            onClick={() => navigate("/shopowner-register")}
          >
            Register as Owner
          </button>
          <button
            className="outline-button inline-button button-lg"
            onClick={() => navigate("/customer-register")}
          >
            Register as Customer
          </button>
        </div>
      </div>
    </div>
  );
}

export default Landing;
