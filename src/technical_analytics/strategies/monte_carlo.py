import pandas as pd
import numpy as np
from datetime import timedelta
from sqlmodel import SQLModel


def estimate_heston_params(df: pd.DataFrame):
    df = df.sort_values("timestamp")
    df["log_return"] = np.log(df["close"] / df["close"].shift(1))
    df = df.dropna()
    # Initial variance
    v0 = df["log_return"].var()
    # Long-term variance
    theta = df["log_return"].var()
    # Mean reversion rate (kappa) approx: covariance regression
    delta_v = df["log_return"].diff().dropna() ** 2
    kappa = 2.0  # rough estimate
    # Volatility of volatility
    xi = np.std(delta_v)
    # Correlation between price and volatility
    rho = np.corrcoef(df["log_return"].iloc[1:], delta_v)[0, 1]
    # Drift
    mu = df["log_return"].mean()
    return v0, theta, kappa, xi, rho, mu


def heston_mc(S0, v0, mu, kappa, theta, xi, rho, T=30, steps_per_day=1, n_simulations=2000):
    dt = 1 / 252 / steps_per_day
    n_steps = int(T * steps_per_day)

    # Preallocate arrays
    S = np.zeros((n_steps + 1, n_simulations))
    v = np.zeros((n_steps + 1, n_simulations))
    S[0] = S0
    v[0] = v0

    # Correlated Brownian motions
    for t in range(1, n_steps + 1):
        Z1 = np.random.standard_normal(n_simulations)
        Z2 = np.random.standard_normal(n_simulations)
        W_S = Z1
        W_v = rho * Z1 + np.sqrt(1 - rho**2) * Z2

        # Update variance
        v[t] = np.abs(
            v[t - 1] + kappa * (theta - v[t - 1]) * dt + xi * np.sqrt(v[t - 1] * dt) * W_v
        )
        # Update price
        S[t] = S[t - 1] * np.exp((mu - 0.5 * v[t - 1]) * dt + np.sqrt(v[t - 1] * dt) * W_S)

    return S


def aggregate_percentiles(S: np.ndarray, ticker, start_date):
    n_steps = S.shape[0]
    timestamps = [start_date + timedelta(days=i) for i in range(n_steps)]
    df_summary = pd.DataFrame(
        {
            "ticker": ticker,
            "timestamp": timestamps,
            "mean_price": S.mean(axis=1),
            "p5": np.percentile(S, 5, axis=1),
            "p10": np.percentile(S, 10, axis=1),
            "p25": np.percentile(S, 25, axis=1),
            "p75": np.percentile(S, 75, axis=1),
            "p90": np.percentile(S, 90, axis=1),
            "p95": np.percentile(S, 95, axis=1),
        }
    )
    return df_summary


def run_heston_pipeline(data: list[SQLModel], ticker: str, days=90, T=30, n_simulations=2000):
    records = [obj.model_dump() for obj in data]
    df = pd.DataFrame(records)

    v0, theta, kappa, xi, rho, mu = estimate_heston_params(df)
    S0 = df["close"].iloc[-1]
    simulated_paths = heston_mc(S0, v0, mu, kappa, theta, xi, rho, T=T, n_simulations=n_simulations)
    start_date = df["timestamp"].iloc[-1] + pd.Timedelta(days=1)
    df_summary = aggregate_percentiles(simulated_paths, ticker, start_date)
    return df_summary
