import pandas as pd
import numpy as np
from datetime import timedelta
from sqlmodel import SQLModel
from typing import Tuple
import warnings


def calculate_realized_volatility(df: pd.DataFrame) -> pd.Series:
    """
    Calculate realized volatility using OHLC data (Yang-Zhang estimator)
    More accurate than just using close-to-close returns
    """
    # Yang-Zhang volatility estimator
    ln_ho = np.log(df["high"] / df["open"])
    ln_lo = np.log(df["low"] / df["open"])
    ln_co = np.log(df["close"] / df["open"])
    ln_oc = np.log(df["open"] / df["close"].shift(1))

    # Rogers-Satchell component
    rs = ln_ho * (ln_ho - ln_co) + ln_lo * (ln_lo - ln_co)

    # Overnight component
    k = 0.34 / (1.34 + (len(df) + 1) / (len(df) - 1))

    # Yang-Zhang volatility
    yz = ln_oc.var() + k * ln_co.var() + (1 - k) * rs.mean()

    return np.sqrt(yz * 252)  # Annualized


def enhanced_heston_params(
    df: pd.DataFrame, window: int = 60
) -> Tuple[float, float, float, float, float, float]:
    """
    Enhanced Heston parameter estimation using multiple volatility measures
    """
    df = df.sort_values("timestamp")

    # Calculate different return measures
    df["log_return"] = np.log(df["close"] / df["close"].shift(1))
    df["overnight_return"] = np.log(df["open"] / df["close"].shift(1))
    df["intraday_return"] = np.log(df["close"] / df["open"])

    # Calculate realized volatility using OHLC
    df["realized_vol"] = calculate_realized_volatility(df)

    # Rolling volatility estimates
    df["rolling_vol"] = df["log_return"].rolling(window=window).std() * np.sqrt(252)

    df = df.dropna()

    if len(df) < window:
        raise ValueError(f"Insufficient data: need at least {window} observations")

    # Use realized volatility for variance estimation
    # recent_vol = df["realized_vol"].iloc[-window:].mean()

    # Initial variance (use more recent data)
    v0 = (df["rolling_vol"].iloc[-10:].mean() / np.sqrt(252)) ** 2

    # Long-term variance (use full sample)
    theta = (df["rolling_vol"].mean() / np.sqrt(252)) ** 2

    # Improved mean reversion estimation using volatility clustering
    vol_changes = df["rolling_vol"].diff().dropna()
    vol_levels = df["rolling_vol"].shift(1).dropna()

    # Estimate kappa using AR(1) regression on volatility
    if len(vol_changes) > 30:
        X = vol_levels.iloc[1:].values.reshape(-1, 1)
        y = vol_changes.iloc[1:].values

        # Simple regression: vol_change = -kappa * (vol_level - theta) + error
        kappa_est = -np.cov(y, X.flatten())[0, 1] / np.var(X.flatten())
        kappa = max(0.1, min(5.0, abs(kappa_est)))  # Bounded between 0.1 and 5
    else:
        kappa = 2.0

    # Volatility of volatility using realized measures
    vol_returns = df["realized_vol"].pct_change().dropna()
    xi = vol_returns.std() * np.sqrt(252) * np.sqrt(theta)
    xi = max(0.01, min(2.0, xi))  # Reasonable bounds

    # Correlation using intraday patterns
    price_changes = df["log_return"].iloc[1:]
    vol_changes_aligned = df["rolling_vol"].diff().iloc[1:]

    if len(price_changes) > 10:
        rho = np.corrcoef(price_changes, vol_changes_aligned)[0, 1]
        rho = max(-0.9, min(0.1, rho))  # Typically negative, bounded
    else:
        rho = -0.3  # Default negative correlation

    # Drift with bias correction
    # Separate overnight and intraday components
    # overnight_drift = df["overnight_return"].mean() * 252
    # intraday_drift = df["intraday_return"].mean() * 252

    # Total drift with volatility adjustment
    total_return = df["log_return"].mean() * 252
    vol_adjustment = 0.5 * v0  # Bias correction for log returns
    mu = total_return + vol_adjustment

    return v0, theta, kappa, xi, rho, mu


def improved_heston_mc(
    S0: float,
    v0: float,
    mu: float,
    kappa: float,
    theta: float,
    xi: float,
    rho: float,
    T: int = 30,
    steps_per_day: int = 4,
    n_simulations: int = 2000,
    antithetic: bool = True,
) -> np.ndarray:
    """
    Improved Heston Monte Carlo with:
    - Antithetic variates for variance reduction
    - Milstein scheme for better accuracy
    - Feller condition checking
    - Multiple intraday steps for smoother paths
    """

    # Check Feller condition: 2*kappa*theta > xi^2
    feller_condition = 2 * kappa * theta
    if feller_condition <= xi**2:
        warnings.warn(f"Feller condition violated: 2κθ={feller_condition:.4f} <= ξ²={xi**2:.4f}")
        xi = min(xi, 0.9 * np.sqrt(feller_condition))  # Adjust xi to satisfy condition

    dt = 1 / 252 / steps_per_day
    n_steps = int(T * steps_per_day)

    # Use antithetic variates for variance reduction
    if antithetic:
        n_sims = n_simulations // 2
        generate_antithetic = True
    else:
        n_sims = n_simulations
        generate_antithetic = False

    # Preallocate arrays
    S = np.zeros((n_steps + 1, n_simulations))
    v = np.zeros((n_steps + 1, n_simulations))
    S[0] = S0
    v[0] = v0

    # Precompute constants
    sqrt_dt = np.sqrt(dt)
    sqrt_1_rho2 = np.sqrt(1 - rho**2)

    for t in range(1, n_steps + 1):
        # Generate random numbers
        Z1 = np.random.standard_normal(n_sims)
        Z2 = np.random.standard_normal(n_sims)

        if generate_antithetic:
            Z1 = np.concatenate([Z1, -Z1])
            Z2 = np.concatenate([Z2, -Z2])

        # Correlated Brownian motions
        W_S = Z1
        W_v = rho * Z1 + sqrt_1_rho2 * Z2

        # Current variance (ensure non-negative)
        v_curr = np.maximum(v[t - 1], 0.0001)  # Floor at small positive value
        sqrt_v_curr = np.sqrt(v_curr)

        # Update variance using Milstein scheme for better accuracy
        v_drift = kappa * (theta - v_curr) * dt
        v_diffusion = xi * sqrt_v_curr * sqrt_dt * W_v
        v_milstein = 0.25 * xi**2 * dt * (W_v**2 - 1)  # Milstein correction term

        v[t] = v_curr + v_drift + v_diffusion + v_milstein
        v[t] = np.maximum(v[t], 0.0001)  # Ensure positivity

        # Update price using exact scheme where possible
        S_drift = (mu - 0.5 * v_curr) * dt
        S_diffusion = sqrt_v_curr * sqrt_dt * W_S

        S[t] = S[t - 1] * np.exp(S_drift + S_diffusion)

    return S


def calculate_greeks(S: np.ndarray, S0: float) -> dict:
    """
    Calculate option-like Greeks for the Monte Carlo simulation
    """
    final_prices = S[-1, :]

    # Delta: sensitivity to underlying price
    # price_change = 0.01 * S0  # 1% change
    upward_returns = (final_prices - S0) / S0
    delta = np.mean(upward_returns) / 0.01

    # Gamma: convexity measure
    returns_squared = upward_returns**2
    gamma = np.mean(returns_squared) / (0.01**2)

    # Theta: time decay (approximate)
    # Compare expected values at different time points
    mid_point = S.shape[0] // 2
    theta_approx = (np.mean(S[mid_point, :]) - np.mean(S[-1, :])) / (S.shape[0] - mid_point)

    return {"delta": delta, "gamma": gamma, "theta": theta_approx}


def enhanced_risk_metrics(
    S: np.ndarray, S0: float, confidence_levels: list = [0.01, 0.05, 0.1]
) -> dict:
    """
    Calculate comprehensive risk metrics
    """
    final_prices = S[-1, :]
    returns = (final_prices - S0) / S0

    metrics = {}

    # Value at Risk for different confidence levels
    for conf in confidence_levels:
        var = np.percentile(returns, conf * 100)
        cvar = np.mean(returns[returns <= var])  # Conditional VaR (Expected Shortfall)

        metrics[f"VaR_{int(conf * 100)}%"] = var
        metrics[f"CVaR_{int(conf * 100)}%"] = cvar

    # Maximum Drawdown along paths
    cumulative_returns = S / S0 - 1
    running_max = np.maximum.accumulate(cumulative_returns, axis=0)
    drawdowns = cumulative_returns - running_max
    max_drawdown = np.min(drawdowns, axis=0)

    metrics["max_drawdown_mean"] = np.mean(max_drawdown)
    metrics["max_drawdown_95th"] = np.percentile(max_drawdown, 95)

    # Probability of different return thresholds
    thresholds = [-0.2, -0.1, -0.05, 0, 0.05, 0.1, 0.2]
    for threshold in thresholds:
        prob = np.mean(returns > threshold) * 100
        metrics[f"prob_return_gt_{int(threshold * 100)}%"] = prob

    # Sharpe ratio (assuming risk-free rate = 0)
    metrics["sharpe_ratio"] = np.mean(returns) / (np.std(returns) + 1e-8)

    # Sortino ratio (downside deviation)
    downside_returns = returns[returns < 0]
    if len(downside_returns) > 0:
        sortino = np.mean(returns) / (np.std(downside_returns) + 1e-8)
    else:
        sortino = float("inf")
    metrics["sortino_ratio"] = sortino

    return metrics


def aggregate_enhanced_percentiles(
    S: np.ndarray, ticker: str, start_date, S0: float
) -> pd.DataFrame:
    """
    Enhanced aggregation with additional metrics
    """
    n_steps = S.shape[0]
    timestamps = [start_date + timedelta(days=i) for i in range(n_steps)]

    # Calculate additional percentiles and metrics
    percentiles = [5, 10, 25, 75, 90, 95]
    perc_data = {}

    for p in percentiles:
        perc_data[f"p{p}"] = np.percentile(S, p, axis=1)

    # Calculate daily statistics
    daily_returns = S[1:] / S[:-1] - 1
    daily_vol = np.std(daily_returns, axis=1) * np.sqrt(252)

    df_summary = pd.DataFrame(
        {"ticker": ticker, "timestamp": timestamps, "mean_price": S.mean(axis=1), **perc_data}
    )

    # Add volatility data (excluding first day)
    vol_df = pd.DataFrame(
        {"ticker": ticker, "timestamp": timestamps[1:], "daily_volatility": daily_vol}
    )

    # Calculate risk metrics for final day
    risk_metrics = enhanced_risk_metrics(S, S0)
    greeks = calculate_greeks(S, S0)

    return df_summary, vol_df, risk_metrics, greeks


def run_enhanced_heston_pipeline(
    data: list[SQLModel],
    ticker: str,
    days: int = 90,
    T: int = 30,
    n_simulations: int = 2000,
    steps_per_day: int = 4,
) -> tuple:
    """
    Enhanced pipeline with comprehensive analysis
    """
    records = [obj.model_dump() for obj in data]
    df = pd.DataFrame(records)

    # Parameter estimation with error handling
    try:
        v0, theta, kappa, xi, rho, mu = enhanced_heston_params(df, window=min(60, len(df) // 2))
    except Exception as e:
        print(f"Parameter estimation failed: {e}")
        # Fallback to simpler estimation
        df["log_return"] = np.log(df["close"] / df["close"].shift(1))
        v0 = df["log_return"].var()
        theta = v0
        kappa = 2.0
        xi = 0.3
        rho = -0.3
        mu = df["log_return"].mean() * 252

    S0 = df["close"].iloc[-1]

    # print(f"Heston Parameters for {ticker}:")
    # print(f"  Initial variance (v0): {v0:.6f}")
    # print(f"  Long-term variance (θ): {theta:.6f}")
    # print(f"  Mean reversion rate (κ): {kappa:.4f}")
    # print(f"  Vol of vol (ξ): {xi:.4f}")
    # print(f"  Correlation (ρ): {rho:.4f}")
    # print(f"  Drift (μ): {mu:.4f}")

    # Run improved simulation
    simulated_paths = improved_heston_mc(
        S0,
        v0,
        mu,
        kappa,
        theta,
        xi,
        rho,
        T=T,
        steps_per_day=steps_per_day,
        n_simulations=n_simulations,
    )

    start_date = df["timestamp"].iloc[-1] + pd.Timedelta(days=1)

    # Get comprehensive results
    df_summary, vol_df, risk_metrics, greeks = aggregate_enhanced_percentiles(
        simulated_paths, ticker, start_date, S0
    )

    return (
        df_summary,
        vol_df,
        risk_metrics,
        greeks,
        {"v0": v0, "theta": theta, "kappa": kappa, "xi": xi, "rho": rho, "mu": mu, "S0": S0},
    )


# Example usage with additional analysis
def analyze_simulation_quality(simulated_paths: np.ndarray, df_historical: pd.DataFrame) -> dict:
    """
    Analyze the quality of the simulation by comparing with historical statistics
    """
    # Historical statistics
    hist_returns = np.log(df_historical["close"] / df_historical["close"].shift(1)).dropna()
    hist_vol = hist_returns.std() * np.sqrt(252)
    hist_skew = hist_returns.skew()
    hist_kurt = hist_returns.kurtosis()

    # Simulated statistics (daily returns)
    sim_returns = np.log(simulated_paths[1:] / simulated_paths[:-1])
    sim_vol = np.std(sim_returns.flatten()) * np.sqrt(252)
    sim_skew = pd.Series(sim_returns.flatten()).skew()
    sim_kurt = pd.Series(sim_returns.flatten()).kurtosis()

    return {
        "historical_vol": hist_vol,
        "simulated_vol": sim_vol,
        "vol_difference": abs(hist_vol - sim_vol) / hist_vol,
        "historical_skew": hist_skew,
        "simulated_skew": sim_skew,
        "historical_kurtosis": hist_kurt,
        "simulated_kurtosis": sim_kurt,
    }
