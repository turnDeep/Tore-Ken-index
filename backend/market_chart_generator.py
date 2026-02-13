import pandas as pd
import numpy as np
import mplfinance as mpf
import matplotlib.pyplot as plt
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_market_chart(df, output_path):
    """
    Generates the Market Analysis chart (SPY) with trend background colors and divergence lines.
    """
    if df.empty:
        logger.error("No market data to plot")
        return False

    # Ensure index is DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        if 'Date' in df.columns:
            df.index = pd.to_datetime(df['Date'])
        else:
            df.index = pd.to_datetime(df.index)

    # Prepare AddPlots
    apds = []

    # --- Panel 1: TSV (Approx) ---
    # Determine which column holds TSV (df['TSV'])
    if 'TSV' in df.columns:
        # TSV line color: Teal (similar to image)
        apds.append(mpf.make_addplot(df['TSV'], panel=1, color='teal', width=1.5, ylabel='TSV'))

        # Add a zero line for TSV
        apds.append(mpf.make_addplot(np.zeros(len(df)), panel=1, color='gray', linestyle='--', width=0.8, secondary_y=False))

    # --- Panel 2: StochRSI ---
    # Fast_K: Cyan, Slow_D: Orange
    # Add horizontal lines for Overbought (80) and Oversold (20)

    # 80 line (Red dotted)
    apds.append(mpf.make_addplot(np.full(len(df), 80), panel=2, color='red', linestyle=':', width=1.0, secondary_y=False))
    # 20 line (Green dotted)
    apds.append(mpf.make_addplot(np.full(len(df), 20), panel=2, color='green', linestyle=':', width=1.0, secondary_y=False))

    # Legacy logic uses 'Fast_K' and 'Slow_D'
    if 'Fast_K' in df.columns and 'Slow_D' in df.columns:
        apds.append(mpf.make_addplot(df['Fast_K'], panel=2, color='cyan', width=1.2, ylabel='StochRSI'))
        apds.append(mpf.make_addplot(df['Slow_D'], panel=2, color='orange', width=1.2))

    # --- Panel 3: Market Bloodbath ---
    if 'New_Lows_Ratio' in df.columns:
        # Bloodbath line
        apds.append(mpf.make_addplot(df['New_Lows_Ratio'], panel=3, color='blue', width=1.5, ylabel='Bloodbath %'))
        # 4% Threshold Line
        apds.append(mpf.make_addplot(np.full(len(df), 4.0), panel=3, color='orange', linestyle='--', width=1.0, secondary_y=False))

    # --- Climax Buy Arrows (Panel 0) ---
    if 'Climax_Entry' in df.columns:
        # Create a series with NaN where False, and Price Low where True
        climax_signals = df['Climax_Entry'].astype(bool)
        # Position slightly below Low
        climax_markers = np.where(climax_signals, df['Low'] * 0.98, np.nan)

        # Only add plot if there are any signals
        if not np.all(np.isnan(climax_markers)):
             apds.append(mpf.make_addplot(climax_markers, type='scatter', markersize=100, marker='^', color='blue', panel=0))

    # --- Trend Background Logic ---
    # We use fill_between logic with mpf.make_addplot
    # We need dummy series for fill_between

    # Calculate Y-axis limits for background fill
    y_high = df['High'].max() * 1.05
    y_low = df['Low'].min() * 0.95

    # Bullish (Green Signal) -> Cyan/SkyBlue background
    # Bearish (Red Signal) -> Red background

    if 'Bullish_Phase' in df.columns and 'Bearish_Phase' in df.columns:
        pass

    # Prepare Fill Between List
    fill_between_list = []

    if 'Bullish_Phase' in df.columns:
         # Bullish Phase (Green Trend)
         fill_between_list.append(dict(y1=y_high, y2=y_low, where=df['Bullish_Phase'].values, color='skyblue', alpha=0.1))
         # Bearish Phase (Red Trend)
         fill_between_list.append(dict(y1=y_high, y2=y_low, where=df['Bearish_Phase'].values, color='red', alpha=0.1))

    if 'New_Lows_Ratio' in df.columns:
         # Bloodbath Background (Light Purple) when > 4%
         bb_cond = (df['New_Lows_Ratio'] > 4.0).values
         # Higher alpha to make it visible over the trend color
         fill_between_list.append(dict(y1=y_high, y2=y_low, where=bb_cond, color='violet', alpha=0.3))

    # Panel Ratios
    ratios = (6, 2, 2)
    if 'New_Lows_Ratio' in df.columns:
        ratios = (6, 2, 2, 2)

    # Style
    mc = mpf.make_marketcolors(up='green', down='red', inherit=True)
    s = mpf.make_mpf_style(marketcolors=mc, gridstyle=':', y_on_right=True)

    # Plot
    try:
        # Create figure first to allow custom plotting (lines) on axes
        fig, axlist = mpf.plot(
            df,
            type='candle',
            style=s,
            addplot=apds,
            volume=False,
            panel_ratios=ratios, # Adjusted ratios
            title="",
            returnfig=True,
            figsize=(10, 14), # Increased height for extra panel
            tight_layout=True,
            fill_between=fill_between_list if fill_between_list else None
        )

        # --- Draw Divergence Lines ---
        # axlist structure:
        # axlist[0] = Main Price Axis (Candlesticks)
        # axlist[2] = Panel 1 (TSV)
        # axlist[4] = Panel 2 (StochRSI)

        ax_price = axlist[0]
        ax_tsv = axlist[2]

        if 'Bullish_Divergence' in df.columns:
            # Iterate through rows where Bullish_Divergence is not None
            # The value is the index of the previous pivot

            # Reset index to integer for plotting if needed, but mplfinance uses dates on x-axis usually?
            # Actually, mplfinance plots against an integer index internally if dates are non-linear (weekends skipped).
            # But here we can use the integer index from 0 to len(df)-1 logic if we are careful.
            # mpf.plot aligns data on 0..N-1 x-axis.

            for i in range(len(df)):
                prev_idx = df['Bullish_Divergence'].iloc[i]
                if pd.notna(prev_idx):
                    prev_idx = int(prev_idx)
                    curr_idx = i

                    # Coordinates for Price Line (Green)
                    # X: indices, Y: Close prices
                    # Note: We use Low price for Bullish Divergence placement usually
                    y1 = df['Low'].iloc[prev_idx]
                    y2 = df['Low'].iloc[curr_idx]

                    ax_price.plot([prev_idx, curr_idx], [y1, y2], color='green', linewidth=2, linestyle='-')

                    # Coordinates for TSV Line (Green)
                    tsv1 = df['TSV'].iloc[prev_idx]
                    tsv2 = df['TSV'].iloc[curr_idx]
                    ax_tsv.plot([prev_idx, curr_idx], [tsv1, tsv2], color='green', linewidth=1.5, linestyle='--')

                    # Add 'B' Label
                    ax_price.annotate('B', (curr_idx, y2), textcoords="offset points", xytext=(0,-15),
                                      ha='center', color='green', fontsize=9, fontweight='bold')

        if 'Bearish_Divergence' in df.columns:
            for i in range(len(df)):
                prev_idx = df['Bearish_Divergence'].iloc[i]
                if pd.notna(prev_idx):
                    prev_idx = int(prev_idx)
                    curr_idx = i

                    # Coordinates for Price Line (Red)
                    # Use High price for Bearish
                    y1 = df['High'].iloc[prev_idx]
                    y2 = df['High'].iloc[curr_idx]

                    ax_price.plot([prev_idx, curr_idx], [y1, y2], color='red', linewidth=2, linestyle='-')

                    # Coordinates for TSV Line (Red)
                    tsv1 = df['TSV'].iloc[prev_idx]
                    tsv2 = df['TSV'].iloc[curr_idx]
                    ax_tsv.plot([prev_idx, curr_idx], [tsv1, tsv2], color='red', linewidth=1.5, linestyle='--')

                    # Add 'B' Label (Bear)
                    ax_price.annotate('B', (curr_idx, y2), textcoords="offset points", xytext=(0,10),
                                      ha='center', color='red', fontsize=9, fontweight='bold')

        fig.savefig(output_path, bbox_inches='tight', dpi=100)
        plt.close(fig)
        logger.info(f"Market chart generated at {output_path}")
        return True

    except Exception as e:
        logger.error(f"Error generating market chart: {e}")
        return False
