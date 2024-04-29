from matplotlib import pyplot as plt
import pandas as pd

def plot_entry_points(data: pd.DataFrame, reset_index: bool = True, ploting_params = ['Close']):
    df = data.copy(deep=True)
    if reset_index:
        df.reset_index(inplace=True)
    # Set the size of the chart
    fig, ax = plt.subplots(figsize=(17, 7))
    ax.grid(True)

    # Plot the 'Close' column
    for plot_name in ploting_params: 
        ax.plot(df.index.values, df[plot_name], label=plot_name)

    # Plot positions with highlighting the range
    for index, (i, series) in enumerate(df.iterrows()):
        position = series['Position']
        if position != 0:
            if reset_index:
                ax.axvspan(xmin=i, xmax=i+1, color='green' if position == 1 else 'red', alpha=0.05)
            else:
                next_idx = min(index + 1, len(df) - 1)
                ax.axvspan(xmin=series.name, xmax=df.iloc[next_idx].name, color='green' if position == 1 else 'red', alpha=0.05)