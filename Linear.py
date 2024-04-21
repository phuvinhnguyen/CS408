import pandas as pd
import pandas_ta as ta

def linear(trading: pd.DataFrame, value='Close', name='', **kwargs):
    df = trading.copy(deep=True)

    window = kwargs.get(f'window{name}')
    df[f'Slope{name}'] = ta.slope(df[value], length=window)
    return df