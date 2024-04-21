import pandas as pd
import pandas_ta as ta

def rsi(trading: pd.DataFrame, value='Close', name='', **kwargs):
    df = trading.copy(deep=True)

    period = kwargs.get(f'period{name}')
    rsi = ta.rsi(df[value], length=period, append=True)

    df.loc[:, f'RSI{name}'] = rsi

    return df