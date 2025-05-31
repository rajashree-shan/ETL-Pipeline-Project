def transform_data(df):
    df = df.dropna()
    df['revenue'] = df['quantity'] * df['price']
    return df
