import pandas as pd
from iexfinance.stocks import Stock
from iexfinance.stocks import get_historical_data
import iexfinance
from datetime import datetime
import os
pd.options.mode.chained_assignment = None

def get_prices(symbol, date):
    """Gets stock prices from IEX Cloud API"""
    # Sandbox API Key
    os.environ['IEX_API_VERSION'] = 'iexcloud-sandbox'
    os.environ['IEX_TOKEN'] = 'Tsk_8e26ec2ad58840f287bb6444fcc5f2de'

    # # Loop through each row of input df
    # try:
    #     # Call IEX Cloud API to get historical price data
    #     price_df = get_historical_data(symbol, date, close_only=True, output_format='pandas')
    #     price = price_df['close'].values[0]
    #     print(symbol + " was @ $" + str(price) + " on " + str(date))
    #     new_stock_data.loc[index, 'Price'] = price
    # except (ValueError, iexfinance.utils.exceptions.IEXQueryError) as e:
    #     print("Couldn't get price for " + symbol + ".")
    # except KeyError:
    #     # Try previous day until a valid price is found (e.g., skip wkdns/holidays)
    #     return get_prices(symbol, date=str(int(date) - 1))
    #     print(stock + " was @ $" + str(price) + " on " + str(date))
    #     new_stock_data.loc[index, 'Price'] = price

    return new_stock_data

def get_financials(new_stock_data, current_quarter):
    """Gets TTM financial data (e.g., Net Income, Book Value, FCF, Shares Outstanding) from IEX API"""

    # Create two dataframes; one with current quarter data, the other with past quarters
    current_quarter_df = new_stock_data[(new_stock_data['Period Date'] == current_quarter)]
    past_quarters_df = new_stock_data[(new_stock_data['Period Date'] != current_quarter)]

    # Create list of tickers to get financial data for
    input_tickers = current_quarter_df['Symbol'].to_list()
    input_tickers = list(dict.fromkeys(input_tickers))

    def ticker_prep(input_tickers):
        """Checks IEX and returns list of valid tickers so as not to waste API credits"""

        # Set IEX Finance API Token (IEXsandbox@gmail.com)
        os.environ['IEX_API_VERSION'] = 'iexcloud-sandbox'
        os.environ['IEX_TOKEN'] = 'Tsk_8e26ec2ad58840f287bb6444fcc5f2de'

        valid_tickers = []
        for ticker in input_tickers:
            company = Stock(ticker, output_format='pandas')
            try:
                company.get_income_statement()
                valid_tickers.append(ticker)
                print("✓ " + ticker)
            except (
                    KeyError, iexfinance.utils.exceptions.IEXQueryError) as e:
                print("x " + ticker)
        return valid_tickers

    def fetch_company_info(group):
        """Function to query API data"""

        # Set IEX Finance API Token (IEXsandbox@gmail.com)
        os.environ['IEX_API_VERSION'] = 'iexcloud-sandbox'
        os.environ['IEX_TOKEN'] = 'Tsk_8e26ec2ad58840f287bb6444fcc5f2de'

        batch = Stock(group, output_format='pandas')

        # Get income from last 4 quarters, sum it, and store to temp df
        df_income = batch.get_income_statement(period="quarter", last='4')
        df_income = df_income.T.sum(level=0)
        income_ttm = df_income.loc[:, ['netIncome']]

        # Get book value from most recent quarter, and store to temp df
        df_book = batch.get_balance_sheet(period="quarter")
        df_book.columns = df_book.columns.get_level_values(0)
        book_value = df_book.loc['shareholderEquity']

        # Get free cash flow from last 4 quarters, sum it, and store to temp df
        df_cash_flow = batch.get_cash_flow(period="quarter", last='4')
        df_cash_flow = df_cash_flow.T.sum(level=0)
        fcf_ttm = df_cash_flow['cashFlow'] + df_cash_flow[
            'capitalExpenditures']

        # Get shares outstanding from most recent quarter, and store to temp df
        df_shares = batch.get_key_stats(period="quarter")
        shares_outstanding = df_shares.loc['sharesOutstanding']

        return income_ttm, book_value, fcf_ttm, shares_outstanding

    tickers = ticker_prep(input_tickers)

    # Chunk ticker list into n# of lists to save on API fees
    n = 100
    batch_tickers = [tickers[i * n:(i + 1) * n] for i in range((len(tickers) + n - 1) // n)]

    # Loop through each chunk of tickers
    for i in range(len(batch_tickers)):
        group = batch_tickers[i]
        company_info = fetch_company_info(group)
        # concat income and shares outstanding
        company_df = pd.concat(company_info, axis=1, sort='true')
        # instantiate output_df to be company_info with first row
        if (i == 0):
            output_df = company_df
        # for other rows, concat company_df
        else:
            output_df = pd.concat([output_df, company_df], axis=0)

    # Add results to temp df
    output_df.columns = ['Net Income', 'Book Value', 'Free Cash Flow', 'Shares Outstanding']
    output_df = output_df[['Net Income', 'Book Value', 'Free Cash Flow', 'Shares Outstanding']]
    output_df = output_df.reset_index()
    output_df = output_df.rename(columns={'index': 'Symbol'})

    # Merge output df with current_quarter_df
    current_quarter_df = output_df.merge(current_quarter_df, on='Symbol')
    current_quarter_df = current_quarter_df[
        ['Company Name', 'Symbol', 'CUSIP', 'Period Date', 'Price', 'Net Income', 'Book Value',
         'Free Cash Flow', 'Shares Outstanding']]

    # Re-combine current_quarter_df with past_quarters_df
    financials_df = current_quarter_df.merge(past_quarters_df, 'outer')

    financials_df = financials_df[['Period Date', 'CUSIP', 'Company Name',	'Symbol', 'Price', 'Net Income', 'Book Value', 'Free Cash Flow', 'Shares Outstanding']]

    return financials_df

holdings_df = pd.read_csv("data.csv")
stock_data = pd.read_csv("stock_data.csv")

holdings_df = holdings_df[['Period Date', 'CUSIP', 'Company Name', 'Symbol']]
holdings_df = holdings_df.drop_duplicates(subset=['Period Date', 'CUSIP'])

# Create dataframe of new stocks to lookup financial data for
new_stock_data = (holdings_df.merge(stock_data, on=['Symbol', 'CUSIP', 'Period Date'],
                                    indicator=True, how='left', suffixes=('', '_')).query("_merge == 'left_only'")[holdings_df.columns])

new_stock_data = new_stock_data.sort_values(["Symbol", "Period Date"]).reset_index(drop=True)

# Exlcude entries where the symbol couldn't be found
new_stock_data = new_stock_data[new_stock_data['Symbol'] != 'No Symbol']

# Loop to find stock prices using IEX Cloud
for index, row in new_stock_data.iterrows():
    symbol = row['Symbol']
    date = row['Period Date']
    # Convert date to datetime format
    date = datetime.strptime(date, '%Y-%m-%d')
    # Convert datetime to "YYYYMMDD" Format
    date = date.strftime('%Y%m%d')
    get_prices(symbol, date)

# Get TTM stock data (e.g., Net Income, Book Value, FCF, Shares Outstanding)
df = get_financials(new_stock_data, '2020-03-31')
print(df)

# Save to stock data file
df.to_csv('stock_data.csv', mode='a', header=False, index=False)
