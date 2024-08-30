import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import time

# URL of the webpage
url = 'https://screener.in/company/RELIANCE/consolidated/'

# Fetch the webpage
webpage = requests.get(url)

# Parse the HTML content
soup = bs(webpage.text, 'html.parser')

# Find the profit and loss section
data = soup.find('section', id="profit-loss")

# Find the table within the section
tdata = data.find("table")

# Extract table data
table_data = []
for row in tdata.find_all('tr'):
    row_data = []
    for cell in row.find_all(['th', 'td']):
        row_data.append(cell.text.strip())
    table_data.append(row_data)

# Create a DataFrame from the table data
# df_table = pd.DataFrame(table_data)

# # Set the first row as the header and drop it from the DataFrame
# df_table.iloc[0, 0] = 'Section'
# df_table.columns = df_table.iloc[0]
# df_table = df_table.iloc[1:, :-1]

# # Clean and transform the data
# for i in df_table.iloc[:, 1:].columns:
#     df_table[i] = df_table[i].str.replace(',', '').str.replace('%', '').apply(eval)

df_table = pd.DataFrame(table_data)
df_table= df_table.T
df_table.iloc[0,0] = 'Year'
df_table.columns = df_table.iloc[0]
df_table = df_table.iloc[1:]
df_table = df_table.drop(df_table.index[-1])
for i in df_table.iloc[:,1:].columns:
    df_table[i] = df_table[i].str.replace(',','').str.replace('%','').apply(eval)

df_table['Stock'] = "Rel" 
df_table = df_table.reset_index()
print(df_table)


db_params = {
    'dbname': 'exampledb',
    'user': 'docker',
    'password': 'docker',
    'host': '172.27.80.1',
    'port': 5432
}

conn = psycopg2.connect(**db_params)
cur = conn.cursor()

create_table_query = '''
CREATE TABLE IF NOT EXISTS profit_loss_data (
    index BIGINT primary key,
    Year TEXT,
    Sales BIGINT,
    Expenses BIGINT,
    Operating_Profit BIGINT,
    OPM_Percent INTEGER,
    Other_Income BIGINT,
    Interest BIGINT,
    Depreciation BIGINT,
    Profit_before_tax BIGINT,
    Tax_Percent INTEGER,
    Net_Profit BIGINT,
    EPS_in_Rs DOUBLE PRECISION,
    Dividend_Payout_Percent INTEGER,
    Stock TEXT
);
'''

cur.execute(create_table_query)
conn.commit()
print("Table and trigger table created")    

insert_query = '''
INSERT INTO profit_loss_data (
    Index,Year, Sales, Expenses, Operating_Profit, OPM_Percent, Other_Income, Interest, Depreciation,
    Profit_before_tax, Tax_Percent, Net_Profit, EPS_in_Rs,Dividend_Payout_Percent, Stock
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s);
'''

for _, row in df_table.iterrows():
    cur.execute(insert_query, tuple(row))
    conn.commit()  # Commit after each insert
    print(f"Inserted data for year: {row['index']}")
    time.sleep(3)


print("Data loaded successfully into PostgreSQL database!")
 



