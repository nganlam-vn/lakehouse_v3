import requests

url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=AC6KBU0FPU0346LS'
r = requests.get(url)
data = r.json()

print(data)