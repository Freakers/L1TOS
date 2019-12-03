import pandas as pd

filename = "C:\\logs\\2019-11-22\\Europe.PA.csv"

output_filename = 'C:\\logs\\2019-11-22\\example.csv'

# df = pd.read_csv(filename)


with open(filename, 'r') as foo:
    bar = foo.readlines()

foo = []

for item in bar:
    foo.append(item.split(','))

foobar = []

# ['LocalTime=10:39:02.493', 'Message=TOS', 'MarketTime=16:39:01.303', 'Symbol=AB.PA', 'Type=0', 'Price=5.430000', 'Size=1000', 'Source=138', 'Condition=0', 'Tick=D', 'Mmid=E', 'SubMarketId=32', 'Date=2019-11-15', 'BuyerId=0', 'SellerId=0\n']

columns = []

for lineitem in foo[0]:
    columns.append(lineitem.split('=')[0])

for lineitem in foo:
    foobar.append([x.split('=')[1].replace('\n', '') for x in lineitem])

with open(output_filename, 'w') as working_file:
    working_file.write(','.join(columns) + '\n')

    for working_line in foobar:
        working_file.write(','.join(working_line) + '\n')

final_df = pd.read_csv(output_filename)

final_df['Datetime'] = final_df['Date'] + ' ' + final_df['MarketTime']
final_df['Datetime'] = pd.to_datetime(final_df['Datetime'], format='%Y-%m-%d %H:%M:%S.%f')

final_df = final_df.sort_values(['Symbol', 'Datetime'])

final_df.to_csv(output_filename, index=False)

