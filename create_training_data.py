import sqlite3, pandas as pd

input_dbs = ['2018-08']
output_dir = 'data/formatted/'

for db in input_dbs:
  conn = sqlite3.connect(f'{db}.db')
  cursor = conn.cursor()
  limit = 5000
  last_time = 0
  curr_length = limit
  counter = 0
  test_done = False
  while curr_length == limit:
    data_frame = pd.read_sql(
        f"""
      SELECT * 
      FROM parent_reply 
      WHERE unix > {last_time} AND parent NOT NULL AND score > 0 
      ORDER BY unix ASC LIMIT {limit}
      """, conn
    )
    last_time = data_frame.tail(1)['unix'].values[0]
    curr_length = len(data_frame)
    if (not test_done):
      with open(f"{output_dir}test_from", 'a', encoding='utf8') as file:
        for content in data_frame['parent'].values:
          file.write(content + '\n')
      with open(f"{output_dir}test_to", 'a', encoding='utf8') as file:
        for content in data_frame['comment'].values:
          file.write(content + '\n')
      test_done = True
    else:
      with open(f"{output_dir}train_from", 'a', encoding='utf8') as file:
        for content in data_frame['parent'].values:
          file.write(content + '\n')
      with open(f"{output_dir}train_to", 'a', encoding='utf8') as file:
        for content in data_frame['comment'].values:
          file.write(content + '\n')
    counter += 1
    if (counter % 20 == 0):
      print(counter * limit, 'rows completed so far')
print('Completed Execution')
