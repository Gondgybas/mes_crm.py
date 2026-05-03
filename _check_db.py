import sqlite3
conn = sqlite3.connect('mes_v5.db')
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
print("Tables:", [t[0] for t in tables])

# Check user_op_types
if ('user_op_types',) in tables:
    rows = conn.execute("SELECT * FROM user_op_types").fetchall()
    print("user_op_types rows:", rows)
else:
    print("TABLE user_op_types DOES NOT EXIST!")

conn.close()

