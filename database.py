import sqlite3

conn = sqlite3.connect('dados.db', check_same_thread=False)

cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS temperaturas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    temperatura REAL,
    media REAL
)
''')

conn.commit()

def salvar(temperatura, media):
    cursor.execute(
        'INSERT INTO temperaturas (temperatura, media) VALUES (?, ?)',
        (temperatura, media)
    )

    conn.commit()

def listar():
    cursor.execute(
        'SELECT * FROM temperaturas ORDER BY id DESC LIMIT 10'
    )

    return cursor.fetchall()