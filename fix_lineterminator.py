import re

with open('src/library/io_/read_write.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('options["line_terminator"]', 'options["lineterminator"]')

with open('src/library/io_/read_write.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Fixed!")
