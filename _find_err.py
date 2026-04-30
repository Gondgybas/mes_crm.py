import re

s = open('mes_crm.py', 'r', encoding='utf-8').read()
m = re.search(r'HTML_APP\s*=\s*r"""(.*?)"""', s, re.DOTALL)
html = m.group(1)
js_start = html.find('<script>')
js_end = html.find('</script>')
js = html[js_start+8:js_end]
lines = js.splitlines()
# Find "Unexpected string" - search for unmatched quotes / backticks
for i, line in enumerate(lines):
    if '`' in line:
        print(f"BACKTICK at JS line {i+1}: {line[:200]}")
    cnt = line.count("'") - line.count("\\'")
    if cnt % 2 != 0:
        print(f"ODD QUOTE at JS line {i+1}: {line[:200]}")

