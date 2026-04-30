import re
s = open('mes_crm.py', 'r', encoding='utf-8').read()
m = re.search(r'HTML_APP\s*=\s*r"""(.*?)"""', s, re.DOTALL)
html = m.group(1)
js_match = re.search(r'<script>(.*)</script>', html, re.DOTALL)
js = js_match.group(1)
open('_test.js', 'w', encoding='utf-8').write(js)
print('JS extracted, length:', len(js))

