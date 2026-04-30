import re
s = open('mes_crm.py','r',encoding='utf-8').read()
m = re.search(r'HTML_APP\s*=\s*r"""(.*?)"""', s, re.DOTALL)
html = m.group(1)
lines = html.splitlines()
for i in range(325, 345):
    if i < len(lines):
        print(i+1, '|', lines[i][:300])

