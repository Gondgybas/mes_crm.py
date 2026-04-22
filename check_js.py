import re, sys

with open('mes_crm.py', encoding='utf-8') as f:
    content = f.read()

# Find HTML_APP
start = content.find('HTML_APP = r"""')
end = content.find('"""', start + 15)
html = content[start+15:end]
print(f"HTML length: {len(html)}")

# Find script tag
sm = html.find('<script>')
em = html.find('</script>')
js = html[sm+8:em]
print(f"JS length: {len(js)}")

# Check h+= occurrences
lines = js.split('\n')
for i, line in enumerate(lines, 1):
    stripped = line.strip()
    if 'h+=' in stripped and stripped.startswith("h+='"):
        print(f"  h+= at JS line {i}: {line[:100]}")

print("h+= count in JS:", js.count("h+="))
print("Done")

