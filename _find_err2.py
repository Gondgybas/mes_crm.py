import re, sys

s = open('mes_crm.py', 'r', encoding='utf-8').read()
m = re.search(r'HTML_APP\s*=\s*r"""(.*?)"""', s, re.DOTALL)
html = m.group(1)

# Find script tag
script_start = html.find('<script>')
script_end = html.rfind('</script>')
print(f"Script starts at HTML char {script_start}, line ~{html[:script_start].count(chr(10))+1}")

js = html[script_start+8:script_end]
lines = js.splitlines()
print(f"Total JS lines: {len(lines)}")

# Browser says syntax error - check for common issues
# Look for 'var ... = ... "string"' pattern (missing operator)
for i, line in enumerate(lines):
    # Check for two consecutive string-like tokens without operator
    stripped = line.strip()
    if re.search(r"""['"]\s+['"]\s*[+\-\*]""", stripped) is None:
        # Look for string followed by identifier or another string NOT via + concat
        if re.search(r"""'\s+'""", stripped) or re.search(r""""\s+[a-zA-Z]""", stripped):
            print(f"POSSIBLE ISSUE line {i+1}: {stripped[:200]}")

