# Docs inventory (draft)

| path | inbound_refs | decision | reason |
|---|---:|---|---|
| (to fill) | 0 | delete/move/keep | результаты lychee/grep |

Инструкции по генерации:

```bash
find docs -type f -name "*.md" | sort > /tmp/docs_all.txt
grep -RhoE "\]\((\.{0,2}/[^)#]+)" docs | sed 's/^\]\(//' | sed 's/#.*$//' | sort -u > /tmp/docs_links.txt
comm -23 /tmp/docs_all.txt /tmp/docs_links.txt > /tmp/docs_orphans.txt

# линк‑чекер
pipx install lychee
lychee --no-progress --exclude-mail --follow-remake docs
```
