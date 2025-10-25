# Security Policy

## XML External Entity (XXE) Protection

Все XML парсеры создаются через `library.xml.make_xml_parser()` с безопасными настройками:

- `resolve_entities=False` — блокирует разрешение внешних сущностей
- `no_network=True` — запрещает сетевые обращения из DTD/XInclude
- `load_dtd=False` — не загружает DTD по умолчанию

### Примеры защиты

**❌ Уязвимый код:**
```python
import xml.etree.ElementTree as ET
root = ET.fromstring(untrusted_xml)  # XXE vulnerability
```

**✅ Безопасный код:**

```python
from library.xml import make_xml_parser
from lxml import etree

parser = make_xml_parser()  # no_network=True, resolve_entities=False
root = etree.fromstring(xml_text.encode('utf-8'), parser)
```

### Дополнительные меры безопасности

1. **Валидация входных данных**: Все XML данные валидируются перед парсингом
2. **Ограничение размера**: Большие XML документы обрабатываются через streaming
3. **Логирование**: Все XML операции логируются для аудита
4. **Тестирование**: Регулярные security тесты включают XXE атаки

## Reporting Security Vulnerabilities

Если вы обнаружили уязвимость безопасности, пожалуйста:

1. НЕ создавайте публичный issue
2. Отправьте email на [security@yourdomain.com](mailto:security@yourdomain.com)
3. Включите детальное описание уязвимости
4. Укажите шаги для воспроизведения

Мы ответим в течение 48 часов и предоставим обновление в течение 7 дней.
