# 4. Security: Secret Redaction

## Overview

Security is a primary concern for the logging system. Accidentally logging
sensitive information, such as API keys or passwords, can create significant
security risks. To mitigate this, the `bioetl` framework includes a built-in,
two-layer mechanism for automatically redacting sensitive data from logs.

This feature is enabled by default (`redact_secrets: True` in `LoggerConfig`)
and should only be disabled in non-production environments with extreme caution.

## The Redaction Mechanism

Redaction is performed at two distinct stages of the logging pipeline to ensure
comprehensive coverage:

### 1. `structlog` Processor (`redact_secrets_processor`)

This processor operates on the structured event dictionary before it is
rendered. It recursively iterates through the dictionary's keys and values. If
any **key** matches a keyword from the sensitive list, its corresponding
**value** is replaced with the string `"[REDACTED]"`.

This is the primary layer of defense and covers all structured data passed to
the logger.

**Example:** If you log
`log.info("API Request", headers={"Authorization": "Bearer xyz123"})`, the
processor will inspect the `headers` dictionary. It will find the key
`"Authorization"`, match it against the keyword list, and transform the event
dictionary before it is rendered.

**Rendered Log:**

```json
{
  "message": "API Request",
  "headers": {
    "Authorization": "[REDACTED]"
  },
  ...
}
```

### 2. `logging.Filter` (`SecretRedactionFilter`)

Some legacy code or third-party libraries may use the standard `logging` library
directly, creating unstructured string messages. The `structlog` processor would
not be able to inspect these.

To cover this case, a standard `logging.Filter` is also attached to the logger.
This filter uses regular expressions to find and replace sensitive key-value
pairs within the final, formatted log string. It provides a second, powerful
layer of defense.

## Redacted Keywords

The redaction mechanism is triggered by a predefined list of case-insensitive
keywords. If any of these substrings appear in a key, the value associated with
that key will be redacted.

The current list of sensitive keywords is:

- `api_key`
- `token`
- `password`
- `secret`
- `authorization`
- `bearer`
- `credential`
- `access_token`
- `refresh_token`
- `private_key`
- `x-api-key`

**Example of Keyword Matching:**

| Original Key              | Matches Keyword | Redacted? |
| ------------------------- | --------------- | --------- |
| `"api_key"`               | `api_key`       | Yes       |
| `"MyApi_Key"`             | `api_key`       | Yes       |
| `"Authorization"`         | `authorization` | Yes       |
| `"user_password_hash"`    | `password`      | Yes       |
| `"secret_ingredient"`     | `secret`        | Yes       |
| `"user_id"`               | (no match)      | No        |
| `"authentication_method"` | (no match)      | No        |

By implementing this dual-layer, automated redaction system, the framework
significantly reduces the risk of sensitive data exposure, making the logs safer
for storage and analysis in production environments.
