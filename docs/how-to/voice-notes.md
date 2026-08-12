# Voice notes

Dictate coding tasks hands-free. Untether downloads each Telegram voice note once, then tries the configured transcription providers in order; the first nonblank result runs as a normal text prompt.

## Enable transcription

=== "untether config"

    ```sh
    untether config set transports.telegram.voice_transcription true
    untether config set transports.telegram.voice_transcription_providers '["avt", "groq", "local", "openai"]'
    untether config set transports.telegram.voice_transcription_language "en"
    ```

=== "toml"

    ```toml
    [transports.telegram]
    voice_transcription = true
    voice_transcription_providers = ["avt", "groq", "local", "openai"] # default order
    voice_transcription_language = "en" # optional ISO-639-1 hint
    ```

The array may contain any nonempty subset in any order. For example, to use Groq first and OpenAI as fallback:

```toml
[transports.telegram]
voice_transcription = true
voice_transcription_providers = ["groq", "openai"]
voice_transcription_groq_api_key = "gsk_..."
voice_transcription_api_key = "sk-..." # optional OpenAI fallback key
```

## Provider prerequisites

- **`avt`** — install the external AVT CLI and set `voice_transcription_local_command` to its executable path. This provider runs as an external process.
- **`groq`** — set `voice_transcription_groq_api_key` or `GROQ_API_KEY`. Untether uses its native Groq multipart adapter.
- **`local`** — install one optional engine: `pip install untether[whisper]` or `pip install untether[parakeet]`. Select it with `voice_transcription_local_backend = "whisper"` or `"parakeet"`, and set `voice_transcription_local_model` as needed. These native adapters are ports adapted from AVT (Apache-2.0).
- **`openai`** — set `voice_transcription_api_key` or `OPENAI_API_KEY`; `voice_transcription_model` and `voice_transcription_base_url` configure the OpenAI SDK path.

Provider failures (missing credentials, unavailable dependencies, timeouts, API/process errors, or blank output) advance to the next configured provider. If every provider fails, Untether sends one provider-neutral reply: `voice transcription is unavailable. please type your message instead.`

!!! tip "Hot-reload"
    Voice transcription settings, including provider order, can be edited in `untether.toml` and take effect without restarting when `watch_config = true`.

!!! warning "Private OpenAI-compatible endpoints"
    `voice_transcription_base_url` is SSRF-validated. Loopback/private hosts require a matching `voice_transcription_url_allowlist` CIDR/IP entry.

## Behavior

When transcription succeeds, the transcript is routed through the same command and directive pipeline as typed text.
!!! user "You"
    🎤 *(voice note — 0:12)*

!!! untether "Untether"
    📝 *"Add error handling to the upload function and make sure it retries on timeout"*

    working · claude · 0s

    ▸ Read `src/upload.py`

<img src="../assets/screenshots/voice-transcription.jpg" alt="Voice note followed by transcribed text and agent run output" width="360" loading="lazy" />

## Related

- [Config reference](../reference/config.md)
