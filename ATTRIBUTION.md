# Third-Party Attribution

## AI-Video-Transcriber (AVT)

This product includes code adapted from [AI-Video-Transcriber](https://github.com/littlebearapps/AI-Video-Transcriber)
(AVT), licensed under the Apache License, Version 2.0.

The following Untether source files are derivative works of AVT:

- `src/untether/telegram/voice_groq.py` — adapted from `backend/groq_transcriber.py`
- `src/untether/telegram/voice_local.py` — adapted from:
  - `backend/transcriber.py` (Whisper local transcriber)
  - `backend/parakeet_transcriber.py` (Parakeet local transcriber)
  - `backend/local_transcription.py` (model resolution and audio preparation)

Modifications made by Untether (Little Bear Apps):
- Native `anyio` integration for async/worker-thread execution
- Per-instance model caching with serialization locks
- Plain-text transcript output (no Markdown formatting)
- Untether-specific user agent, cache directory, and logging
- No runtime `pip install` behavior (dependencies declared at install time)

The original AVT source is licensed under the Apache License, Version 2.0.
See `LICENSES/APACHE-2.0.txt` for the full license text.

Original copyright: Copyright 2024-2026 AI-Video-Transcriber contributors.
