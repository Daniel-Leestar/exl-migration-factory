"""A swappable connection to a language model, with every call written to a log.
可替換的語言模型連線,每次呼叫都會寫入紀錄。

Ollama runs the model on this machine, so prompts never leave the laptop.
Ollama 在本機執行模型,因此 prompt 不會離開這台電腦。
"""
from __future__ import annotations

import datetime as dt
import json
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable, Protocol


class LLMError(Exception):
    """The model could not be reached or did not answer usefully.
    無法連上模型,或模型沒有給出可用的回答。
    """


class LLMClient(Protocol):
    model: str

    def complete(self, system: str, prompt: str) -> str: ...


class OllamaClient:
    """Talk to a local Ollama server over HTTP.
    透過 HTTP 與本機的 Ollama 伺服器溝通。

    Retryable: connection refused / timeout (the server may still be loading the model).
    Not retryable: HTTP 4xx such as "model not found" (retrying cannot fix it).
    可重試:連線被拒/逾時(伺服器可能還在載入模型)。
    不可重試:HTTP 4xx,例如找不到模型(重試也沒用)。
    """

    def __init__(self, model: str = "qwen2.5-coder:3b", host: str = "http://localhost:11434",
                 timeout_seconds: float = 600, retries: int = 1):
        self.model = model
        self._url = host.rstrip("/") + "/api/chat"
        self._timeout = timeout_seconds
        self._retries = retries

    def complete(self, system: str, prompt: str) -> str:
        payload = json.dumps({
            "model": self.model,
            "stream": False,
            # Temperature 0 keeps answers as repeatable as a local model allows.
            # temperature 設 0,讓回答在本機模型能做到的範圍內盡量可重現。
            "options": {"temperature": 0, "num_ctx": 8192},
            "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}],
        }).encode("utf-8")
        request = urllib.request.Request(self._url, data=payload, headers={"Content-Type": "application/json"})
        last_error = None
        for attempt in range(self._retries + 1):
            try:
                with urllib.request.urlopen(request, timeout=self._timeout) as response:
                    body = json.loads(response.read().decode("utf-8"))
                text = (body.get("message") or {}).get("content")
                if not text:
                    raise LLMError(f"Ollama returned no text for model {self.model}")
                return text
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", "replace")[:200]
                raise LLMError(f"Ollama rejected the request (HTTP {exc.code}): {detail}") from exc
            except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                last_error = exc
                if attempt < self._retries:
                    time.sleep(3)
            except json.JSONDecodeError as exc:
                raise LLMError("Ollama returned a response that is not JSON") from exc
        raise LLMError(f"cannot reach Ollama at {self._url} for model {self.model}: {last_error}")


class FakeLLM:
    """A stand-in for tests: answers from a list or a function, no network.
    測試用替身:從清單或函數取得回答,不需要網路。
    """

    def __init__(self, answers: list[str] | Callable[[str, str], str], model: str = "fake"):
        self.model = model
        self._answers = answers
        self.calls: list[tuple[str, str]] = []

    def complete(self, system: str, prompt: str) -> str:
        self.calls.append((system, prompt))
        if callable(self._answers):
            return self._answers(system, prompt)
        if not self._answers:
            raise LLMError("fake model has no more answers")
        return self._answers.pop(0)


class LoggedLLM:
    """Wrap a client so that every prompt, answer and error lands in a JSON-lines file.
    包住一個 client,讓每個 prompt、回答與錯誤都寫進 JSON-lines 檔。
    """

    def __init__(self, inner: LLMClient, log_path: Path, job_id: str):
        self.model = inner.model
        self._inner = inner
        self._log_path = Path(log_path)
        self._job_id = job_id
        self.purpose = "unspecified"

    def complete(self, system: str, prompt: str) -> str:
        started = time.time()
        record = {
            "time": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            "job": self._job_id,
            "purpose": self.purpose,
            "model": self.model,
            "system": system,
            "prompt": prompt,
        }
        try:
            answer = self._inner.complete(system, prompt)
            record["response"] = answer
            return answer
        except LLMError as exc:
            record["error"] = str(exc)
            raise
        finally:
            record["seconds"] = round(time.time() - started, 2)
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
            with self._log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
