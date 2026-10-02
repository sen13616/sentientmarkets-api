"""
pipeline/nlp/finbert.py

FinBERT sentiment scoring for financial text articles.

Paper Stage 3 (lines 1128–1132):
    "Articles surviving validation and relevance filtering are passed through
    FinBERT (ProsusAI/finbert), a transformer-based sentiment model fine-tuned
    on financial text."

Score formula (paper line 238):
    S_i = P_i(positive) − P_i(negative)

Produces a continuous score in [-1, +1] from the three-class softmax output.

The model is loaded lazily on first call to avoid torch import at startup
(same pattern as dedup.py for sentence-transformers).
"""
from __future__ import annotations

import logging
import os

_log = logging.getLogger(__name__)

_model = None
_tokenizer = None
_device: str | None = None  # None = CPU (inputs stay where the tokenizer put them)

_MODEL_NAME = "ProsusAI/finbert"

# ProsusAI/finbert label order: positive=0, negative=1, neutral=2
_POS_IDX = 0
_NEG_IDX = 1
_NEU_IDX = 2


def _get_model():
    """Load the FinBERT model and tokenizer on first use (~440 MB)."""
    global _model, _tokenizer, _device
    if _model is None:
        import torch  # noqa: PLC0415
        from transformers import AutoModelForSequenceClassification, AutoTokenizer  # noqa: PLC0415

        _log.info("Loading FinBERT model '%s' …", _MODEL_NAME)
        _tokenizer = AutoTokenizer.from_pretrained(_MODEL_NAME)
        _model = AutoModelForSequenceClassification.from_pretrained(_MODEL_NAME)
        _model.eval()
        # FINBERT_DEVICE (e.g. "mps" on Apple silicon) is an explicit opt-in for
        # offline runs like scripts/backfill/news_backfill.py — ~7x faster than
        # one CPU thread, same probabilities to ~1e-7. Unset → unchanged
        # behaviour (CUDA if present, else CPU).
        device = os.environ.get("FINBERT_DEVICE")
        if device:
            _model = _model.to(device)
            _device = device
            _log.info("FinBERT loaded on %s (FINBERT_DEVICE)", device)
        elif torch.cuda.is_available():
            _model = _model.cuda()
            _device = "cuda"
            _log.info("FinBERT loaded on CUDA")
        else:
            _log.info("FinBERT loaded on CPU")
    return _model, _tokenizer


def score_text(text: str) -> dict:
    """
    Score a single text string with FinBERT.

    Returns
    -------
    dict with keys: finbert_score, finbert_pos, finbert_neg, finbert_neu.
    finbert_score = P(positive) - P(negative), range [-1, +1].
    """
    import torch  # noqa: PLC0415

    model, tokenizer = _get_model()
    inputs = tokenizer(
        text,
        return_tensors="pt",
        truncation=True,
        max_length=512,
    )
    if next(model.parameters()).is_cuda:
        inputs = {k: v.cuda() for k, v in inputs.items()}

    with torch.no_grad():
        logits = model(**inputs).logits
    probs = torch.softmax(logits, dim=1).squeeze()

    pos = probs[_POS_IDX].item()
    neg = probs[_NEG_IDX].item()
    neu = probs[_NEU_IDX].item()

    return {
        "finbert_score": pos - neg,
        "finbert_pos": pos,
        "finbert_neg": neg,
        "finbert_neu": neu,
    }


def score_batch(texts: list[str], batch_size: int = 32) -> list[dict]:
    """
    Score a batch of text strings with FinBERT.

    Processes `texts` in fixed-size chunks (default 32) to bound peak
    transient activation memory. Padding is applied per-chunk via the
    tokenizer's `padding=True`, so a single long article only pads its own
    chunk rather than the entire input.

    Returns
    -------
    list of dicts, each with keys: finbert_score, finbert_pos, finbert_neg,
    finbert_neu.
    """
    if not texts:
        return []

    import torch  # noqa: PLC0415

    model, tokenizer = _get_model()

    results: list[dict] = []
    for start in range(0, len(texts), batch_size):
        chunk = texts[start:start + batch_size]
        inputs = tokenizer(
            chunk,
            return_tensors="pt",
            truncation=True,
            max_length=512,
            padding=True,
        )
        if _device is not None:
            inputs = {k: v.to(_device) for k, v in inputs.items()}

        with torch.inference_mode():
            logits = model(**inputs).logits
        probs = torch.softmax(logits, dim=1)

        for i in range(len(chunk)):
            pos = probs[i][_POS_IDX].item()
            neg = probs[i][_NEG_IDX].item()
            neu = probs[i][_NEU_IDX].item()
            results.append({
                "finbert_score": pos - neg,
                "finbert_pos": pos,
                "finbert_neg": neg,
                "finbert_neu": neu,
            })
    return results
