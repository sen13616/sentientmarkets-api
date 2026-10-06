"""
tests/test_finbert.py — FinBERT inference module tests.

Tests the scoring logic (score_text, score_batch) with a mocked model,
verifying score computation S_i = P(pos) - P(neg) and batch handling.

The actual ProsusAI/finbert model is NOT loaded in unit tests.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


def _make_mock_model_and_tokenizer(probs_list):
    """
    Create mock model and tokenizer that return predetermined probabilities.

    probs_list: list of (pos, neg, neu) tuples, one per call.
    """
    import torch

    mock_tokenizer = MagicMock()
    mock_model = MagicMock()

    # Model parameters check for CUDA
    mock_param = MagicMock()
    mock_param.is_cuda = False
    mock_model.parameters.return_value = iter([mock_param])

    call_idx = [0]

    def fake_forward(**kwargs):
        idx = call_idx[0]
        if idx < len(probs_list):
            batch = probs_list[idx:]
            # Determine batch size from input_ids
            batch_size = kwargs.get("input_ids", torch.zeros(1, 1)).shape[0]
            batch = probs_list[idx:idx + batch_size]
            call_idx[0] += batch_size
        else:
            batch = [probs_list[-1]]

        logits_data = []
        for pos, neg, neu in batch:
            # Pre-softmax logits that produce these probs approximately
            # Just use the probs directly since we'll mock softmax behavior
            logits_data.append([pos, neg, neu])

        logits = torch.tensor(logits_data)
        result = MagicMock()
        result.logits = logits
        return result

    mock_model.__call__ = fake_forward
    mock_model.return_value = MagicMock(logits=torch.zeros(1, 3))

    def fake_tokenizer_call(texts, **kwargs):
        if isinstance(texts, str):
            texts = [texts]
        return {
            "input_ids": torch.zeros(len(texts), 10, dtype=torch.long),
            "attention_mask": torch.ones(len(texts), 10, dtype=torch.long),
        }

    mock_tokenizer.side_effect = fake_tokenizer_call

    return mock_model, mock_tokenizer


class TestFinBERTScoreComputation:
    """Test S_i = P(pos) - P(neg) formula and output structure."""

    def test_score_positive(self):
        """Positive article: P(pos) > P(neg) → positive score."""
        import torch

        from pipeline.nlp.finbert import score_text

        # Mock the model to return known softmax outputs
        with patch("pipeline.nlp.finbert._model", None), \
             patch("pipeline.nlp.finbert._tokenizer", None):

            def mock_get_model():
                import pipeline.nlp.finbert as fb
                model = MagicMock()
                tokenizer = MagicMock()

                param = MagicMock()
                param.is_cuda = False
                model.parameters.return_value = iter([param])

                # Softmax output: pos=0.8, neg=0.1, neu=0.1
                logits = torch.tensor([[2.0, -1.0, -1.0]])
                model.return_value = MagicMock(logits=logits)
                tokenizer.return_value = {
                    "input_ids": torch.zeros(1, 5, dtype=torch.long),
                    "attention_mask": torch.ones(1, 5, dtype=torch.long),
                }
                fb._model = model
                fb._tokenizer = tokenizer
                return model, tokenizer

            with patch("pipeline.nlp.finbert._get_model", mock_get_model):
                result = score_text("Company reports strong earnings")

            assert "finbert_score" in result
            assert "finbert_pos" in result
            assert "finbert_neg" in result
            assert "finbert_neu" in result
            # With logits [2, -1, -1], softmax gives pos much higher than neg
            assert result["finbert_score"] > 0
            assert result["finbert_pos"] > result["finbert_neg"]

    def test_score_range(self):
        """finbert_score should be in [-1, +1]."""
        import torch

        from pipeline.nlp.finbert import score_text

        with patch("pipeline.nlp.finbert._model", None), \
             patch("pipeline.nlp.finbert._tokenizer", None):

            def mock_get_model():
                import pipeline.nlp.finbert as fb
                model = MagicMock()
                tokenizer = MagicMock()

                param = MagicMock()
                param.is_cuda = False
                model.parameters.return_value = iter([param])

                logits = torch.tensor([[0.5, 0.3, 0.2]])
                model.return_value = MagicMock(logits=logits)
                tokenizer.return_value = {
                    "input_ids": torch.zeros(1, 5, dtype=torch.long),
                    "attention_mask": torch.ones(1, 5, dtype=torch.long),
                }
                fb._model = model
                fb._tokenizer = tokenizer
                return model, tokenizer

            with patch("pipeline.nlp.finbert._get_model", mock_get_model):
                result = score_text("Test article")

            assert -1.0 <= result["finbert_score"] <= 1.0

    def test_probabilities_sum_approximately_one(self):
        """pos + neg + neu should sum to ~1.0 (softmax output)."""
        import torch

        from pipeline.nlp.finbert import score_text

        with patch("pipeline.nlp.finbert._model", None), \
             patch("pipeline.nlp.finbert._tokenizer", None):

            def mock_get_model():
                import pipeline.nlp.finbert as fb
                model = MagicMock()
                tokenizer = MagicMock()

                param = MagicMock()
                param.is_cuda = False
                model.parameters.return_value = iter([param])

                logits = torch.tensor([[1.0, 0.5, -0.5]])
                model.return_value = MagicMock(logits=logits)
                tokenizer.return_value = {
                    "input_ids": torch.zeros(1, 5, dtype=torch.long),
                    "attention_mask": torch.ones(1, 5, dtype=torch.long),
                }
                fb._model = model
                fb._tokenizer = tokenizer
                return model, tokenizer

            with patch("pipeline.nlp.finbert._get_model", mock_get_model):
                result = score_text("Test article")

            prob_sum = result["finbert_pos"] + result["finbert_neg"] + result["finbert_neu"]
            assert abs(prob_sum - 1.0) < 1e-5


class TestFinBERTBatch:
    """Test batch scoring."""

    def test_empty_batch(self):
        """Empty input → empty output."""
        from pipeline.nlp.finbert import score_batch
        assert score_batch([]) == []

    def test_batch_returns_correct_count(self):
        """Batch of N texts → N results."""
        import torch

        from pipeline.nlp.finbert import score_batch

        with patch("pipeline.nlp.finbert._model", None), \
             patch("pipeline.nlp.finbert._tokenizer", None):

            def mock_get_model():
                import pipeline.nlp.finbert as fb
                model = MagicMock()
                tokenizer = MagicMock()

                param = MagicMock()
                param.is_cuda = False
                model.parameters.return_value = iter([param])

                # Return 3 sets of logits for 3 articles
                logits = torch.tensor([
                    [2.0, -1.0, 0.0],
                    [-1.0, 2.0, 0.0],
                    [0.0, 0.0, 2.0],
                ])
                model.return_value = MagicMock(logits=logits)
                tokenizer.return_value = {
                    "input_ids": torch.zeros(3, 10, dtype=torch.long),
                    "attention_mask": torch.ones(3, 10, dtype=torch.long),
                }
                fb._model = model
                fb._tokenizer = tokenizer
                return model, tokenizer

            with patch("pipeline.nlp.finbert._get_model", mock_get_model):
                results = score_batch(["text1", "text2", "text3"])

            assert len(results) == 3
            # First article: positive dominant → positive score
            assert results[0]["finbert_score"] > 0
            # Second article: negative dominant → negative score
            assert results[1]["finbert_score"] < 0
            # All have the required keys
            for r in results:
                assert set(r.keys()) == {"finbert_score", "finbert_pos", "finbert_neg", "finbert_neu"}
