"""
Unit tests for the quality scoring functionality in the data quality validation framework.

Tests various scoring models, weight normalization, and threshold validation to ensure
accurate quality assessment of validation results.
"""
import pytest  # package_version: 7.3.1
from unittest import mock  # package_version: standard library

from src.backend.quality.engines.quality_scorer import QualityScorer, ScoringModel, calculate_simple_score, calculate_weighted_score, calculate_impact_score, normalize_weights  # src/backend/quality/engines/quality_scorer.py
from src.backend.constants import QualityDimension, VALIDATION_STATUS_PASSED, VALIDATION_STATUS_FAILED  # src/backend/constants.py
from src.test.fixtures.backend.quality_fixtures import create_test_validation_result, sample_validation_results  # src/test/fixtures/backend/quality_fixtures.py


def test_quality_scorer_initialization():
    """Tests the initialization of QualityScorer with different models"""
    # Create QualityScorer with default model
    scorer = QualityScorer()
    # Verify default model is ScoringModel.WEIGHTED
    assert scorer._model == ScoringModel.WEIGHTED

    # Create QualityScorer with SIMPLE model
    scorer = QualityScorer(model=ScoringModel.SIMPLE)
    # Verify model is set correctly
    assert scorer._model == ScoringModel.SIMPLE

    # Create QualityScorer with IMPACT model
    scorer = QualityScorer(model=ScoringModel.IMPACT)
    # Verify model is set correctly
    assert scorer._model == ScoringModel.IMPACT

    # Create QualityScorer with ADAPTIVE model
    scorer = QualityScorer(model=ScoringModel.ADAPTIVE)
    # Verify model is set correctly
    assert scorer._model == ScoringModel.ADAPTIVE


def test_quality_scorer_set_model():
    """Tests changing the scoring model after initialization"""
    # Create QualityScorer with default model
    scorer = QualityScorer()

    # Change model to ScoringModel.SIMPLE
    scorer.set_model(ScoringModel.SIMPLE)
    # Verify model is updated correctly
    assert scorer._model == ScoringModel.SIMPLE

    # Change model to ScoringModel.IMPACT
    scorer.set_model(ScoringModel.IMPACT)
    # Verify model is updated correctly
    assert scorer._model == ScoringModel.IMPACT

    # Change model to ScoringModel.ADAPTIVE
    scorer.set_model(ScoringModel.ADAPTIVE)
    # Verify model is updated correctly
    assert scorer._model == ScoringModel.ADAPTIVE

    # Test that invalid model raises ValueError
    with pytest.raises(ValueError):
        scorer.set_model("INVALID_MODEL")


def test_quality_scorer_threshold():
    """Tests setting and getting quality threshold"""
    # Create QualityScorer with default threshold
    scorer = QualityScorer()
    # Verify default threshold is 0.8
    assert scorer.get_quality_threshold() == 0.8

    # Set threshold to 0.9
    scorer.set_quality_threshold(0.9)
    # Verify threshold is updated correctly
    assert scorer.get_quality_threshold() == 0.9

    # Set threshold to 0.5
    scorer.set_quality_threshold(0.5)
    # Verify threshold is updated correctly
    assert scorer.get_quality_threshold() == 0.5

    # Test that invalid threshold (>1.0) raises ValueError
    with pytest.raises(ValueError):
        scorer.set_quality_threshold(1.1)

    # Test that invalid threshold (<0.0) raises ValueError
    with pytest.raises(ValueError):
        scorer.set_quality_threshold(-0.1)


def test_quality_scorer_passes_threshold():
    """Tests the passes_threshold method with different scores"""
    # Create QualityScorer with threshold 0.8
    scorer = QualityScorer()

    # Test score 0.9 passes threshold
    assert scorer.passes_threshold(0.9)

    # Test score 0.8 passes threshold (equal to threshold)
    assert scorer.passes_threshold(0.8)

    # Test score 0.7 fails threshold
    assert not scorer.passes_threshold(0.7)

    # Change threshold to 0.6
    scorer.set_quality_threshold(0.6)
    # Test score 0.7 now passes threshold
    assert scorer.passes_threshold(0.7)

    # Test score 0.5 fails threshold
    assert not scorer.passes_threshold(0.5)


def test_calculate_simple_score():
    """Tests the simple scoring model based on pass/fail ratio"""
    # Create list of validation results with varying pass/fail statuses
    results = [
        {"passed": True},
        {"passed": False},
        {"passed": True},
        {"passed": False},
        {"passed": True}
    ]

    # Calculate simple score using calculate_simple_score
    score = calculate_simple_score(results)
    # Verify score equals (passed_count / total_count)
    assert score == 3 / 5

    # Test with all passing results (score should be 1.0)
    all_pass_results = [{"passed": True} for _ in range(5)]
    assert calculate_simple_score(all_pass_results) == 1.0

    # Test with all failing results (score should be 0.0)
    all_fail_results = [{"passed": False} for _ in range(5)]
    assert calculate_simple_score(all_fail_results) == 0.0

    # Test with empty results list (score should be 0.0)
    assert calculate_simple_score([]) == 0.0


def test_calculate_weighted_score():
    """Tests the weighted scoring model based on quality dimensions"""
    # Create list of validation results with different quality dimensions
    results = [
        {"dimension": QualityDimension.COMPLETENESS, "passed": True},
        {"dimension": QualityDimension.ACCURACY, "passed": False},
        {"dimension": QualityDimension.COMPLETENESS, "passed": True},
        {"dimension": QualityDimension.CONSISTENCY, "passed": False},
        {"dimension": QualityDimension.TIMELINESS, "passed": True}
    ]

    # Define dimension weights dictionary
    weights = {
        QualityDimension.COMPLETENESS: 0.3,
        QualityDimension.ACCURACY: 0.2,
        QualityDimension.CONSISTENCY: 0.2,
        QualityDimension.TIMELINESS: 0.3
    }

    # Calculate weighted score using calculate_weighted_score
    score = calculate_weighted_score(results, weights)
    # Verify score is calculated correctly based on dimension weights
    expected_score = (0.3 * (2/2)) + (0.2 * (0/1)) + (0.2 * (0/1)) + (0.3 * (1/1))
    assert score == expected_score

    # Test with custom weights emphasizing different dimensions
    custom_weights = {
        QualityDimension.COMPLETENESS: 0.1,
        QualityDimension.ACCURACY: 0.5,
        QualityDimension.CONSISTENCY: 0.3,
        QualityDimension.TIMELINESS: 0.1
    }
    custom_score = calculate_weighted_score(results, custom_weights)
    expected_custom_score = (0.1 * (2/2)) + (0.5 * (0/1)) + (0.3 * (0/1)) + (0.1 * (1/1))
    assert custom_score == expected_custom_score

    # Test with missing dimension in weights (should use default weight)
    missing_weights = {
        QualityDimension.COMPLETENESS: 0.4,
        QualityDimension.ACCURACY: 0.6
    }
    missing_score = calculate_weighted_score(results, missing_weights)
    # The missing dimensions should be ignored, and the weights should be normalized
    expected_missing_score = (0.4 * (2/2)) + (0.6 * (0/1))
    assert missing_score == expected_missing_score

    # Test with empty results list (score should be 0.0)
    assert calculate_weighted_score([]) == 0.0


def test_calculate_impact_score():
    """Tests the impact-based scoring model"""
    # Create list of validation results with different rule IDs
    results = [
        {"rule_id": "rule_001", "passed": True},
        {"rule_id": "rule_002", "passed": False},
        {"rule_id": "rule_001", "passed": True},
        {"rule_id": "rule_003", "passed": False},
        {"rule_id": "rule_004", "passed": True}
    ]

    # Define impact factors dictionary mapping rule IDs to impact values
    impact_factors = {
        "rule_001": 2.0,
        "rule_002": 3.0,
        "rule_003": 1.0,
        "rule_004": 4.0
    }

    # Calculate impact score using calculate_impact_score
    score = calculate_impact_score(results, impact_factors)
    # Verify score is calculated correctly based on impact factors
    total_impact = 2.0 + 3.0 + 2.0 + 1.0 + 4.0
    failed_impact = 3.0 + 1.0
    expected_score = 1.0 - (failed_impact / total_impact)
    assert score == expected_score

    # Test with high-impact rule failures (should significantly lower score)
    high_impact_results = [
        {"rule_id": "rule_001", "passed": False},
        {"rule_id": "rule_002", "passed": False}
    ]
    high_impact_score = calculate_impact_score(high_impact_results, impact_factors)
    total_impact_high = 2.0 + 3.0
    failed_impact_high = 2.0 + 3.0
    expected_high_impact_score = 1.0 - (failed_impact_high / total_impact_high)
    assert high_impact_score == expected_high_impact_score

    # Test with low-impact rule failures (should minimally affect score)
    low_impact_results = [
        {"rule_id": "rule_003", "passed": False},
        {"rule_id": "rule_004", "passed": False}
    ]
    low_impact_score = calculate_impact_score(low_impact_results, impact_factors)
    total_impact_low = 1.0 + 4.0
    failed_impact_low = 1.0 + 4.0
    expected_low_impact_score = 1.0 - (failed_impact_low / total_impact_low)
    assert low_impact_score == expected_low_impact_score

    # Test with missing rule ID in impact factors (should use default impact)
    missing_impact_results = [
        {"rule_id": "rule_005", "passed": False}
    ]
    missing_impact_score = calculate_impact_score(missing_impact_results, impact_factors)
    expected_missing_impact_score = 0.0
    assert missing_impact_score == expected_missing_impact_score

    # Test with empty results list (score should be 0.0)
    assert calculate_impact_score([]) == 0.0


def test_normalize_weights():
    """Tests the weight normalization function"""
    # Create weights dictionary with arbitrary values
    weights = {
        "A": 2.0,
        "B": 3.0,
        "C": 5.0
    }

    # Normalize weights using normalize_weights
    normalized_weights = normalize_weights(weights)
    # Verify sum of normalized weights equals 1.0
    assert sum(normalized_weights.values()) == pytest.approx(1.0)
    # Verify relative proportions are maintained
    assert normalized_weights["A"] == pytest.approx(0.2)
    assert normalized_weights["B"] == pytest.approx(0.3)
    assert normalized_weights["C"] == pytest.approx(0.5)

    # Test with weights already summing to 1.0 (should remain unchanged)
    normalized_weights = {
        "A": 0.2,
        "B": 0.3,
        "C": 0.5
    }
    assert normalize_weights(normalized_weights) == normalized_weights

    # Test with single weight (should become 1.0)
    single_weight = {"A": 10.0}
    assert normalize_weights(single_weight) == {"A": 1.0}

    # Test with empty weights dictionary (should return empty dictionary)
    assert normalize_weights({}) == {}

    # Test with all zero weights (should return equal weights)
    zero_weights = {"A": 0.0, "B": 0.0, "C": 0.0}
    equal_weights = normalize_weights(zero_weights)
    assert sum(equal_weights.values()) == pytest.approx(1.0)
    assert equal_weights["A"] == pytest.approx(1/3)
    assert equal_weights["B"] == pytest.approx(1/3)
    assert equal_weights["C"] == pytest.approx(1/3)


def test_quality_scorer_calculate_score_simple():
    """Tests the calculate_score method with SIMPLE model"""
    # Create QualityScorer with SIMPLE model
    scorer = QualityScorer(model=ScoringModel.SIMPLE)
    # Create list of validation results with varying pass/fail statuses
    results = [
        {"passed": True},
        {"passed": False},
        {"passed": True},
        {"passed": False},
        {"passed": True}
    ]

    # Calculate score using scorer.calculate_score
    score = scorer.calculate_score(results)
    # Verify score equals expected simple score
    assert score == 3 / 5

    # Mock calculate_simple_score to verify it's called
    with mock.patch('src.backend.quality.engines.quality_scorer.calculate_simple_score') as mock_calculate_simple_score:
        mock_calculate_simple_score.return_value = 0.7
        score = scorer.calculate_score(results)
        assert score == 0.7
        # Verify calculate_simple_score is called with correct parameters
        mock_calculate_simple_score.assert_called_once_with(results)


def test_quality_scorer_calculate_score_weighted():
    """Tests the calculate_score method with WEIGHTED model"""
    # Create QualityScorer with WEIGHTED model
    scorer = QualityScorer(model=ScoringModel.WEIGHTED)
    # Create list of validation results with different quality dimensions
    results = [
        {"dimension": QualityDimension.COMPLETENESS, "passed": True},
        {"dimension": QualityDimension.ACCURACY, "passed": False},
        {"dimension": QualityDimension.COMPLETENESS, "passed": True},
        {"dimension": QualityDimension.CONSISTENCY, "passed": False},
        {"dimension": QualityDimension.TIMELINESS, "passed": True}
    ]

    # Set custom dimension weights
    weights = {
        QualityDimension.COMPLETENESS: 0.3,
        QualityDimension.ACCURACY: 0.2,
        QualityDimension.CONSISTENCY: 0.2,
        QualityDimension.TIMELINESS: 0.3
    }
    scorer.set_dimension_weights(weights)

    # Calculate score using scorer.calculate_score
    score = scorer.calculate_score(results)
    # Verify score equals expected weighted score
    expected_score = (0.3 * (2/2)) + (0.2 * (0/1)) + (0.2 * (0/1)) + (0.3 * (1/1))
    assert score == expected_score

    # Mock calculate_weighted_score to verify it's called
    with mock.patch('src.backend.quality.engines.quality_scorer.calculate_weighted_score') as mock_calculate_weighted_score:
        mock_calculate_weighted_score.return_value = 0.8
        score = scorer.calculate_score(results)
        assert score == 0.8
        # Verify calculate_weighted_score is called with correct parameters
        mock_calculate_weighted_score.assert_called_once_with(results, weights)


def test_quality_scorer_calculate_score_impact():
    """Tests the calculate_score method with IMPACT model"""
    # Create QualityScorer with IMPACT model
    scorer = QualityScorer(model=ScoringModel.IMPACT)
    # Create list of validation results with different rule IDs
    results = [
        {"rule_id": "rule_001", "passed": True},
        {"rule_id": "rule_002", "passed": False},
        {"rule_id": "rule_001", "passed": True},
        {"rule_id": "rule_003", "passed": False},
        {"rule_id": "rule_004", "passed": True}
    ]

    # Set custom impact factors
    impact_factors = {
        "rule_001": 2.0,
        "rule_002": 3.0,
        "rule_003": 1.0,
        "rule_004": 4.0
    }
    scorer.set_impact_factors(impact_factors)

    # Calculate score using scorer.calculate_score
    score = scorer.calculate_score(results)
    # Verify score equals expected impact score
    total_impact = 2.0 + 3.0 + 2.0 + 1.0 + 4.0
    failed_impact = 3.0 + 1.0
    expected_score = 1.0 - (failed_impact / total_impact)
    assert score == expected_score

    # Mock calculate_impact_score to verify it's called
    with mock.patch('src.backend.quality.engines.quality_scorer.calculate_impact_score') as mock_calculate_impact_score:
        mock_calculate_impact_score.return_value = 0.9
        score = scorer.calculate_score(results)
        assert score == 0.9
        # Verify calculate_impact_score is called with correct parameters
        mock_calculate_impact_score.assert_called_once_with(results, impact_factors)


def test_quality_scorer_calculate_score_adaptive():
    """Tests the calculate_score method with ADAPTIVE model"""
    # Create QualityScorer with ADAPTIVE model
    scorer = QualityScorer(model=ScoringModel.ADAPTIVE)
    # Create list of validation results with various characteristics
    results = [
        {"rule_id": "rule_001", "passed": True, "dimension": QualityDimension.COMPLETENESS},
        {"rule_id": "rule_002", "passed": False, "dimension": QualityDimension.ACCURACY},
    ]

    # Mock _select_adaptive_model to return a specific model
    with mock.patch.object(QualityScorer, '_select_adaptive_model') as mock_select_adaptive_model:
        mock_select_adaptive_model.return_value = ScoringModel.WEIGHTED

        # Calculate score using scorer.calculate_score
        score = scorer.calculate_score(results)

        # Verify _select_adaptive_model is called with correct parameters
        mock_select_adaptive_model.assert_called_once_with(results)

        # Verify appropriate scoring function is called based on selected model
        with mock.patch('src.backend.quality.engines.quality_scorer.calculate_weighted_score') as mock_calculate_weighted_score:
            mock_calculate_weighted_score.return_value = 0.8
            score = scorer.calculate_score(results)
            assert score == 0.8
            mock_calculate_weighted_score.assert_called_once()


def test_quality_scorer_dimension_weights():
    """Tests setting custom dimension weights"""
    # Create QualityScorer with default weights
    scorer = QualityScorer()
    # Define custom dimension weights
    weights = {
        QualityDimension.COMPLETENESS: 0.4,
        QualityDimension.ACCURACY: 0.3,
        QualityDimension.CONSISTENCY: 0.2,
        QualityDimension.TIMELINESS: 0.1
    }

    # Set dimension weights using set_dimension_weights
    scorer.set_dimension_weights(weights)
    # Verify weights are updated correctly
    assert scorer._dimension_weights == weights
    # Verify weights are normalized to sum to 1.0
    assert sum(scorer._dimension_weights.values()) == pytest.approx(1.0)

    # Test that invalid weights (non-QualityDimension keys) raises ValueError
    with pytest.raises(ValueError):
        scorer.set_dimension_weights({"invalid": 0.5, QualityDimension.ACCURACY: 0.5})


def test_quality_scorer_impact_factors():
    """Tests setting custom impact factors"""
    # Create QualityScorer with default impact factors
    scorer = QualityScorer()
    # Define custom impact factors
    impact_factors = {
        "rule_001": 2.0,
        "rule_002": 3.0,
        "rule_003": 1.0
    }

    # Set impact factors using set_impact_factors
    scorer.set_impact_factors(impact_factors)
    # Verify impact factors are updated correctly
    assert scorer._impact_factors == impact_factors

    # Test that invalid impact factors (non-string keys) raises ValueError
    with pytest.raises(ValueError):
        scorer.set_impact_factors({123: 0.5, "rule_002": 0.5})


@pytest.mark.parametrize('model', [ScoringModel.SIMPLE, ScoringModel.WEIGHTED, ScoringModel.IMPACT])
def test_quality_scorer_with_fixtures(sample_validation_results, model):
    """Tests quality scorer with test fixtures"""
    # Create QualityScorer with specified model
    scorer = QualityScorer(model=model)

    # Calculate score using sample_validation_results fixture
    score = scorer.calculate_score(sample_validation_results)
    # Verify score is within expected range (0.0 to 1.0)
    assert 0.0 <= score <= 1.0

    # Verify score calculation doesn't raise exceptions
    try:
        scorer.calculate_score(sample_validation_results)
    except Exception:
        pytest.fail("Score calculation raised an exception")


def test_edge_cases():
    """Tests edge cases for quality scoring"""
    # Test with empty validation results (should return 0.0)
    scorer = QualityScorer()
    assert scorer.calculate_score([]) == 0.0

    # Test with None validation results (should raise TypeError)
    with pytest.raises(TypeError):
        scorer.calculate_score(None)

    # Test with invalid validation result objects (should raise ValueError)
    invalid_results = [1, "test", {"key": "value"}]
    scorer = QualityScorer()
    with pytest.raises(AttributeError):
        scorer.calculate_score(invalid_results)

    # Test with extremely large number of validation results (performance test)
    large_results = [{"passed": True} for _ in range(10000)]
    start_time = time.time()
    scorer.calculate_score(large_results)
    end_time = time.time()
    # Ensure calculation completes within a reasonable time (e.g., < 1 second)
    assert end_time - start_time < 1.0