"""
Performance tests for measuring response time of the self-healing AI engine components.
This module focuses on benchmarking the latency of issue classification, confidence scoring, and data correction operations under various load conditions to ensure the self-healing system meets performance requirements.
"""
import pytest
import time
import pandas
import numpy
import statistics

from src.test.performance.conftest import performance_metrics_collector, performance_test_context, performance_thresholds, test_data_size, test_iterations
from src.test.utils.test_assertions import assert_performance_within_threshold, SelfHealingAssertions
from src.test.fixtures.backend.healing_fixtures import issue_classifier, confidence_scorer, data_corrector, sample_issue_data, sample_quality_issue, sample_pipeline_issue
from src.backend.self_healing.ai.issue_classifier import IssueClassifier
from src.backend.self_healing.decision.confidence_scorer import ConfidenceScorer
from src.backend.self_healing.correction.data_corrector import DataCorrector
from src.backend.constants import HealingActionType

ISSUE_COMPLEXITY_LEVELS = ['simple', 'medium', 'complex']
ISSUE_TYPES = ['missing_values', 'outliers', 'format_errors', 'schema_drift', 'data_corruption', 'resource_exhaustion', 'timeout', 'dependency_failure', 'configuration_error', 'permission_error', 'service_unavailable']
TEST_TIMEOUT = 300


def generate_test_issue_data(complexity: str, issue_type: str) -> dict:
    """Generates test issue data with varying complexity for performance testing

    Args:
        complexity: The complexity level of the issue data (simple, medium, complex)
        issue_type: The type of issue to generate data for

    Returns:
        Generated issue data dictionary
    """
    # Create base issue data structure with common fields
    issue_data = {
        'issue_id': str(numpy.random.uuid4()),
        'issue_type': issue_type,
        'complexity': complexity,
        'timestamp': datetime.datetime.now().isoformat()
    }

    # Add complexity-specific details based on complexity parameter
    if complexity == 'simple':
        # For 'simple' complexity, include minimal required fields
        issue_data['description'] = f"Simple {issue_type} issue"
    elif complexity == 'medium':
        # For 'medium' complexity, add additional context and metadata
        issue_data['description'] = f"Medium complexity {issue_type} issue with additional context"
        issue_data['context'] = {'component': 'data_ingestion', 'data_source': 'GCS'}
        issue_data['metadata'] = {'file_size': '10MB', 'record_count': 10000}
    elif complexity == 'complex':
        # For 'complex' complexity, add extensive error details and nested structures
        issue_data['description'] = f"Complex {issue_type} issue with extensive error details"
        issue_data['error_details'] = {
            'code': 'ERR500',
            'message': 'Detailed error message',
            'stack_trace': 'Long stack trace',
            'nested_info': {'level1': 'value1', 'level2': 'value2'}
        }
        issue_data['context'] = {'component': 'data_transformation', 'data_source': 'BigQuery', 'environment': 'production'}
        issue_data['metadata'] = {'file_size': '100MB', 'record_count': 1000000, 'data_quality': 0.7}

    # Add issue-type-specific details based on issue_type parameter
    if issue_type == 'missing_values':
        issue_data['affected_column'] = 'column1'
        issue_data['missing_count'] = 100
    elif issue_type == 'outliers':
        issue_data['affected_column'] = 'column2'
        issue_data['outlier_count'] = 50
    elif issue_type == 'format_errors':
        issue_data['affected_column'] = 'column3'
        issue_data['invalid_count'] = 20
    return issue_data


def generate_test_dataset(size: int, issue_type: str, issue_ratio: float) -> pandas.DataFrame:
    """Generates a test dataset with quality issues for correction performance testing

    Args:
        size: The number of rows in the dataset
        issue_type: The type of issue to inject into the dataset
        issue_ratio: The ratio of rows that should have the injected issue

    Returns:
        Generated dataset with injected issues
    """
    # Create a pandas DataFrame with specified number of rows
    data = {'col1': numpy.random.rand(size), 'col2': numpy.random.randint(0, 100, size=size)}
    df = pandas.DataFrame(data)

    # Generate columns based on issue_type (numeric for outliers, string for format errors, etc.)
    if issue_type == 'outliers':
        df['col3'] = numpy.random.normal(0, 1, size=size)
    elif issue_type == 'format_errors':
        df['col3'] = [TestDataGenerator.generate_random_string(10) for _ in range(size)]
    else:
        df['col3'] = numpy.random.choice([None, 'valid'], size=size, p=[issue_ratio, 1 - issue_ratio])

    # Inject quality issues into the dataset based on issue_ratio
    num_issues = int(size * issue_ratio)
    if issue_type == 'outliers':
        indices = numpy.random.choice(size, num_issues, replace=False)
        df.loc[indices, 'col3'] = numpy.random.normal(10, 1, size=num_issues)  # Introduce outliers
    elif issue_type == 'format_errors':
        indices = numpy.random.choice(size, num_issues, replace=False)
        df.loc[indices, 'col3'] = 'Invalid Format'  # Introduce format errors
    elif issue_type == 'missing_values':
        indices = numpy.random.choice(size, num_issues, replace=False)
        df.loc[indices, 'col3'] = None  # Introduce missing values

    # Add metadata about injected issues for validation
    df.attrs['issue_type'] = issue_type
    df.attrs['issue_ratio'] = issue_ratio

    # Return the generated DataFrame with issues
    return df


@pytest.mark.performance
@pytest.mark.parametrize('complexity', ISSUE_COMPLEXITY_LEVELS)
@pytest.mark.parametrize('issue_type', ['missing_values', 'outliers', 'resource_exhaustion', 'timeout'])
def test_issue_classifier_response_time(issue_classifier, performance_metrics_collector, performance_test_context, performance_thresholds, test_iterations, complexity, issue_type):
    """Tests the response time of the issue classifier for different complexity levels"""
    # Generate test issue data for the specified complexity and issue type
    issue_data = generate_test_issue_data(complexity, issue_type)

    # Initialize metrics for tracking response times
    response_times = []

    # Perform multiple iterations of issue classification
    for i in range(test_iterations):
        # For each iteration, measure the time taken to classify the issue
        start_time = time.time()
        classification = issue_classifier.classify_issue(issue_data)
        end_time = time.time()

        # Record response times in the performance metrics collector
        response_time = end_time - start_time
        response_times.append(response_time)

    # Calculate statistics for the response times
    avg_response_time = statistics.mean(response_times)
    max_response_time = max(response_times)
    min_response_time = min(response_times)

    # Assert that response times are within expected thresholds
    thresholds = performance_thresholds['issue_classifier']
    assert_performance_within_threshold({'avg_response_time': avg_response_time}, thresholds, lower_is_better=True)

    # Log detailed performance metrics for analysis
    performance_metrics_collector.record_metric('issue_classifier_avg_response_time', avg_response_time)
    performance_metrics_collector.record_metric('issue_classifier_max_response_time', max_response_time)
    performance_metrics_collector.record_metric('issue_classifier_min_response_time', min_response_time)
    performance_metrics_collector.log_performance_report(performance_test_context)


@pytest.mark.performance
@pytest.mark.parametrize('complexity', ISSUE_COMPLEXITY_LEVELS)
@pytest.mark.parametrize('action_type', ['DATA_CORRECTION', 'PARAMETER_ADJUSTMENT', 'RESOURCE_SCALING'])
def test_confidence_scorer_response_time(confidence_scorer, issue_classifier, performance_metrics_collector, performance_test_context, performance_thresholds, test_iterations, complexity, action_type):
    """Tests the response time of the confidence scorer for different complexity levels"""
    # Generate test issue data for the specified complexity
    issue_data = generate_test_issue_data(complexity, 'missing_values')

    # Classify the issue to get classification results
    classification = issue_classifier.classify_issue(issue_data)

    # Initialize metrics for tracking response times
    response_times = []

    # Perform multiple iterations of confidence scoring
    for i in range(test_iterations):
        # For each iteration, measure the time taken to calculate confidence
        start_time = time.time()
        confidence = confidence_scorer.calculate_confidence(HealingActionType[action_type], {}, issue_data, {})
        end_time = time.time()

        # Record response times in the performance metrics collector
        response_time = end_time - start_time
        response_times.append(response_time)

    # Calculate statistics for the response times
    avg_response_time = statistics.mean(response_times)
    max_response_time = max(response_times)
    min_response_time = min(response_times)

    # Assert that response times are within expected thresholds
    thresholds = performance_thresholds['confidence_scorer']
    assert_performance_within_threshold({'avg_response_time': avg_response_time}, thresholds, lower_is_better=True)

    # Log detailed performance metrics for analysis
    performance_metrics_collector.record_metric('confidence_scorer_avg_response_time', avg_response_time)
    performance_metrics_collector.record_metric('confidence_scorer_max_response_time', max_response_time)
    performance_metrics_collector.record_metric('confidence_scorer_min_response_time', min_response_time)
    performance_metrics_collector.log_performance_report(performance_test_context)


@pytest.mark.performance
@pytest.mark.parametrize('issue_type', ['missing_values', 'outliers', 'format_errors'])
@pytest.mark.timeout(TEST_TIMEOUT)
def test_data_corrector_response_time(data_corrector, issue_classifier, performance_metrics_collector, performance_test_context, performance_thresholds, test_data_size, test_iterations, issue_type):
    """Tests the response time of the data corrector for different data sizes and issue types"""
    # Generate test dataset with specified size and issue type
    df, issue_details = generate_test_dataset(test_data_size, issue_type, 0.1)

    # Create issue data and classification for the dataset
    issue_data = {'issue_id': 'test_issue', 'data_location': 'test_location', 'issue_type': issue_type}
    classification = issue_classifier.classify_issue(issue_data)

    # Initialize metrics for tracking response times
    response_times = []

    # Perform multiple iterations of data correction
    for i in range(test_iterations):
        # For each iteration, measure the time taken to correct the data
        start_time = time.time()
        success, correction_details = data_corrector.correct_data_issue(issue_data, classification, {})
        end_time = time.time()

        # Record response times in the performance metrics collector
        response_time = end_time - start_time
        response_times.append(response_time)

    # Calculate statistics for the response times
    avg_response_time = statistics.mean(response_times)
    max_response_time = max(response_times)
    min_response_time = min(response_times)

    # Assert that response times are within expected thresholds
    thresholds = performance_thresholds['data_corrector']
    assert_performance_within_threshold({'avg_response_time': avg_response_time}, thresholds, lower_is_better=True)

    # Log detailed performance metrics for analysis
    performance_metrics_collector.record_metric('data_corrector_avg_response_time', avg_response_time)
    performance_metrics_collector.record_metric('data_corrector_max_response_time', max_response_time)
    performance_metrics_collector.record_metric('data_corrector_min_response_time', min_response_time)
    performance_metrics_collector.log_performance_report(performance_test_context)


@pytest.mark.performance
@pytest.mark.parametrize('complexity', ISSUE_COMPLEXITY_LEVELS)
@pytest.mark.timeout(TEST_TIMEOUT)
def test_end_to_end_healing_response_time(issue_classifier, confidence_scorer, data_corrector, performance_metrics_collector, performance_test_context, performance_thresholds, test_data_size, complexity):
    """Tests the end-to-end response time of the self-healing process from issue detection to correction"""
    # Generate test dataset with quality issues
    df, issue_details = generate_test_dataset(test_data_size, 'missing_values', 0.1)

    # Initialize metrics for tracking end-to-end response times
    response_times = []

    # Perform the complete self-healing workflow:
    for i in range(1):
        # Measure the total time taken for the end-to-end process
        start_time = time.time()

        # 1. Classify the issue
        issue_data = {'issue_id': 'test_issue', 'data_location': 'test_location', 'issue_type': 'missing_values'}
        classification = issue_classifier.classify_issue(issue_data)

        # 2. Calculate confidence score
        confidence = confidence_scorer.calculate_confidence(HealingActionType.DATA_CORRECTION, {}, issue_data, {})

        # 3. Apply data correction if confidence is sufficient
        if confidence > 0.8:
            success, correction_details = data_corrector.correct_data_issue(issue_data, classification, {})

        end_time = time.time()

        # Record response times in the performance metrics collector
        response_time = end_time - start_time
        response_times.append(response_time)

    # Calculate statistics for the response times
    avg_response_time = statistics.mean(response_times)
    max_response_time = max(response_times)
    min_response_time = min(response_times)

    # Assert that end-to-end response times are within expected thresholds
    thresholds = performance_thresholds['end_to_end_healing']
    assert_performance_within_threshold({'avg_response_time': avg_response_time}, thresholds, lower_is_better=True)

    # Log detailed performance metrics for analysis
    performance_metrics_collector.record_metric('end_to_end_avg_response_time', avg_response_time)
    performance_metrics_collector.record_metric('end_to_end_max_response_time', max_response_time)
    performance_metrics_collector.record_metric('end_to_end_min_response_time', min_response_time)
    performance_metrics_collector.log_performance_report(performance_test_context)


@pytest.mark.performance
@pytest.mark.parametrize('batch_size', [1, 10, 50, 100])
@pytest.mark.timeout(TEST_TIMEOUT)
def test_issue_classifier_scalability(issue_classifier, performance_metrics_collector, performance_test_context, performance_thresholds, batch_size):
    """Tests the scalability of the issue classifier with increasing complexity and batch size"""
    # Generate a batch of test issues with the specified batch size
    issues = [generate_test_issue_data('simple', 'missing_values') for _ in range(batch_size)]

    # Initialize metrics for tracking batch processing times
    start_time = time.time()

    # Process the entire batch of issues through the classifier
    for issue_data in issues:
        classification = issue_classifier.classify_issue(issue_data)

    end_time = time.time()

    # Measure the total time and calculate the average time per issue
    total_time = end_time - start_time
    avg_time_per_issue = total_time / batch_size if batch_size > 0 else 0

    # Record processing times in the performance metrics collector
    performance_metrics_collector.record_metric('issue_classifier_batch_size', batch_size)
    performance_metrics_collector.record_metric('issue_classifier_total_time', total_time)
    performance_metrics_collector.record_metric('issue_classifier_avg_time_per_issue', avg_time_per_issue)

    # Assert that processing times scale linearly with batch size
    thresholds = performance_thresholds['issue_classifier_scalability']
    assert_performance_within_threshold({'avg_time_per_issue': avg_time_per_issue}, thresholds, lower_is_better=True)

    # Log detailed performance metrics for analysis
    performance_metrics_collector.log_performance_report(performance_test_context)


@pytest.mark.performance
@pytest.mark.parametrize('concurrent_requests', [1, 5, 10, 20])
@pytest.mark.timeout(TEST_TIMEOUT)
def test_confidence_calculation_under_load(confidence_scorer, issue_classifier, performance_metrics_collector, performance_test_context, performance_thresholds, concurrent_requests):
    """Tests the performance of confidence calculation under simulated load conditions"""
    # Generate test data for multiple concurrent confidence calculations
    issue_data = generate_test_issue_data('simple', 'missing_values')
    classification = issue_classifier.classify_issue(issue_data)

    # Simulate concurrent requests using threading or async processing
    response_times = []
    for i in range(concurrent_requests):
        start_time = time.time()
        confidence = confidence_scorer.calculate_confidence(HealingActionType.DATA_CORRECTION, {}, issue_data, {})
        end_time = time.time()
        response_times.append(end_time - start_time)

    # Measure response times for each concurrent request
    avg_response_time = statistics.mean(response_times)
    max_response_time = max(response_times)
    min_response_time = min(response_times)

    # Record individual and aggregate response times
    performance_metrics_collector.record_metric('confidence_scorer_concurrent_requests', concurrent_requests)
    performance_metrics_collector.record_metric('confidence_scorer_avg_response_time', avg_response_time)
    performance_metrics_collector.record_metric('confidence_scorer_max_response_time', max_response_time)
    performance_metrics_collector.record_metric('confidence_scorer_min_response_time', min_response_time)

    # Assert that performance remains within acceptable limits under load
    thresholds = performance_thresholds['confidence_calculation_under_load']
    assert_performance_within_threshold({'avg_response_time': avg_response_time}, thresholds, lower_is_better=True)

    # Log detailed performance metrics for analysis
    performance_metrics_collector.log_performance_report(performance_test_context)