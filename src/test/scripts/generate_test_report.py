#!/usr/bin/env python3
"""
Test Report Generator

This script generates consolidated test reports by aggregating results from
different test types (unit, integration, performance, and end-to-end).
Supports multiple output formats including HTML, JSON, and JUnit XML.
"""

import argparse
import os
import sys
import json
import glob
import datetime
import logging
from xml.etree import ElementTree
import jinja2
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for server environments
import matplotlib.pyplot as plt
import pandas as pd

from src.test.utils.test_helpers import create_temp_directory, compare_nested_structures

# Global constants
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
TEST_DIR = os.path.join(PROJECT_ROOT, 'src', 'test')
TEMPLATE_DIR = os.path.join(TEST_DIR, 'templates', 'reports')
DEFAULT_OUTPUT_DIR = os.path.join(TEST_DIR, 'reports', 'combined')
LOG_FORMAT = '%(asctime)s [%(levelname)8s] %(message)s (%(filename)s:%(lineno)s)'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def setup_logging(verbose=False):
    """
    Configure logging for the report generator.
    
    Args:
        verbose (bool): Whether to enable verbose (DEBUG) logging.
        
    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger('test_report_generator')
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    
    # Create console handler with formatting
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    logger.addHandler(handler)
    
    return logger


def parse_arguments():
    """
    Parse command-line arguments for the script.
    
    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description='Generate consolidated test reports from multiple test sources.'
    )
    
    parser.add_argument(
        '--unit-dir',
        help='Directory containing unit test results',
        default=os.path.join(TEST_DIR, 'reports', 'unit'),
    )
    
    parser.add_argument(
        '--integration-dir',
        help='Directory containing integration test results',
        default=os.path.join(TEST_DIR, 'reports', 'integration'),
    )
    
    parser.add_argument(
        '--performance-dir',
        help='Directory containing performance test results',
        default=os.path.join(TEST_DIR, 'reports', 'performance'),
    )
    
    parser.add_argument(
        '--e2e-dir',
        help='Directory containing end-to-end test results',
        default=os.path.join(TEST_DIR, 'reports', 'e2e'),
    )
    
    parser.add_argument(
        '--output-dir',
        help='Directory to output the consolidated reports',
        default=DEFAULT_OUTPUT_DIR,
    )
    
    parser.add_argument(
        '--format',
        choices=['html', 'json', 'junit', 'all'],
        default='all',
        help='Output format for the report',
    )
    
    parser.add_argument(
        '--title',
        help='Report title',
        default='Self-Healing Data Pipeline Test Report',
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output',
    )
    
    return parser.parse_args()


def find_test_result_files(args, logger):
    """
    Find all test result files in the specified directories.
    
    Args:
        args (argparse.Namespace): Command-line arguments.
        logger (logging.Logger): Logger instance.
        
    Returns:
        dict: Dictionary of test result files by test type.
    """
    result_files = {
        'unit': {'json': [], 'xml': []},
        'integration': {'json': [], 'xml': []},
        'performance': {'json': [], 'xml': []},
        'e2e': {'json': [], 'xml': []},
    }
    
    # Find unit test result files
    if args.unit_dir and os.path.isdir(args.unit_dir):
        result_files['unit']['json'] = glob.glob(os.path.join(args.unit_dir, '*.json'))
        result_files['unit']['xml'] = glob.glob(os.path.join(args.unit_dir, '*.xml'))
        logger.info(f"Found {len(result_files['unit']['json'])} JSON and {len(result_files['unit']['xml'])} XML unit test result files")
    
    # Find integration test result files
    if args.integration_dir and os.path.isdir(args.integration_dir):
        result_files['integration']['json'] = glob.glob(os.path.join(args.integration_dir, '*.json'))
        result_files['integration']['xml'] = glob.glob(os.path.join(args.integration_dir, '*.xml'))
        logger.info(f"Found {len(result_files['integration']['json'])} JSON and {len(result_files['integration']['xml'])} XML integration test result files")
    
    # Find performance test result files
    if args.performance_dir and os.path.isdir(args.performance_dir):
        result_files['performance']['json'] = glob.glob(os.path.join(args.performance_dir, '*.json'))
        result_files['performance']['xml'] = glob.glob(os.path.join(args.performance_dir, '*.xml'))
        logger.info(f"Found {len(result_files['performance']['json'])} JSON and {len(result_files['performance']['xml'])} XML performance test result files")
    
    # Find end-to-end test result files
    if args.e2e_dir and os.path.isdir(args.e2e_dir):
        result_files['e2e']['json'] = glob.glob(os.path.join(args.e2e_dir, '*.json'))
        result_files['e2e']['xml'] = glob.glob(os.path.join(args.e2e_dir, '*.xml'))
        logger.info(f"Found {len(result_files['e2e']['json'])} JSON and {len(result_files['e2e']['xml'])} XML end-to-end test result files")
    
    return result_files


def parse_junit_xml(file_path, logger):
    """
    Parse JUnit XML test results into a structured format.
    
    Args:
        file_path (str): Path to the JUnit XML file.
        logger (logging.Logger): Logger instance.
        
    Returns:
        dict: Parsed test results in a structured format.
    """
    try:
        tree = ElementTree.parse(file_path)
        root = tree.getroot()
        
        # Handle both single testsuite and multiple testsuites formats
        if root.tag == 'testsuite':
            testsuites = [root]
        else:  # root.tag == 'testsuites' or similar
            testsuites = root.findall('.//testsuite')
        
        results = {
            'name': os.path.basename(file_path),
            'timestamp': datetime.datetime.now().isoformat(),
            'testsuites': [],
            'summary': {
                'total': 0,
                'passed': 0,
                'failed': 0,
                'skipped': 0,
                'time': 0.0,
            }
        }
        
        for testsuite in testsuites:
            suite = {
                'name': testsuite.get('name', 'Unknown'),
                'time': float(testsuite.get('time', 0)),
                'tests': int(testsuite.get('tests', 0)),
                'failures': int(testsuite.get('failures', 0)),
                'errors': int(testsuite.get('errors', 0)),
                'skipped': int(testsuite.get('skipped', 0)),
                'testcases': []
            }
            
            # Process test cases
            for testcase in testsuite.findall('.//testcase'):
                case = {
                    'name': testcase.get('name', 'Unknown'),
                    'classname': testcase.get('classname', 'Unknown'),
                    'time': float(testcase.get('time', 0)),
                    'status': 'passed',
                    'failures': [],
                    'errors': []
                }
                
                # Check for failures
                for failure in testcase.findall('.//failure'):
                    case['status'] = 'failed'
                    case['failures'].append({
                        'message': failure.get('message', ''),
                        'type': failure.get('type', ''),
                        'content': failure.text or ''
                    })
                
                # Check for errors
                for error in testcase.findall('.//error'):
                    case['status'] = 'failed'
                    case['errors'].append({
                        'message': error.get('message', ''),
                        'type': error.get('type', ''),
                        'content': error.text or ''
                    })
                
                # Check for skipped
                if testcase.find('.//skipped') is not None:
                    case['status'] = 'skipped'
                
                suite['testcases'].append(case)
            
            # Update summary
            results['summary']['total'] += suite['tests']
            results['summary']['failed'] += suite['failures'] + suite['errors']
            results['summary']['skipped'] += suite['skipped']
            results['summary']['passed'] += suite['tests'] - suite['failures'] - suite['errors'] - suite['skipped']
            results['summary']['time'] += suite['time']
            
            results['testsuites'].append(suite)
        
        logger.debug(f"Successfully parsed XML file: {file_path}")
        return results
    
    except Exception as e:
        logger.error(f"Error parsing XML file {file_path}: {str(e)}")
        return None


def parse_json_results(file_path, logger):
    """
    Parse JSON test results into a structured format.
    
    Args:
        file_path (str): Path to the JSON file.
        logger (logging.Logger): Logger instance.
        
    Returns:
        dict: Parsed test results in a structured format.
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Validate basic structure
        if not isinstance(data, dict):
            logger.warning(f"Invalid JSON format in {file_path}: root must be an object")
            return None
        
        # Check for different known formats and normalize
        if 'testsuites' in data or 'testsuite' in data:
            # JUnit-like JSON format
            results = {
                'name': data.get('name', os.path.basename(file_path)),
                'timestamp': data.get('timestamp', datetime.datetime.now().isoformat()),
                'testsuites': data.get('testsuites', []) or [data.get('testsuite')] if data.get('testsuite') else [],
                'summary': data.get('summary', {})
            }
            
            # Calculate summary if not provided
            if not results['summary']:
                results['summary'] = {
                    'total': 0,
                    'passed': 0,
                    'failed': 0,
                    'skipped': 0,
                    'time': 0.0,
                }
                
                for suite in results['testsuites']:
                    results['summary']['total'] += suite.get('tests', 0)
                    results['summary']['failed'] += suite.get('failures', 0) + suite.get('errors', 0)
                    results['summary']['skipped'] += suite.get('skipped', 0)
                    results['summary']['time'] += suite.get('time', 0.0)
                
                results['summary']['passed'] = (
                    results['summary']['total'] - 
                    results['summary']['failed'] - 
                    results['summary']['skipped']
                )
        
        elif 'results' in data or 'tests' in data:
            # Custom JSON format - construct a JUnit-like structure
            tests = data.get('results', []) or data.get('tests', [])
            
            passed = sum(1 for t in tests if t.get('status') in ['pass', 'passed'])
            failed = sum(1 for t in tests if t.get('status') in ['fail', 'failed'])
            skipped = sum(1 for t in tests if t.get('status') in ['skip', 'skipped'])
            total = len(tests)
            time = sum(t.get('time', 0.0) for t in tests)
            
            # Convert to JUnit-like structure
            testcases = []
            for test in tests:
                status = test.get('status', '').lower()
                if status in ['pass', 'passed']:
                    status = 'passed'
                elif status in ['fail', 'failed']:
                    status = 'failed'
                elif status in ['skip', 'skipped']:
                    status = 'skipped'
                
                testcase = {
                    'name': test.get('name', 'Unknown'),
                    'classname': test.get('classname', test.get('class', 'Unknown')),
                    'time': test.get('time', 0.0),
                    'status': status,
                    'failures': [],
                    'errors': []
                }
                
                if status == 'failed':
                    if 'failure' in test:
                        testcase['failures'].append({
                            'message': test['failure'].get('message', ''),
                            'type': test['failure'].get('type', ''),
                            'content': test['failure'].get('content', '')
                        })
                    elif 'error' in test:
                        testcase['errors'].append({
                            'message': test['error'].get('message', ''),
                            'type': test['error'].get('type', ''),
                            'content': test['error'].get('content', '')
                        })
                
                testcases.append(testcase)
            
            results = {
                'name': data.get('name', os.path.basename(file_path)),
                'timestamp': data.get('timestamp', datetime.datetime.now().isoformat()),
                'testsuites': [{
                    'name': data.get('name', 'Test Suite'),
                    'time': time,
                    'tests': total,
                    'failures': failed,
                    'errors': 0,
                    'skipped': skipped,
                    'testcases': testcases
                }],
                'summary': {
                    'total': total,
                    'passed': passed,
                    'failed': failed,
                    'skipped': skipped,
                    'time': time,
                }
            }
        
        elif 'stats' in data and 'tests' in data:
            # Pytest JSON format
            total = data.get('stats', {}).get('total', 0)
            passed = data.get('stats', {}).get('passed', 0)
            failed = data.get('stats', {}).get('failed', 0)
            skipped = data.get('stats', {}).get('skipped', 0)
            
            # Convert to JUnit-like structure
            testcases = []
            for test in data.get('tests', []):
                status = test.get('outcome', '').lower()
                if status == 'passed':
                    status = 'passed'
                elif status == 'failed':
                    status = 'failed'
                elif status in ['skipped', 'xfailed', 'xpassed']:
                    status = 'skipped'
                
                testcase = {
                    'name': test.get('name', 'Unknown'),
                    'classname': test.get('nodeid', 'Unknown').split('::')[0],
                    'time': test.get('duration', 0.0),
                    'status': status,
                    'failures': [],
                    'errors': []
                }
                
                if status == 'failed' and 'call' in test:
                    testcase['failures'].append({
                        'message': test['call'].get('crash', {}).get('message', ''),
                        'type': 'failure',
                        'content': str(test['call'].get('crash', {}))
                    })
                
                testcases.append(testcase)
            
            results = {
                'name': data.get('name', os.path.basename(file_path)),
                'timestamp': data.get('created', datetime.datetime.now().isoformat()),
                'testsuites': [{
                    'name': 'Pytest Suite',
                    'time': sum(t.get('duration', 0.0) for t in data.get('tests', [])),
                    'tests': total,
                    'failures': failed,
                    'errors': 0,
                    'skipped': skipped,
                    'testcases': testcases
                }],
                'summary': {
                    'total': total,
                    'passed': passed,
                    'failed': failed,
                    'skipped': skipped,
                    'time': sum(t.get('duration', 0.0) for t in data.get('tests', [])),
                }
            }
        
        else:
            # Unknown format
            logger.warning(f"Unknown JSON format in {file_path}")
            return None
        
        logger.debug(f"Successfully parsed JSON file: {file_path}")
        return results
    
    except Exception as e:
        logger.error(f"Error parsing JSON file {file_path}: {str(e)}")
        return None


class TestResultProcessor:
    """Class for processing and normalizing test results from different formats."""
    
    def __init__(self, logger):
        """
        Initialize the TestResultProcessor.
        
        Args:
            logger (logging.Logger): Logger instance.
        """
        self._result_cache = {}
        self._logger = logger
    
    def process_file(self, file_path, test_type):
        """
        Process a test result file based on its format.
        
        Args:
            file_path (str): Path to the test result file.
            test_type (str): Type of test (unit, integration, performance, e2e).
            
        Returns:
            dict: Processed test results.
        """
        # Check if already processed
        cached_result = self.get_cached_results(file_path)
        if cached_result:
            return cached_result
        
        # Determine file format based on extension
        _, ext = os.path.splitext(file_path)
        ext = ext.lower()
        
        if ext == '.xml':
            results = self.parse_junit_xml(file_path)
        elif ext == '.json':
            results = self.parse_json_result(file_path)
        else:
            self._logger.warning(f"Unsupported file extension: {ext}")
            return None
        
        # Add test type metadata
        if results:
            for testsuite in results.get('testsuites', []):
                testsuite['test_type'] = test_type
        
        # Cache the results
        self._result_cache[file_path] = results
        return results
    
    def parse_junit_xml(self, file_path):
        """
        Parse a JUnit XML test result file.
        
        Args:
            file_path (str): Path to the JUnit XML file.
            
        Returns:
            dict: Parsed test results.
        """
        return parse_junit_xml(file_path, self._logger)
    
    def parse_json_result(self, file_path):
        """
        Parse a JSON test result file.
        
        Args:
            file_path (str): Path to the JSON file.
            
        Returns:
            dict: Parsed test results.
        """
        return parse_json_results(file_path, self._logger)
    
    def normalize_results(self, results, format_type):
        """
        Normalize test results to a common format.
        
        Args:
            results (dict): Test results to normalize.
            format_type (str): Format of the results (json, xml).
            
        Returns:
            dict: Normalized test results.
        """
        # This function would normalize different result formats to a common structure
        # Currently, our parse functions already normalize to a common format
        return results
    
    def get_cached_results(self, file_path):
        """
        Get cached results for a file if available.
        
        Args:
            file_path (str): Path to the test result file.
            
        Returns:
            dict: Cached results or None if not cached.
        """
        return self._result_cache.get(file_path)


def aggregate_test_results(result_files, logger):
    """
    Aggregate test results from multiple files into a unified structure.
    
    Args:
        result_files (dict): Dictionary of test result files by test type.
        logger (logging.Logger): Logger instance.
        
    Returns:
        dict: Aggregated test results.
    """
    processor = TestResultProcessor(logger)
    aggregated_results = {
        'timestamp': datetime.datetime.now().isoformat(),
        'test_types': {
            'unit': {
                'summary': {'total': 0, 'passed': 0, 'failed': 0, 'skipped': 0, 'time': 0.0},
                'testsuites': []
            },
            'integration': {
                'summary': {'total': 0, 'passed': 0, 'failed': 0, 'skipped': 0, 'time': 0.0},
                'testsuites': []
            },
            'performance': {
                'summary': {'total': 0, 'passed': 0, 'failed': 0, 'skipped': 0, 'time': 0.0},
                'testsuites': []
            },
            'e2e': {
                'summary': {'total': 0, 'passed': 0, 'failed': 0, 'skipped': 0, 'time': 0.0},
                'testsuites': []
            }
        },
        'summary': {'total': 0, 'passed': 0, 'failed': 0, 'skipped': 0, 'time': 0.0}
    }
    
    # Process all files by test type
    for test_type, formats in result_files.items():
        # Process JSON files
        for file_path in formats['json']:
            results = processor.process_file(file_path, test_type)
            if results:
                # Add to test type results
                aggregated_results['test_types'][test_type]['testsuites'].extend(results['testsuites'])
                
                # Update test type summary
                for key in ['total', 'passed', 'failed', 'skipped', 'time']:
                    aggregated_results['test_types'][test_type]['summary'][key] += results['summary'][key]
        
        # Process XML files
        for file_path in formats['xml']:
            results = processor.process_file(file_path, test_type)
            if results:
                # Add to test type results
                aggregated_results['test_types'][test_type]['testsuites'].extend(results['testsuites'])
                
                # Update test type summary
                for key in ['total', 'passed', 'failed', 'skipped', 'time']:
                    aggregated_results['test_types'][test_type]['summary'][key] += results['summary'][key]
    
    # Calculate overall summary
    for test_type, data in aggregated_results['test_types'].items():
        for key in ['total', 'passed', 'failed', 'skipped', 'time']:
            aggregated_results['summary'][key] += data['summary'][key]
    
    logger.info(f"Aggregated results: {aggregated_results['summary']['total']} tests, "
                f"{aggregated_results['summary']['passed']} passed, "
                f"{aggregated_results['summary']['failed']} failed, "
                f"{aggregated_results['summary']['skipped']} skipped")
    
    return aggregated_results


class ReportGenerator:
    """Class for generating test reports in different formats."""
    
    def __init__(self, aggregated_results, output_dir, report_title, logger):
        """
        Initialize the ReportGenerator.
        
        Args:
            aggregated_results (dict): Aggregated test results.
            output_dir (str): Output directory for the reports.
            report_title (str): Title for the reports.
            logger (logging.Logger): Logger instance.
        """
        self._aggregated_results = aggregated_results
        self._output_dir = output_dir
        self._report_title = report_title
        self._logger = logger
        
        # Initialize Jinja2 environment
        self._jinja_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(TEMPLATE_DIR),
            autoescape=jinja2.select_autoescape(['html']),
            trim_blocks=True,
            lstrip_blocks=True
        )
    
    def generate_html(self):
        """
        Generate an HTML report.
        
        Returns:
            str: Path to the generated HTML report.
        """
        try:
            # Load the template
            template = self._jinja_env.get_template('report_template.html')
            
            # Generate charts
            chart_paths = self.generate_charts()
            
            # Prepare template context
            context = {
                'title': self._report_title,
                'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'results': self._aggregated_results,
                'chart_paths': chart_paths,
            }
            
            # Render the template
            html_content = template.render(**context)
            
            # Write to output file
            output_path = os.path.join(self._output_dir, 'test_report.html')
            with open(output_path, 'w') as f:
                f.write(html_content)
            
            # Copy assets (CSS, JS)
            self.copy_assets()
            
            self._logger.info(f"Generated HTML report: {output_path}")
            return output_path
        
        except Exception as e:
            self._logger.error(f"Error generating HTML report: {str(e)}")
            return None
    
    def generate_json(self):
        """
        Generate a JSON report.
        
        Returns:
            str: Path to the generated JSON report.
        """
        try:
            # Add timestamp to results
            report_data = {
                'timestamp': datetime.datetime.now().isoformat(),
                'results': self._aggregated_results,
            }
            
            # Write to output file
            output_path = os.path.join(self._output_dir, 'test_report.json')
            with open(output_path, 'w') as f:
                json.dump(report_data, f, indent=2)
            
            self._logger.info(f"Generated JSON report: {output_path}")
            return output_path
        
        except Exception as e:
            self._logger.error(f"Error generating JSON report: {str(e)}")
            return None
    
    def generate_junit(self):
        """
        Generate a JUnit XML report.
        
        Returns:
            str: Path to the generated JUnit XML report.
        """
        try:
            # Create root element
            root = ElementTree.Element('testsuites')
            
            # Add attributes to root
            root.set('name', self._report_title or 'Combined Test Report')
            root.set('time', str(self._aggregated_results['summary']['time']))
            root.set('tests', str(self._aggregated_results['summary']['total']))
            root.set('failures', str(self._aggregated_results['summary']['failed']))
            root.set('errors', '0')  # We don't track errors separately
            
            # Add test suites for each test type
            for test_type, data in self._aggregated_results['test_types'].items():
                # Create test suite element for each test type
                testsuite_elem = ElementTree.SubElement(root, 'testsuite')
                testsuite_elem.set('name', f"{test_type.capitalize()} Tests")
                testsuite_elem.set('time', str(data['summary']['time']))
                testsuite_elem.set('tests', str(data['summary']['total']))
                testsuite_elem.set('failures', str(data['summary']['failed']))
                testsuite_elem.set('errors', '0')  # We don't track errors separately
                testsuite_elem.set('skipped', str(data['summary']['skipped']))
                
                # Add test cases
                for suite in data['testsuites']:
                    for case in suite.get('testcases', []):
                        testcase_elem = ElementTree.SubElement(testsuite_elem, 'testcase')
                        testcase_elem.set('name', case['name'])
                        testcase_elem.set('classname', case.get('classname', 'Unknown'))
                        testcase_elem.set('time', str(case.get('time', 0)))
                        
                        # Add failure elements
                        if case.get('status') == 'failed':
                            if case.get('failures'):
                                for failure in case['failures']:
                                    failure_elem = ElementTree.SubElement(testcase_elem, 'failure')
                                    failure_elem.set('message', failure.get('message', ''))
                                    failure_elem.set('type', failure.get('type', ''))
                                    failure_elem.text = failure.get('content', '')
                            
                            elif case.get('errors'):
                                for error in case['errors']:
                                    error_elem = ElementTree.SubElement(testcase_elem, 'error')
                                    error_elem.set('message', error.get('message', ''))
                                    error_elem.set('type', error.get('type', ''))
                                    error_elem.text = error.get('content', '')
                            
                            else:
                                # Generic failure if no details provided
                                failure_elem = ElementTree.SubElement(testcase_elem, 'failure')
                                failure_elem.set('message', 'Test failed')
                                failure_elem.set('type', 'failure')
                        
                        # Add skipped element
                        elif case.get('status') == 'skipped':
                            skipped_elem = ElementTree.SubElement(testcase_elem, 'skipped')
            
            # Write to output file
            output_path = os.path.join(self._output_dir, 'test_report.xml')
            tree = ElementTree.ElementTree(root)
            tree.write(output_path, encoding='utf-8', xml_declaration=True)
            
            self._logger.info(f"Generated JUnit XML report: {output_path}")
            return output_path
        
        except Exception as e:
            self._logger.error(f"Error generating JUnit XML report: {str(e)}")
            return None
    
    def generate_charts(self):
        """
        Generate charts and visualizations for the HTML report.
        
        Returns:
            dict: Dictionary of chart file paths by chart type.
        """
        charts_dir = os.path.join(self._output_dir, 'charts')
        os.makedirs(charts_dir, exist_ok=True)
        
        chart_paths = {}
        
        try:
            # Define colors
            colors = {
                'passed': '#4CAF50',  # Green
                'failed': '#F44336',  # Red
                'skipped': '#FFC107'   # Amber
            }
            
            # 1. Overall Pie Chart
            plt.figure(figsize=(8, 8))
            summary = self._aggregated_results['summary']
            labels = ['Passed', 'Failed', 'Skipped']
            sizes = [summary['passed'], summary['failed'], summary['skipped']]
            
            # Don't plot if there's no data
            if sum(sizes) > 0:
                plt.pie(
                    sizes, 
                    labels=labels, 
                    colors=[colors['passed'], colors['failed'], colors['skipped']],
                    autopct='%1.1f%%', 
                    startangle=90,
                    explode=(0.05, 0.1, 0.05)
                )
                plt.axis('equal')
                plt.title('Overall Test Results')
                
                pie_chart_path = os.path.join(charts_dir, 'overall_pie.png')
                plt.savefig(pie_chart_path)
                plt.close()
                chart_paths['overall_pie'] = os.path.join('charts', 'overall_pie.png')
            
            # 2. Test Type Bar Chart
            plt.figure(figsize=(10, 6))
            test_types = list(self._aggregated_results['test_types'].keys())
            passed_values = [self._aggregated_results['test_types'][t]['summary']['passed'] for t in test_types]
            failed_values = [self._aggregated_results['test_types'][t]['summary']['failed'] for t in test_types]
            skipped_values = [self._aggregated_results['test_types'][t]['summary']['skipped'] for t in test_types]
            
            bar_width = 0.25
            r1 = range(len(test_types))
            r2 = [x + bar_width for x in r1]
            r3 = [x + bar_width for x in r2]
            
            plt.bar(r1, passed_values, color=colors['passed'], width=bar_width, label='Passed')
            plt.bar(r2, failed_values, color=colors['failed'], width=bar_width, label='Failed')
            plt.bar(r3, skipped_values, color=colors['skipped'], width=bar_width, label='Skipped')
            
            plt.xlabel('Test Type')
            plt.ylabel('Count')
            plt.title('Test Results by Type')
            plt.xticks([r + bar_width for r in range(len(test_types))], [t.capitalize() for t in test_types])
            plt.legend()
            
            type_bar_path = os.path.join(charts_dir, 'test_type_bar.png')
            plt.savefig(type_bar_path)
            plt.close()
            chart_paths['test_type_bar'] = os.path.join('charts', 'test_type_bar.png')
            
            # 3. Duration Line Chart
            plt.figure(figsize=(10, 6))
            test_types = list(self._aggregated_results['test_types'].keys())
            durations = [self._aggregated_results['test_types'][t]['summary']['time'] for t in test_types]
            
            plt.plot(test_types, durations, marker='o', linestyle='-', linewidth=2)
            plt.fill_between(test_types, durations, alpha=0.3)
            
            plt.xlabel('Test Type')
            plt.ylabel('Duration (seconds)')
            plt.title('Test Duration by Type')
            plt.xticks(range(len(test_types)), [t.capitalize() for t in test_types])
            plt.grid(True, alpha=0.3)
            
            duration_line_path = os.path.join(charts_dir, 'duration_line.png')
            plt.savefig(duration_line_path)
            plt.close()
            chart_paths['duration_line'] = os.path.join('charts', 'duration_line.png')
            
            # 4. Stacked Test Count Bar Chart
            plt.figure(figsize=(12, 6))
            test_types = list(self._aggregated_results['test_types'].keys())
            test_counts = [self._aggregated_results['test_types'][t]['summary']['total'] for t in test_types]
            
            plt.bar(test_types, test_counts)
            plt.xlabel('Test Type')
            plt.ylabel('Test Count')
            plt.title('Number of Tests by Type')
            plt.xticks(range(len(test_types)), [t.capitalize() for t in test_types])
            
            for i, v in enumerate(test_counts):
                plt.text(i, v + 0.5, str(v), ha='center')
            
            count_bar_path = os.path.join(charts_dir, 'test_count_bar.png')
            plt.savefig(count_bar_path)
            plt.close()
            chart_paths['test_count_bar'] = os.path.join('charts', 'test_count_bar.png')
            
            return chart_paths
        
        except Exception as e:
            self._logger.error(f"Error generating charts: {str(e)}")
            return {}
    
    def copy_assets(self):
        """
        Copy CSS and JavaScript assets to the output directory.
        
        Returns:
            bool: True if assets were copied successfully.
        """
        try:
            # Create assets directory
            assets_dir = os.path.join(self._output_dir, 'assets')
            os.makedirs(assets_dir, exist_ok=True)
            
            # Copy assets from template directory
            template_assets_dir = os.path.join(TEMPLATE_DIR, 'assets')
            if os.path.exists(template_assets_dir):
                for asset_file in os.listdir(template_assets_dir):
                    src_path = os.path.join(template_assets_dir, asset_file)
                    dst_path = os.path.join(assets_dir, asset_file)
                    
                    if os.path.isfile(src_path):
                        shutil.copy2(src_path, dst_path)
                        self._logger.debug(f"Copied asset: {asset_file}")
            
            return True
        
        except Exception as e:
            self._logger.error(f"Error copying assets: {str(e)}")
            return False


def create_output_directory(output_dir, logger):
    """
    Create the output directory if it doesn't exist.
    
    Args:
        output_dir (str): Output directory path.
        logger (logging.Logger): Logger instance.
        
    Returns:
        bool: True if directory was created or already exists.
    """
    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
            logger.info(f"Created output directory: {output_dir}")
        except Exception as e:
            logger.error(f"Failed to create output directory {output_dir}: {str(e)}")
            return False
    else:
        logger.info(f"Output directory already exists: {output_dir}")
    
    return True


def main():
    """
    Main function that orchestrates the report generation process.
    
    Returns:
        int: Exit code (0 for success, non-zero for failure).
    """
    args = parse_arguments()
    logger = setup_logging(args.verbose)
    
    logger.info(f"Starting test report generation with format: {args.format}")
    
    # Find test result files
    result_files = find_test_result_files(args, logger)
    
    # Check if we found any files
    total_files = sum(len(files) for format_files in result_files.values() 
                     for files in format_files.values())
    
    if total_files == 0:
        logger.warning("No test result files found. Nothing to do.")
        return 0
    
    # Aggregate test results
    aggregated_results = aggregate_test_results(result_files, logger)
    
    # Create output directory
    if not create_output_directory(args.output_dir, logger):
        return 1
    
    # Generate reports in requested format
    generated_reports = []
    
    if args.format in ['html', 'all']:
        html_path = generate_html_report(aggregated_results, args.output_dir, args.title, logger)
        if html_path:
            generated_reports.append(f"HTML report: {html_path}")
    
    if args.format in ['json', 'all']:
        json_path = generate_json_report(aggregated_results, args.output_dir, logger)
        if json_path:
            generated_reports.append(f"JSON report: {json_path}")
    
    if args.format in ['junit', 'all']:
        junit_path = generate_junit_report(aggregated_results, args.output_dir, logger)
        if junit_path:
            generated_reports.append(f"JUnit XML report: {junit_path}")
    
    # Print summary
    if generated_reports:
        logger.info("Generated reports:")
        for report in generated_reports:
            logger.info(f"  - {report}")
    else:
        logger.warning("No reports were generated.")
    
    return 0


def generate_html_report(aggregated_results, output_dir, report_title, logger):
    """
    Generate an HTML report from the aggregated test results.
    
    Args:
        aggregated_results (dict): Aggregated test results.
        output_dir (str): Output directory for the report.
        report_title (str): Title for the report.
        logger (logging.Logger): Logger instance.
        
    Returns:
        str: Path to the generated HTML report.
    """
    report_generator = ReportGenerator(aggregated_results, output_dir, report_title, logger)
    return report_generator.generate_html()


def generate_json_report(aggregated_results, output_dir, logger):
    """
    Generate a JSON report from the aggregated test results.
    
    Args:
        aggregated_results (dict): Aggregated test results.
        output_dir (str): Output directory for the report.
        logger (logging.Logger): Logger instance.
        
    Returns:
        str: Path to the generated JSON report.
    """
    report_generator = ReportGenerator(aggregated_results, output_dir, "", logger)
    return report_generator.generate_json()


def generate_junit_report(aggregated_results, output_dir, logger):
    """
    Generate a JUnit XML report from the aggregated test results.
    
    Args:
        aggregated_results (dict): Aggregated test results.
        output_dir (str): Output directory for the report.
        logger (logging.Logger): Logger instance.
        
    Returns:
        str: Path to the generated JUnit XML report.
    """
    report_generator = ReportGenerator(aggregated_results, output_dir, "", logger)
    return report_generator.generate_junit()


if __name__ == "__main__":
    sys.exit(main())