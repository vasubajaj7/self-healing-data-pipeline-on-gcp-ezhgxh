#!/usr/bin/env python3
"""
CI Test Runner Script

This script orchestrates the execution of all test types (unit, integration, 
performance, and end-to-end) in CI/CD environments. It provides a unified 
interface for running tests with appropriate configurations for CI environments, 
handling test dependencies, and generating consolidated reports.
"""

import argparse
import os
import sys
import subprocess
import logging
import datetime
import shutil
from pathlib import Path

from src.test.utils.test_helpers import create_temp_directory, cleanup_temp_resources

# Directory and file path constants
SCRIPT_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent
TEST_DIR = PROJECT_ROOT / 'src' / 'test'
REPORT_DIR = TEST_DIR / 'reports'
COMBINED_REPORT_DIR = REPORT_DIR / 'combined'
LOG_DIR = TEST_DIR / 'logs'
LOG_FILE = LOG_DIR / f'ci_tests_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log'


def setup_logging(verbose: bool) -> logging.Logger:
    """
    Configure logging for the CI test runner.
    
    Args:
        verbose: Whether to enable verbose logging
        
    Returns:
        Configured logger instance
    """
    # Create logger instance
    logger = logging.getLogger('ci_test_runner')
    
    # Set log level based on verbose flag
    log_level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(log_level)
    
    # Create log directory if it doesn't exist
    os.makedirs(LOG_DIR, exist_ok=True)
    
    # Configure file handler with appropriate formatting
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setLevel(log_level)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    
    # Configure console handler with appropriate formatting
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments for the script.
    
    Returns:
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description='Run tests for the self-healing data pipeline in CI/CD environments'
    )
    
    # Add argument for test types to run
    parser.add_argument(
        '--test-types',
        choices=['unit', 'integration', 'performance', 'e2e', 'all'],
        default='all',
        help='Types of tests to run (default: all)'
    )
    
    # Add argument for test environment
    parser.add_argument(
        '--environment',
        choices=['local', 'dev', 'staging', 'prod'],
        default='local',
        help='Target environment for tests (default: local)'
    )
    
    # Add argument for coverage reporting
    parser.add_argument(
        '--coverage',
        action='store_true',
        help='Generate coverage reports'
    )
    
    # Add argument for report formats
    parser.add_argument(
        '--report-formats',
        choices=['html', 'json', 'junit', 'all'],
        default='all',
        help='Test report formats to generate (default: all)'
    )
    
    # Add argument for verbose output
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    # Add argument for fail-fast
    parser.add_argument(
        '--fail-fast',
        action='store_true',
        help='Stop after first test failure'
    )
    
    # Add argument for test data size
    parser.add_argument(
        '--data-size',
        choices=['small', 'medium', 'large'],
        default='small',
        help='Size of test datasets to use (default: small)'
    )
    
    # Add argument for e2e test framework
    parser.add_argument(
        '--e2e-framework',
        choices=['cypress', 'playwright', 'both'],
        default='playwright',
        help='Framework to use for e2e tests (default: playwright)'
    )
    
    # Add argument for browser
    parser.add_argument(
        '--browser',
        choices=['chrome', 'firefox', 'webkit', 'all'],
        default='chrome',
        help='Browser to use for e2e tests (default: chrome)'
    )
    
    # Add arguments for environment setup/teardown
    parser.add_argument(
        '--skip-setup',
        action='store_true',
        help='Skip environment setup steps'
    )
    
    parser.add_argument(
        '--skip-teardown',
        action='store_true',
        help='Skip environment teardown steps'
    )
    
    return parser.parse_args()


def setup_environment(args: argparse.Namespace, logger: logging.Logger) -> dict:
    """
    Prepare the test environment for CI execution.
    
    Args:
        args: Command-line arguments
        logger: Logger instance
        
    Returns:
        Environment configuration dictionary
    """
    logger.info("Setting up test environment")
    
    # Create necessary directories
    os.makedirs(REPORT_DIR, exist_ok=True)
    os.makedirs(COMBINED_REPORT_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)
    
    # Create temp directory for test execution
    temp_dir = create_temp_directory(prefix="ci_test_")
    logger.debug(f"Created temporary directory: {temp_dir}")
    
    # Set environment variables for CI execution
    os.environ['CI_TESTING'] = 'true'
    os.environ['TEST_ENVIRONMENT'] = args.environment
    
    # Configure test environment based on args.environment
    env_config = {
        'temp_dir': temp_dir,
        'environment': args.environment,
        'report_dir': REPORT_DIR,
        'combined_report_dir': COMBINED_REPORT_DIR,
        'ci_mode': True
    }
    
    # Environment-specific configuration
    if args.environment == 'local':
        env_config['db_connection'] = 'sqlite:///:memory:'
        env_config['use_mocks'] = True
    elif args.environment == 'dev':
        env_config['db_connection'] = os.environ.get('DEV_DB_CONNECTION', 'sqlite:///dev.db')
        env_config['use_mocks'] = os.environ.get('USE_MOCKS', 'true').lower() == 'true'
    elif args.environment == 'staging':
        env_config['db_connection'] = os.environ.get('STAGING_DB_CONNECTION')
        env_config['use_mocks'] = False
    elif args.environment == 'prod':
        env_config['db_connection'] = os.environ.get('PROD_DB_CONNECTION')
        env_config['use_mocks'] = False
    
    # Prepare test data based on args.data_size
    if args.data_size == 'small':
        env_config['data_size_factor'] = 1
    elif args.data_size == 'medium':
        env_config['data_size_factor'] = 10
    elif args.data_size == 'large':
        env_config['data_size_factor'] = 100
    
    logger.info(f"Environment configured for: {args.environment}")
    logger.debug(f"Environment config: {env_config}")
    
    return env_config


def run_unit_tests(args: argparse.Namespace, env_config: dict, logger: logging.Logger) -> bool:
    """
    Execute unit tests for both backend and frontend.
    
    Args:
        args: Command-line arguments
        env_config: Environment configuration
        logger: Logger instance
        
    Returns:
        True if tests passed, False otherwise
    """
    logger.info("Starting unit tests")
    
    # Construct command for running unit tests
    cmd = [
        sys.executable, "-m", "pytest",
        f"{TEST_DIR}/unit",
        "-v",
        f"--junitxml={REPORT_DIR}/unit-tests.xml"
    ]
    
    # Add coverage flag if args.coverage is True
    if args.coverage:
        cmd.extend([
            "--cov=src",
            "--cov-report=term",
            f"--cov-report=html:{REPORT_DIR}/coverage",
            f"--cov-report=xml:{REPORT_DIR}/coverage.xml"
        ])
    
    # Add CI mode flag for appropriate reporting
    cmd.append("--ci-mode")
    
    logger.debug(f"Unit test command: {' '.join(cmd)}")
    
    # Execute command using subprocess.run
    try:
        result = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            env=os.environ
        )
        
        # Capture exit code and output
        exit_code = result.returncode
        
        # Log test results and details
        if exit_code == 0:
            logger.info("Unit tests passed successfully")
        else:
            logger.error(f"Unit tests failed with exit code {exit_code}")
            logger.debug(f"Stdout: {result.stdout}")
            logger.debug(f"Stderr: {result.stderr}")
        
        # Return True if exit code is 0, False otherwise
        return exit_code == 0
    
    except Exception as e:
        logger.error(f"Error running unit tests: {str(e)}")
        return False


def run_integration_tests(args: argparse.Namespace, env_config: dict, logger: logging.Logger) -> bool:
    """
    Execute integration tests for system components.
    
    Args:
        args: Command-line arguments
        env_config: Environment configuration
        logger: Logger instance
        
    Returns:
        True if tests passed, False otherwise
    """
    logger.info("Starting integration tests")
    
    # Construct command for running integration tests
    cmd = [
        sys.executable, "-m", "pytest",
        f"{TEST_DIR}/integration",
        "-v",
        f"--junitxml={REPORT_DIR}/integration-tests.xml"
    ]
    
    # Add environment setup/teardown flags based on args
    if args.skip_setup:
        cmd.append("--skip-setup")
    
    if args.skip_teardown:
        cmd.append("--skip-teardown")
    
    # Add CI mode flag for appropriate reporting
    cmd.append("--ci-mode")
    
    # Add environment-specific config
    cmd.append(f"--env={args.environment}")
    
    logger.debug(f"Integration test command: {' '.join(cmd)}")
    
    # Execute command using subprocess.run
    try:
        result = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            env=os.environ
        )
        
        # Capture exit code and output
        exit_code = result.returncode
        
        # Log test results and details
        if exit_code == 0:
            logger.info("Integration tests passed successfully")
        else:
            logger.error(f"Integration tests failed with exit code {exit_code}")
            logger.debug(f"Stdout: {result.stdout}")
            logger.debug(f"Stderr: {result.stderr}")
        
        # Return True if exit code is 0, False otherwise
        return exit_code == 0
    
    except Exception as e:
        logger.error(f"Error running integration tests: {str(e)}")
        return False


def run_performance_tests(args: argparse.Namespace, env_config: dict, logger: logging.Logger) -> bool:
    """
    Execute performance tests for system components.
    
    Args:
        args: Command-line arguments
        env_config: Environment configuration
        logger: Logger instance
        
    Returns:
        True if tests passed, False otherwise
    """
    logger.info("Starting performance tests")
    
    # Construct command for running performance tests
    cmd = [
        sys.executable, "-m", "pytest",
        f"{TEST_DIR}/performance",
        "-v",
        f"--junitxml={REPORT_DIR}/performance-tests.xml"
    ]
    
    # Add test data size parameter from args
    cmd.append(f"--data-size={args.data_size}")
    
    # Add CI mode flag for appropriate reporting
    cmd.append("--ci-mode")
    
    logger.debug(f"Performance test command: {' '.join(cmd)}")
    
    # Execute command using subprocess.run
    try:
        result = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            env=os.environ
        )
        
        # Capture exit code and output
        exit_code = result.returncode
        
        # Log test results and details
        if exit_code == 0:
            logger.info("Performance tests passed successfully")
        else:
            logger.error(f"Performance tests failed with exit code {exit_code}")
            logger.debug(f"Stdout: {result.stdout}")
            logger.debug(f"Stderr: {result.stderr}")
        
        # Return True if exit code is 0, False otherwise
        return exit_code == 0
    
    except Exception as e:
        logger.error(f"Error running performance tests: {str(e)}")
        return False


def run_e2e_tests(args: argparse.Namespace, env_config: dict, logger: logging.Logger) -> bool:
    """
    Execute end-to-end tests for the complete system.
    
    Args:
        args: Command-line arguments
        env_config: Environment configuration
        logger: Logger instance
        
    Returns:
        True if tests passed, False otherwise
    """
    logger.info("Starting end-to-end tests")
    
    # Determine which framework to use
    frameworks = []
    if args.e2e_framework == 'both':
        frameworks = ['cypress', 'playwright']
    else:
        frameworks = [args.e2e_framework]
    
    all_success = True
    
    for framework in frameworks:
        logger.info(f"Running {framework} e2e tests")
        
        if framework == 'cypress':
            # Construct command for running Cypress e2e tests
            cmd = [
                "npx", "cypress", "run",
                "--headless",
                f"--browser={args.browser}",
                "--reporter", "junit",
                f"--reporter-options=mochaFile={REPORT_DIR}/e2e-cypress-tests.xml"
            ]
        else:  # playwright
            # Construct command for running Playwright e2e tests
            cmd = [
                "npx", "playwright", "test",
                f"--project={args.browser}",
                "--reporter=junit",
                f"--reporter-output={REPORT_DIR}/e2e-playwright-tests.xml"
            ]
        
        logger.debug(f"E2E test command ({framework}): {' '.join(cmd)}")
        
        # Execute command using subprocess.run
        try:
            result = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
                env=os.environ
            )
            
            # Capture exit code and output
            exit_code = result.returncode
            
            # Log test results and details
            if exit_code == 0:
                logger.info(f"{framework} e2e tests passed successfully")
            else:
                logger.error(f"{framework} e2e tests failed with exit code {exit_code}")
                logger.debug(f"Stdout: {result.stdout}")
                logger.debug(f"Stderr: {result.stderr}")
                all_success = False
        
        except Exception as e:
            logger.error(f"Error running {framework} e2e tests: {str(e)}")
            all_success = False
    
    return all_success


def generate_combined_report(args: argparse.Namespace, test_results: dict, logger: logging.Logger) -> bool:
    """
    Generate a combined report from all test results.
    
    Args:
        args: Command-line arguments
        test_results: Dictionary of test results by test type
        logger: Logger instance
        
    Returns:
        True if report generation succeeded, False otherwise
    """
    logger.info("Generating combined test report")
    
    # Determine which report formats to generate
    formats = []
    if args.report_formats == 'all':
        formats = ['html', 'json', 'junit']
    else:
        formats = [args.report_formats]
    
    # Construct command for generate_test_report.py
    cmd = [
        sys.executable,
        f"{TEST_DIR}/scripts/generate_test_report.py",
        "--output-dir", str(COMBINED_REPORT_DIR),
    ]
    
    # Add input directories for all executed test types
    if test_results.get('unit', {}).get('executed', False):
        cmd.extend(["--unit-results", f"{REPORT_DIR}/unit-tests.xml"])
    
    if test_results.get('integration', {}).get('executed', False):
        cmd.extend(["--integration-results", f"{REPORT_DIR}/integration-tests.xml"])
    
    if test_results.get('performance', {}).get('executed', False):
        cmd.extend(["--performance-results", f"{REPORT_DIR}/performance-tests.xml"])
    
    if test_results.get('e2e', {}).get('executed', False):
        if 'cypress' in args.e2e_framework or args.e2e_framework == 'both':
            cmd.extend(["--e2e-cypress-results", f"{REPORT_DIR}/e2e-cypress-tests.xml"])
        if 'playwright' in args.e2e_framework or args.e2e_framework == 'both':
            cmd.extend(["--e2e-playwright-results", f"{REPORT_DIR}/e2e-playwright-tests.xml"])
    
    # Add report formats
    for fmt in formats:
        cmd.append(f"--{fmt}")
    
    logger.debug(f"Report generation command: {' '.join(cmd)}")
    
    # Execute command using subprocess.run
    try:
        result = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            env=os.environ
        )
        
        # Capture exit code and output
        exit_code = result.returncode
        
        # Log report generation results
        if exit_code == 0:
            logger.info(f"Combined report generated successfully in {COMBINED_REPORT_DIR}")
            for fmt in formats:
                logger.info(f"- {fmt.upper()} report: {COMBINED_REPORT_DIR}/report.{fmt}")
        else:
            logger.error(f"Report generation failed with exit code {exit_code}")
            logger.debug(f"Stdout: {result.stdout}")
            logger.debug(f"Stderr: {result.stderr}")
        
        # Return True if exit code is 0, False otherwise
        return exit_code == 0
    
    except Exception as e:
        logger.error(f"Error generating combined report: {str(e)}")
        return False


def cleanup_environment(env_config: dict, logger: logging.Logger) -> bool:
    """
    Clean up the test environment after execution.
    
    Args:
        env_config: Environment configuration
        logger: Logger instance
        
    Returns:
        True if cleanup succeeded, False otherwise
    """
    logger.info("Cleaning up test environment")
    
    try:
        # Remove temporary directories and files
        if 'temp_dir' in env_config and os.path.exists(env_config['temp_dir']):
            shutil.rmtree(env_config['temp_dir'])
            logger.debug(f"Removed temporary directory: {env_config['temp_dir']}")
        
        # Call cleanup_temp_resources from test_helpers
        cleanup_temp_resources()
        logger.debug("Cleaned up temporary resources")
        
        # Reset environment variables if needed
        if 'CI_TESTING' in os.environ:
            del os.environ['CI_TESTING']
        
        logger.info("Environment cleanup completed")
        return True
    
    except Exception as e:
        logger.error(f"Error during environment cleanup: {str(e)}")
        return False


class TestExecutionManager:
    """Class for managing test execution in CI environments."""
    
    def __init__(self, args: argparse.Namespace, logger: logging.Logger):
        """
        Initialize the TestExecutionManager.
        
        Args:
            args: Command-line arguments
            logger: Logger instance
        """
        self._args = args
        self._logger = logger
        self._test_results = {}
        self._env_config = setup_environment(args, logger)
        self._logger.info("TestExecutionManager initialized")
    
    def execute_tests(self) -> bool:
        """
        Execute all requested test types.
        
        Returns:
            True if all tests passed, False otherwise
        """
        # Determine which test types to run
        test_types = []
        if self._args.test_types == 'all':
            test_types = ['unit', 'integration', 'performance', 'e2e']
        else:
            test_types = [self._args.test_types]
        
        self._logger.info(f"Executing test types: {', '.join(test_types)}")
        
        # Execute each test type in the appropriate order
        all_passed = True
        
        for test_type in test_types:
            if test_type == 'unit':
                success = self.execute_unit_tests()
            elif test_type == 'integration':
                success = self.execute_integration_tests()
            elif test_type == 'performance':
                success = self.execute_performance_tests()
            elif test_type == 'e2e':
                success = self.execute_e2e_tests()
            else:
                self._logger.warning(f"Unknown test type: {test_type}")
                continue
            
            # If args.fail_fast is True, stop after first failure
            if not success and self._args.fail_fast:
                self._logger.info("Stopping test execution due to fail-fast option")
                all_passed = False
                break
            
            all_passed = all_passed and success
        
        # Generate combined report if requested
        if self._test_results:
            generate_combined_report(self._args, self._test_results, self._logger)
        
        return all_passed
    
    def execute_unit_tests(self) -> bool:
        """
        Execute unit tests.
        
        Returns:
            True if tests passed, False otherwise
        """
        self._test_results['unit'] = {'executed': True}
        success = run_unit_tests(self._args, self._env_config, self._logger)
        self._test_results['unit']['success'] = success
        return success
    
    def execute_integration_tests(self) -> bool:
        """
        Execute integration tests.
        
        Returns:
            True if tests passed, False otherwise
        """
        self._test_results['integration'] = {'executed': True}
        success = run_integration_tests(self._args, self._env_config, self._logger)
        self._test_results['integration']['success'] = success
        return success
    
    def execute_performance_tests(self) -> bool:
        """
        Execute performance tests.
        
        Returns:
            True if tests passed, False otherwise
        """
        self._test_results['performance'] = {'executed': True}
        success = run_performance_tests(self._args, self._env_config, self._logger)
        self._test_results['performance']['success'] = success
        return success
    
    def execute_e2e_tests(self) -> bool:
        """
        Execute end-to-end tests.
        
        Returns:
            True if tests passed, False otherwise
        """
        self._test_results['e2e'] = {'executed': True}
        success = run_e2e_tests(self._args, self._env_config, self._logger)
        self._test_results['e2e']['success'] = success
        return success
    
    def generate_report(self) -> bool:
        """
        Generate combined test report.
        
        Returns:
            True if report generation succeeded, False otherwise
        """
        return generate_combined_report(self._args, self._test_results, self._logger)
    
    def cleanup(self) -> bool:
        """
        Clean up test environment.
        
        Returns:
            True if cleanup succeeded, False otherwise
        """
        return cleanup_environment(self._env_config, self._logger)
    
    def get_results_summary(self) -> dict:
        """
        Get a summary of test execution results.
        
        Returns:
            Summary of test results
        """
        executed_count = sum(1 for t in self._test_results.values() if t.get('executed', False))
        success_count = sum(1 for t in self._test_results.values() if t.get('success', False))
        
        return {
            'executed': executed_count,
            'successful': success_count,
            'failed': executed_count - success_count,
            'all_passed': executed_count > 0 and success_count == executed_count
        }


def main() -> int:
    """
    Main function that orchestrates the CI test execution.
    
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Parse command-line arguments
    args = parse_arguments()
    
    # Set up logging based on verbosity
    logger = setup_logging(args.verbose)
    logger.info("CI Test Runner started")
    
    try:
        # Create test execution manager
        manager = TestExecutionManager(args, logger)
        
        # Execute requested tests
        all_passed = manager.execute_tests()
        
        # Get results summary
        summary = manager.get_results_summary()
        logger.info(f"Test execution summary: {summary['executed']} executed, "
                    f"{summary['successful']} successful, {summary['failed']} failed")
        
        # Clean up test environment
        manager.cleanup()
        
        # Return appropriate exit code
        if all_passed:
            logger.info("All tests passed successfully!")
            return 0
        else:
            logger.error("Some tests failed!")
            return 1
    
    except Exception as e:
        logger.error(f"Error during test execution: {str(e)}", exc_info=True)
        return 2


if __name__ == "__main__":
    sys.exit(main())