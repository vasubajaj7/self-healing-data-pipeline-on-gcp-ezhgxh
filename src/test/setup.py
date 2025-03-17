import setuptools
import os
import io

def read(filename):
    """
    Reads the content of a file relative to the setup.py location
    
    Args:
        filename (str): The name of the file to read
        
    Returns:
        str: Content of the file
    """
    # Get the directory containing setup.py
    current_dir = os.path.abspath(os.path.dirname(__file__))
    # Join the directory path with the filename
    filepath = os.path.join(current_dir, filename)
    # Open the file in read mode with UTF-8 encoding
    with io.open(filepath, mode="r", encoding="utf-8") as f:
        # Read and return the file content
        return f.read()

def get_requirements():
    """
    Parses requirements.txt and returns a list of dependencies
    
    Returns:
        list: List of package requirements
    """
    # Read the requirements.txt file
    content = read("requirements.txt")
    # Split the content by lines
    requirements = content.strip().split("\n")
    # Filter out empty lines and comments
    requirements = [req for req in requirements if req and not req.startswith("#")]
    return requirements

def get_test_requirements():
    """
    Parses test-specific requirements and returns a list of test dependencies
    
    Returns:
        list: List of test-specific package requirements
    """
    # Get all requirements
    all_requirements = get_requirements()
    # Filter for testing-related packages
    test_requirements = [
        req for req in all_requirements 
        if any(test_pkg in req.lower() for test_pkg in [
            'pytest', 'mock', 'coverage', 'unittest', 'test', 'fixture', 
            'faker', 'hypothesis', 'tox'
        ])
    ]
    return test_requirements

# Setup configuration
setuptools.setup(
    name="self-healing-pipeline-tests",
    version="0.1.0",
    description="Comprehensive testing framework for the self-healing data pipeline for BigQuery",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    author="Data Engineering Team",
    author_email="data-engineering@example.com",
    license="MIT",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Software Development :: Testing",
        "Topic :: Database :: Database Engines/Servers",
    ],
    keywords="testing, data pipeline, self-healing, bigquery, google cloud, data quality, monitoring",
    packages=setuptools.find_packages(),
    python_requires=">=3.9",
    install_requires=get_requirements(),
    tests_require=get_test_requirements(),
    entry_points={
        "console_scripts": [
            "run-tests=src.test.scripts.run_all_tests:main",
            "setup-test-env=src.test.scripts.setup_test_env:main",
            "teardown-test-env=src.test.scripts.teardown_test_env:main",
            "generate-test-data=src.test.scripts.generate_test_data:main",
            "generate-test-report=src.test.scripts.generate_test_report:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
    project_urls={
        "Documentation": "https://github.com/example/self-healing-pipeline/docs",
        "Source": "https://github.com/example/self-healing-pipeline",
        "Issues": "https://github.com/example/self-healing-pipeline/issues",
    },
)