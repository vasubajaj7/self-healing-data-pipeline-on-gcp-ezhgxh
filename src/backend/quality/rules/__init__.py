"""
Initialization file for the quality/rules module that exposes key classes and functions
for managing data quality validation rules. This module is a central part of the
self-healing data pipeline's quality validation framework, providing tools for rule
definition, management, and execution.
"""

# Import core rule engine classes and functions
from .rule_engine import (  # src/backend/quality/rules/rule_engine.py
    Rule,
    RuleResult,
    RuleEngine,
    validate_rule_structure,
    generate_rule_id,
)

# Import rule loading and saving functionality
from .rule_loader import (  # src/backend/quality/rules/rule_loader.py
    load_rules_from_file,
    save_rules_to_file,
    get_rule_files_in_directory,
    load_rules_from_directory,
    convert_rule_to_dict,
    export_rules_to_file,
    import_rules_from_file,
)

# Import rule editing and template functionality
from .rule_editor import (  # src/backend/quality/rules/rule_editor.py
    RuleEditor,
    RuleTemplate,
    create_rule_template,
    validate_rule_parameters,
    get_rule_templates,
    get_rule_documentation,
)

__all__ = [
    "Rule",
    "RuleResult",
    "RuleEngine",
    "RuleEditor",
    "RuleTemplate",
    "validate_rule_structure",
    "validate_rule_parameters",
    "generate_rule_id",
    "create_rule_template",
    "get_rule_templates",
    "get_rule_documentation",
    "load_rules_from_file",
    "save_rules_to_file",
    "get_rule_files_in_directory",
    "load_rules_from_directory",
    "convert_rule_to_dict",
    "export_rules_to_file",
    "import_rules_from_file",
]