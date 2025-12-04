# Contributing to dbzero

Thank you for your interest in contributing to dbzero! We welcome contributions from the community and are grateful for your support.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [How to Contribute](#how-to-contribute)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Commit Message Guidelines](#commit-message-guidelines)
- [Pull Request Process](#pull-request-process)
- [Reporting Bugs](#reporting-bugs)
- [Suggesting Enhancements](#suggesting-enhancements)
- [Documentation](#documentation)
- [Community](#community)

## Code of Conduct

This project adheres to a code of conduct that all contributors are expected to follow. Please be respectful, inclusive, and considerate in all interactions.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/dbzero.git
   cd dbzero
   ```
3. **Add the upstream repository** as a remote:
   ```bash
   git remote add upstream https://github.com/ORIGINAL_OWNER/dbzero.git
   ```

## Development Setup

### Prerequisites

- Python 3.8 or higher
- Git
- A virtual environment tool (venv, virtualenv, or conda)

### Setting Up Your Environment

1. **Create a virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. **Install dependencies**:
   ```bash
   pip install -e ".[dev]"  # Install in editable mode with dev dependencies
   ```

3. **Verify the installation**:
   ```bash
   python -m pytest
   ```

## How to Contribute

### Types of Contributions

We welcome various types of contributions:

- **Bug fixes** - Fix issues and improve stability
- **New features** - Add new functionality
- **Documentation** - Improve or add documentation
- **Tests** - Add or improve test coverage
- **Performance improvements** - Optimize existing code
- **Code refactoring** - Improve code quality and maintainability

### Finding Something to Work On

- Check the [issue tracker](../../issues) for open issues
- Look for issues labeled `good first issue` or `help wanted`
- Review the roadmap and feature requests
- If you have a new idea, open an issue to discuss it first

## Coding Standards

### Python Style Guide

- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) style guidelines
- Use meaningful variable and function names
- Keep functions small and focused on a single task
- Maximum line length: 100 characters (flexibility allowed for readability)

### Code Quality

- **Type hints**: Use type annotations where appropriate
- **Docstrings**: Document all public modules, functions, classes, and methods using Google or NumPy style
- **Comments**: Write clear comments for complex logic
- **DRY principle**: Don't Repeat Yourself - extract common code into reusable functions

### Formatting Tools

We recommend using the following tools:

```bash
# Format code
black .

# Sort imports
isort .

# Lint code
flake8 .

# Type checking
mypy .
```

## Testing Guidelines

### Writing Tests

- Write tests for all new features and bug fixes
- Aim for high test coverage (>80%)
- Use descriptive test names that explain what is being tested
- Follow the Arrange-Act-Assert pattern
- Keep tests independent and isolated

### Test Structure

```python
def test_feature_name_should_do_something():
    # Arrange
    setup_data = create_test_data()
    
    # Act
    result = function_under_test(setup_data)
    
    # Assert
    assert result == expected_value
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=dbzero

# Run specific test file
pytest tests/test_module.py

# Run specific test
pytest tests/test_module.py::test_function_name
```

## Commit Message Guidelines

Write clear and meaningful commit messages following these conventions:

### Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types

- **feat**: A new feature
- **fix**: A bug fix
- **docs**: Documentation changes
- **style**: Code style changes (formatting, missing semicolons, etc.)
- **refactor**: Code refactoring without changing functionality
- **perf**: Performance improvements
- **test**: Adding or updating tests
- **chore**: Maintenance tasks, dependency updates

### Examples

```
feat(transactions): add support for nested transactions

Implement nested transaction handling with proper rollback
support for inner transactions.

Closes #123
```

```
fix(cache): prevent memory leak in object cache

The cache was not properly releasing references to deleted
objects, causing memory growth over time.

Fixes #456
```

## Pull Request Process

### Before Submitting

1. **Update your fork** with the latest upstream changes:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

2. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make your changes** following the coding standards

4. **Add tests** for your changes

5. **Run the test suite** to ensure everything passes

6. **Update documentation** if needed

### Submitting the Pull Request

1. **Push your branch** to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

2. **Open a Pull Request** on GitHub with:
   - Clear title describing the change
   - Detailed description of what and why
   - Reference to related issues (e.g., "Closes #123")
   - Screenshots or examples if applicable

3. **Respond to feedback** - Be open to suggestions and make requested changes promptly

### PR Review Criteria

Your PR will be reviewed for:

- Code quality and style compliance
- Test coverage
- Documentation completeness
- Backward compatibility
- Performance implications
- Security considerations

## Reporting Bugs

### Before Reporting

- Check if the bug has already been reported in the issue tracker
- Verify the bug exists in the latest version
- Collect relevant information about your environment

### Bug Report Template

When reporting bugs, please include:

- **Description**: Clear description of the issue
- **Steps to Reproduce**: Minimal steps to reproduce the problem
- **Expected Behavior**: What you expected to happen
- **Actual Behavior**: What actually happened
- **Environment**:
  - Python version
  - dbzero version
  - Operating system
  - Relevant dependencies
- **Code Sample**: Minimal code that reproduces the issue
- **Error Messages**: Full error messages and stack traces
- **Screenshots**: If applicable

## Suggesting Enhancements

We welcome feature suggestions! When proposing enhancements:

1. **Check existing issues** to avoid duplicates
2. **Open an issue** with the `enhancement` label
3. **Provide context**:
   - What problem does this solve?
   - Who would benefit from this feature?
   - How should it work?
   - Are there alternative approaches?
4. **Be open to discussion** about implementation details

## Documentation

Good documentation is crucial. When contributing:

### Code Documentation

- Add docstrings to all public APIs
- Include examples in docstrings where helpful
- Keep documentation up-to-date with code changes

### User Documentation

- Update README.md for user-facing changes
- Add tutorials or guides for new features
- Update API reference documentation

### Documentation Style

- Use clear, concise language
- Provide practical examples
- Explain both the "how" and the "why"
- Keep formatting consistent

## Community

### Getting Help

- Open an issue for bugs or questions
- Check existing documentation and issues first
- Be specific and provide context

### Staying Updated

- Watch the repository for updates
- Follow project announcements
- Participate in discussions

### Recognition

Contributors are recognized in:
- The project's contributors list
- Release notes for significant contributions
- Special mentions for exceptional contributions

## License

By contributing to dbzero, you agree that your contributions will be licensed under the same license as the project (AGPL v3).

---

Thank you for contributing to dbzero! Your efforts help make this project better for everyone. 🎉
