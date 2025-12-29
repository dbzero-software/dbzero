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
- [Contributor License Agreement](#contributor-license-agreement)
- [License](#license)

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
   git remote add upstream https://github.com/dbzero-software/dbzero.git
   ```

## Development Setup

### Prerequisites

- C++ compiler (GCC 13+ or Microsoft C/C++ Optimizing Compiler 19.41+)
- Meson
- Ninja build system
- Git
- Python 3.9 or higher (for Python components and tooling)
- A virtual environment tool (venv, virtualenv, or conda) for Python dependencies

### Setting Up Your Environment

1. **Build the C++ components**:
   ```bash
   # Build debug version without tests
   ./scripts/build.sh
   
   # Build debug version with tests
   ./scripts/build.sh -t
   
   # Build release version
   ./scripts/build.sh -r
   
   # Build release version with tests
   ./scripts/build.sh -r -t
   ```
   
   Alternatively, you can use Meson directly:
   ```bash
   meson setup build
   meson compile -C build
   ```

2. **Create a Python virtual environment** (for Python components):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt  # Install dependencies
   ```

4. **Verify the installation**:
   ```bash
   # Run C++ tests (debug build)
   ./build/debug/testsD.x
   
   # Run C++ tests (release build)
   ./build/release/tests.x
   
   # Or use Meson test runner
   meson test -C build
   
   # Run Python tests (excludes stress tests)
   ./scripts/run_tests.sh
   
   # Run specific Python test
   ./scripts/run_tests.sh -k=test_name
   
   # Run stress tests only (takes longer)
   ./scripts/run_stress_tests.sh
   
   # Run specific stress test
   ./scripts/run_stress_tests.sh -k=test_name
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

- Check the [issue tracker](https://github.com/dbzero-software/dbzero/issues) for open issues
- Look for issues labeled `good first issue` or `help wanted`
- Review the roadmap and feature requests
- If you have a new idea, open an issue to discuss it first

## Coding Standards

### C++ Style Guide (Primary)

Since the project is predominantly C++ (85%), these are our primary coding standards:

- Follow **Modern C++** best practices
- Use **meaningful variable and function names** with clear intent
- Follow **RAII principles** for resource management
- Prefer **smart pointers** over raw pointers
- Use **const correctness** throughout
- Keep functions small and focused on a single task
- Maximum line length: 120 characters

#### C++ Code Quality

- **Naming Conventions**:
  - Classes/Structs: `PascalCase`
  - Functions/Methods: `camelCase`
  - Variables: `snake_case`
  - Constants: `UPPER_SNAKE_CASE`
  - Namespaces: `lowercase`

- **Documentation**: Use Doxygen-style comments for all public APIs
  ```cpp
  /**
   * @brief Brief description of the function
   * @param param1 Description of parameter
   * @return Description of return value
   */
  ```

- **Error Handling**: Use exceptions for exceptional cases, return values for expected errors
- **Comments**: Write clear comments for complex logic, algorithms, and non-obvious decisions
- **DRY Principle**: Extract common code into reusable functions and templates

### Python Style Guide (Additional)

For the Python components (~15% of the project):

- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) style guidelines
- Use meaningful variable and function names
- Keep functions small and focused on a single task
- Maximum line length: 100 characters

#### Python Code Quality

- **Type hints**: Use type annotations where appropriate
- **Docstrings**: Document all public modules, functions, classes, and methods using NumPy style
- **Comments**: Write clear comments for complex logic
- **DRY principle**: Extract common code into reusable functions

## Testing Guidelines

### Writing Tests

- Write tests for all new features and bug fixes
- Aim for high test coverage (>80%)
- Use descriptive test names that explain what is being tested
- Keep tests independent and isolated

### C++ Testing

We use Google Test (gtest) for C++ unit tests:

```cpp
TEST(TestSuiteName, TestName) {
    // Arrange
    auto test_data = SetupTestData();
    
    // Act
    auto result = FunctionUnderTest(test_data);
    
    // Assert
    EXPECT_EQ(result, expected_value);
    ASSERT_TRUE(condition) << "Error message if fails";
}
```

## Commit Message Guidelines

Write clear and meaningful commit messages explaining scope and purpose of changes you are making.
If your contributions are related to a submitted issue on the github repository, please include issue number(s) at the beginning of the commit message.

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
   - Description of what and why
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
  - C++ compiler and version (e.g., GCC 13.3.0)
  - dbzero version
  - Operating system
  - Python version (if relevant)
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

## Contributor License Agreement

**Important**: Before your start contributing please make sure you read and agree to our Contributor License Agreement (CLA): [CLA.md](./CLA.md)

### Why We Have a CLA

dbzero is primarily licensed under LGPL-2.1, but we want to reserve the right to offer the software under alternative licenses to support different use cases and our business model.

To enable this flexible licensing approach while maintaining our commitment to open source, we need contributors to grant us additional rights to use their contributions.

### What You're Agreeing To

By contributing to dbzero, you agree that:

1. **Your contribution will be licensed under LGPL-2.1** in the open-source dbzero Community Edition
2. **You grant the dbzero project maintainers** a perpetual, worldwide, non-exclusive, royalty-free, irrevocable license to:
   - Use, modify, and distribute your contributions in any form
   - Include your contributions in the open-source project and any alternative licensing options we may offer
   - Sublicense your contributions under different license terms, including proprietary licenses
   - Create derivative works based on your contributions
3. **You retain full copyright** to your contributions and can use them in your own projects
4. **You confirm** that:
   - You have the legal right to grant this license
   - Your contribution is your original work (or properly attributed if based on others' work)
   - Your contribution doesn't violate any third-party intellectual property rights

### How It Works

When you submit your first pull request:
1. Our CLA Assistant bot will prompt you to review and accept the CLA
2. Your acceptance is recorded (this is a one-time process)
3. Your PR will be marked as ready for review
4. All future contributions will be covered under the same agreement

### Questions About the CLA?

If you have questions or concerns about the CLA, please open an issue to discuss before contributing. We're happy to clarify any points.

## License

By contributing to dbzero, you agree that your contributions will be:
- Licensed under the **GNU Lesser General Public License v2.1** (LGPL-2.1) for the open-source dbzero Community Edition
- Subject to the additional rights grant specified in our Contributor License Agreement: [CLA.md](./CLA.md)

---

Thank you for contributing to dbzero! Your efforts help make this project better for everyone. 🎉
