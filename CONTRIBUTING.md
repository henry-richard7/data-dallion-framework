# Contributing to DataDallion Framework 🤝

Thank you for your interest in contributing to the **DataDallion Framework**! We welcome contributions from the community to help make this framework even more robust and capable for data engineers everywhere.

---

## 🚀 How to Get Started

### 1. Fork and Clone
Fork the repository on GitHub and clone your fork locally:

```bash
git clone https://github.com/your-username/data-dallion-framework.git
cd data-dallion-framework
```

### 2. Set Up Development Environment
We use [uv](https://github.com/astral-sh/uv) for dependency management. 

```bash
# Install dependencies and set up venv
uv sync
```

### 3. Create a Branch
Always create a new branch for your work:

```bash
git checkout -b feature/your-feature-name
# OR
git checkout -b fix/your-fix-name
```

---

## 🛠 Coding Standards

To maintain a high-quality codebase, we follow these guidelines:

- **Follow PEP 8**: Adhere to standard Python coding conventions.
- **Type Hinting**: All new functions and classes should include type hints for parameters and return values.
- **Docstrings**: Use **Google-style docstrings** for all public classes and methods.
- **Error Handling**: Avoid bare `except:` blocks. Always catch specific exceptions and log meaningful errors.
- **Resource Management**: Use context managers (`with` statements) for file and network operations.

---

## 🧪 Testing

Before submitting a Pull Request, please ensure your changes do not break existing functionality.

Currently, we use functional test scripts found in the `tests/` directory. You can run them directly:

```bash
# Test API Extraction flow
python tests/test_sample_api.py

# Test DDL Generation
python tests/test_ddl.py
```

> [!TIP]
> We are planning to transition to `pytest`. New contributions that include `pytest`-style unit tests are highly encouraged!

---

## 📬 Submitting a Pull Request

1.  **Commit with Clarity**: Write descriptive commit messages.
2.  **Sync with Upstream**: Before submitting, ensure your branch is up to date with the `main` branch.
3.  **Open the PR**: Provide a clear description of the changes, why they are needed, and any potential side effects.

---

## ⚖️ License
By contributing to DataDallion Framework, you agree that your contributions will be licensed under the **Apache License 2.0**.

---

<p align="center">
  Happy Coding! 🦁
</p>
