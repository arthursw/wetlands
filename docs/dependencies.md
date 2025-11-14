# 📦 Dependency Specification Documentation

This document explains how to define dependencies using *Wetlands*' structured format. The schema supports specifying dependencies for different platforms, optional dependencies, and conditional dependencies via `conda` or `pip`.

## 🔧 Type Definitions

### Platform
```python
Platform = Literal["osx-64", "osx-arm64", "win-64", "win-arm64", "linux-64", "linux-arm64"]
```
Defines supported operating systems and architectures.

### Dependency
```python
class Dependency(TypedDict):
    name: str
    platforms: NotRequired[list[Platform]]
    optional: NotRequired[bool]
    dependencies: NotRequired[bool]
```

Represents an individual dependency with additional metadata:

- **name** *(str)*: The name of the package (e.g., `"numpy"`) with an optional channel specification (for conda specification) and a version specifier. Format: `channel::package==version.number`. Supports [PEP 440 version specifiers](https://packaging.python.org/en/latest/specifications/dependency-specifiers/#version-specifiers) like `>=1.20,<2.0`, `~=1.5.0`, `!=1.0.0`, etc.
- **platforms** *(optional)*: A list of platforms on which this package should be installed.
- **optional** *(optional)*: Marks the dependency as optional (e.g., for extra features like enabling computation on GPU).
- **dependencies** *(optional)*: Indicates whether to install sub-dependencies.

---

### Dependencies
```python
class Dependencies(TypedDict):
    python: NotRequired[str]
    conda: NotRequired[list[str | Dependency]]
    pip: NotRequired[list[str | Dependency]]
```

Top-level dependency configuration:

- **python** *(optional str)*: Specifies the Python version required (e.g., `"==3.9"`).
- **conda** *(optional list)*: Conda dependencies (package names or `Dependency` objects).
- **pip** *(optional list)*: Pip dependencies (package names or `Dependency` objects).

---

## 🧪 Example

Here’s an example dependency specification:

```python
dependencies: Dependencies = {
    "python": "==3.11",
    "conda": [
        "numpy",
        {"name": "nvidia::cudatoolkit=11.0.*", "optional": True, "platforms": ["linux-64", "windows-64"]},
        {"name": "nvidia::nvidia::cudnn=8.0.*", "optional": True, "platforms": ["linux-64", "windows-64"]},
        {"name": "pyobjc", "platforms": ["osx-64", "osx-arm64"], "optional": True},
    ],
    "pip": [
        "tensorflow==2.16.1",
        "csbdeep==0.8.1", 
        "stardist==0.9.1",
        {"name": "some-macos-only-package", "platforms": ["osx-arm64"]},
        {"name": "helper", "optional": True, "dependencies": False}
    ]
}
```

### Explanation:

- `python: "==3.11"`: Requires Python version 3.11 exactly.
- `conda` section:
    - `"numpy"`: required on **all platforms**.
    - `"nvidia::cudatoolkit=11.0.*"`: An **optional** CUDA toolkit, installed only on **Linux and Windows (x86_64)** (so that GPU is used on x86_64 linux and windows, and CPU is used otherwise).
    - `"nvidia::nvidia::cudnn=8.0.*"`: An **optional** cuDNN library for deep learning acceleration on **Linux and Windows (x86_64)** (so that GPU is used on x86_64 linux and windows, and CPU is used otherwise).
    - `"pyobjc"`: An **optional** macOS-only dependency for Python–Objective-C bridging, included on both **Intel and Apple Silicon macOS**.
- `pip` section:
    - `"tensorflow==2.16.1"`: Required version of TensorFlow for all platforms.
    - `"csbdeep==0.8.1"` and `"stardist==0.9.1"`: Required deep learning packages for image restoration and segmentation.
    - `"some-macos-only-package"`: Only installed on **macOS Apple Silicon** (`osx-arm64`).
    - `helper`: An **optional** pip package which much be installed without its dependencies.


## ✅ Usage Recommendations

- Use `platforms` to restrict platform-specific packages (e.g., `pyobjc` for macOS).
- Use `optional` for optional feature packages.
- Use `dependencies=False` to only install the package without its dependencies.
