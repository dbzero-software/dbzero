# dbzero

**A state management system for Python 3.x that unifies your application's business logic, data persistence, and caching into a single, efficient layer.**

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

---

## Overview

**dbzero** lets you code as if you have infinite memory. Inspired by a thought experiment from *Architecture Patterns with Python* by Harry Percival and Bob Gregory, dbzero handles the complexities of data management in the background while you work with simple Python objects.

dbzero implements the **DISTIC memory** model:
- **D**urable - Data persists automatically across application restarts
- **I**nfinite - Work with data as if memory constraints don't exist
- **S**hared - Multiple processes can access and share the same data
- **T**ransactions - Transaction support for data integrity
- **I**solated - Operations are isolated and thread-safe
- **C**omposable - A single process can integrate multiple memory partitions (prefixes) to suit its specific requirements

The result is a simplified application stack that can eliminate the need for separate databases, ORMs, and caching layers—reducing development time by up to **80%** and, in some cases, enabling applications to run **hundreds of times faster**.

---

## Key Features

### Database Capabilities
- **ACID Transactions** - Serializable consistency guarantees for all data operations
- **Single-writer, Multiple-reader** concurrency model
- **Automatic Persistence** - Objects are automatically saved to disk
- **Efficient Caching** - Only accessed data is loaded into memory
- **Time Travel** - Query historical states at any transaction point
- **Horizontal Scalability** - Built-in support for data partitioning and sharding
- **Custom Data Models** - Unlike traditional databases, dbzero allows you to define custom data structures that match your domain's needs, enabling not just constant speedup but algorithmic speedup (e.g., O(log N) → O(1))

### Developer Experience
- **Invisible by Design** - Minimal API surface; write regular Python code
- **Dynamic Schema** - No separate data models or schema definitions needed
- **AI-Friendly** - Works exceptionally well with AI coding agents
- **Type Support** - Full support for Python's built-in types and collections
- **Reference Counting** - Automatic garbage collection of unused objects

---

## Quick Start

### Installation

```bash
pip install dbzero
```

### Hello World Example

```python
from dataclasses import dataclass
import dbzero as db0

@db0.memo(singleton=True)
@dataclass
class Root:
    greeting: str

    def greet(self):
        print(self.greeting)

if __name__ == "__main__":
    db0.init(dbzero_root="./data")
    
    if db0.exists(Root):
        print("Process started with data...")
        root = db0.fetch(Root)
    else:
        root = Root("Hello, World!")
    
    root.greet()  # Output: Hello, World!
```

When the process exits, the application state is persisted automatically. The same data will be available the next time the app starts.

**Note:** All objects linked to `Root` (and any objects they reference) are automatically managed by dbzero. There's no need for explicit conversions, fetching, or saving—dbzero handles persistence transparently for the entire object graph.

---

## Core Concepts

### Memo Classes

Transform any Python class into a persistent, managed object by applying the `@db0.memo` decorator:

```python
import dbzero as db0

@db0.memo
class Person:
    def __init__(self, name: str, age: int):
        self.name = name
        self.age = age

# Instantiation works just like regular Python
person = Person("Alice", 30)

# Attributes can be modified dynamically
person.age += 1
person.address = "123 Main St"  # Add new attributes on the fly
```

**Features of memo classes:**
- Automatic durability and persistence
- Efficient memory/disk management
- Automatic UUID assignment
- Support for tagging and querying
- Reference counting for garbage collection

### Collections

dbzero provides durable, transactional versions of Python's built-in collections:

```python
# Create persistent collections
my_dict = db0.dict(name="Alice", age=30)
my_list = db0.list([1, 2, 3, 4, 5])
my_tuple = db0.tuple(("a", "b", "c"))

# They work just like standard Python collections
my_dict["city"] = "New York"
my_list.append(6)
print(len(my_list))  # 6
```

All standard operations are supported, and changes are automatically persisted within transactions.

### Transactions

By default, dbzero uses **autocommit** mode, periodically persisting changes to storage:

```python
# Manual commit for batch operations
tasks = db0.list()
for i in range(100):
    tasks.append(Task(f"Task {i}"))

db0.commit()  # Persist all 100 tasks at once
```

### Queries and Tags

Find objects using type-based queries and flexible tag logic:

```python
# Create and tag objects
person = Person("Alice", 30)
db0.tags(person).add("employee")
db0.tags(person).add("manager")

# Find by type
all_persons = db0.find(Person)

# Find by tag
managers = db0.find("manager")

# Combine type and tags (AND logic)
employee_persons = db0.find(Person, "employee")

# OR logic using a list
results = db0.find(["tag1", "tag2"])

# NOT logic using db0.no()
non_managers = db0.find("employee", db0.no("manager"))
```

### Snapshots and Time Travel

Create isolated, read-only views of your data at any point in time:

```python
# Create a snapshot of the current state
with db0.snapshot() as snap:
    # Changes to live objects won't affect the snapshot
    obj.value = 999
    
    # Query the snapshot - sees old data
    old_obj = snap.fetch(MemoTestClass)
    assert old_obj.value == 123

# Time travel to a specific transaction
state_num = db0.get_state_num()
# ... make changes ...

with db0.snapshot(state_num) as past_snap:
    # Access data exactly as it was at that state
    past_obj = past_snap.fetch(obj_uuid)
```

### Prefixes (Data Partitioning)

Organize data into isolated partitions with independent commit histories:

```python
# Scope a class to a specific prefix
@db0.memo(prefix="settings-prefix")
class AppSettings:
    def __init__(self, theme: str):
        self.theme = theme

# Open and work with different prefixes
db0.open("user-data", "rw")  # Set current prefix
settings = AppSettings(theme="dark")  # Goes to "settings-prefix"
note = UserNote(content="Hello")      # Goes to "user-data"
```

### Indexes

Create fast, sorted access to your data:

```python
# Create an index
priority_queue = db0.index()

# Add items with keys for sorting
task1 = Task("High priority task")
task2 = Task("Low priority task")

priority_queue.add(1, task1)  # key=1
priority_queue.add(10, task2)  # key=10

# Iterate in sorted order
for key, task in priority_queue:
    print(f"Priority {key}: {task.description}")
```

---

## Advanced Features

### Multi-Process Synchronization

dbzero automatically synchronizes data between processes:

```python
# Writer process
db0.open(prefix_name, "w")
obj.value = 124
db0.commit()

# Reader process - sees changes automatically
db0.open(prefix_name, "r")
assert obj.value == 124  # Updated value visible
```

### Change Data Capture

Compare snapshots to identify changes:

```python
snap_before = db0.snapshot()
# ... make changes ...
snap_after = db0.snapshot()

# Find what changed
new_items = db0.select_new(query, snap_before, snap_after)
deleted_items = db0.select_deleted(query, snap_before, snap_after)
modified_items = db0.select_modified(query, snap_before, snap_after)
```

### Singleton Pattern

Create unique instances per prefix:

```python
@db0.memo(singleton=True)
class AppConfig:
    def __init__(self, version: str):
        self.version = version

# Only one instance can exist per prefix
config = AppConfig("1.0.0")
config_ref = db0.fetch(AppConfig)  # Same instance
assert config is config_ref
```

---

## Scalability

dbzero is built for horizontal scalability using proven distributed systems patterns:

- **Data Partitioning** - Automatic sharding across multiple prefixes
- **Multiple-Handshake Transactions** - Ensures data integrity across distributed systems
- **Cache Locality** - Indexes can be bound to specific contexts for optimal performance

---

## Configuration

Configure dbzero during initialization:

```python
db0.init(
    storage_dir="/path/to/data",
    config={
        'autocommit': True,
        'autocommit_interval': 1000,  # milliseconds
        'cache_size': 1024 * 1024 * 100  # 100MB
    }
)
```

Or configure specific prefixes:

```python
db0.open("my-prefix", "rw", autocommit=False)
```

---

## Use Cases

- **Web Applications** - Unified state management for backend services
- **Data Processing Pipelines** - Efficient batch operations with transaction support
- **Event-Driven Systems** - Change data capture and time travel for auditing
- **Distributed Systems** - Built-in support for multi-process synchronization
- **AI Applications** - Simplified state management for AI agents and workflows

---

## Documentation

For comprehensive documentation, visit: **[docs.dbzero.io](https://docs.dbzero.io)**

Topics covered:
- API Reference
- Tutorials
- Data Modeling Patterns
- Performance Optimization
- Migration Guides

---

## Commercial Support

Need help building large-scale solutions with dbzero?

We offer:
- Custom UI and admin tools
- System integrations
- Expert consulting and architectural reviews
- Performance tuning

Contact us at: **info@dbzero.io**

---

## Why dbzero?

### Traditional Stack
```
Application Code
    ↓
ORM Layer
    ↓
Caching Layer
    ↓
Database Layer
    ↓
Storage
```

### With dbzero
```
Application Code + dbzero
    ↓
Storage
```

By eliminating intermediate layers, dbzero reduces complexity, improves performance, and accelerates development—all while providing the reliability and features you expect from a database system.

---

## License

This project is licensed under the GNU Affero General Public License v3.0 (AGPLv3). See `LICENSE` for the full text.

- If you modify and run this software over a network, you must offer the complete corresponding source code to users interacting with it (AGPLv3 §13).
- Redistributions must preserve copyright and license notices and provide source.

For a short summary, see `COPYING`. For attribution details, see `NOTICE`.

---

## Support

- **Documentation**: [docs.dbzero.io](https://docs.dbzero.io)
- **Email**: info@dbzero.io
- **Issues**: https://github.com/dbzero-software/dbzero/issues

---

**Start coding as if you have infinite memory. Let dbzero handle the rest.**
