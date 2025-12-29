<picture align="center">
  <source media="(prefers-color-scheme: dark)" srcset="https://dbzero.io/images/white-01.svg">
  <img alt="Dbzero logo" src="https://dbzero.io/images/dbzero-logo.png">
</picture>


**A state management system for Python 3.x that unifies your application's business logic, data persistence, and caching into a single, efficient layer.**

[![License: LGPL 2.1](https://img.shields.io/badge/License-LGPL%202.1-blue.svg)](https://www.gnu.org/licenses/lgpl-2.1)

> "If we had infinite memory in our laptop, we'd have no need for clumsy databases. Instead, we could just use our objects whenever we liked."
> 
> — Harry Percival and Bob Gregory, *Architecture Patterns with Python*

## Overview

**dbzero** lets you code as if you have infinite memory. Inspired by a thought experiment from *Architecture Patterns with Python* by Harry Percival and Bob Gregory, dbzero handles the complexities of data management in the background while you work with simple Python objects.

dbzero implements the **DISTIC memory** model:
- **D**urable - Data persists across application restarts
- **I**nfinite - Work with data as if memory constraints don't exist (e.g. create lists, dicts or sets with billions of elements)
- **S**hared - Multiple processes can access and share the same data
- **T**ransactional - Transaction support for data integrity
- **I**solated - Reads performed against a consistent point-in-time snapshot
- **C**omposable - Plug in multiple prefixes (memory partitions) on demand and access other apps’ data by simply attaching their prefix.

With dbzero, you don’t need separate pieces like a database, ORM, or cache layer. Your app becomes easier to build and it runs faster, because there are no roundtrips to a database, memory is used better, and you can shape your data to fit your problem.

## Key Platform Features

**dbzero** provides the reliability of a traditional database system with modern capabilities and extra features on top.

- **Persistence**: Application objects (classes and common structures like `list`, `dict`, `set`, etc.) are automatically persisted (e.g. to a local or network-attached file)
- **Efficient caching**: Only the data actually accessed is retrieved and cached. For example, if a list has 1 million elements but only 10 are accessed, only those 10 are loaded.
- **Constrained memory usage**: You can define memory limits for the process to control RAM consumption.
- **Serializable consistency**: Data changes can be read immediately, maintaining a consistent view.
- **Transactions**: Make atomic, exception-safe changes using the `with dbzero.atomic():` context manager.
- **Snapshots & Time Travel**: Query data as it existed at a specific point in the past. This enables tracking of data changes and simplify auditing.
- **Tags**: Tag objects and use tags to filter or retrieve data.
- **Indexing**: Define lightweight, imperative indexes that can be dynamically created and updated.
- **Data composability**: Combine data from different apps, processes, or servers and access it through a unified interface - i.e. your application’s objects, methods and functions.
- **UUID support**: All objects are automatically assigned a universally unique identifier, allowing to always reference them directly.
- **Custom data models** - Unlike traditional databases, dbzero allows you to define custom data structures to match your domain's needs.

## Requirements

- **Python**: 3.9+
- **Operating Systems**: Linux, macOS, Windows
- **Storage**: Local filesystem or network-attached storage
- **Memory**: Varies by workload; active working set should fit in RAM for best performance

## Quick Start

### Installation

```bash
pip install dbzero
```

### Simple Example

The guiding philosophy behind **dbzero** is *invisibility*—it stays out of your way as much as possible. In most cases, unless you're using advanced features, you won’t even notice it’s there. No schema definitions, no explicit save calls, no ORM configuration. You just write regular Python code, as you always have. See the complete working example below:

```python
import dbzero as db0

@db0.memo(singleton=True)
class GreeterAppRoot:
    def __init__(self, greeting, persons):
        self.greeting = greeting
        self.persons = persons
        self.counter = 0

    def greet(self):
        print(f"{self.greeting}{''.join(f', {person}' for person in self.persons)}!")
        self.counter += 1

if __name__ == "__main__":
    # Initialize dbzero
    db0.init("./app-data", prefix="main")
    # Initialize the application's root object
    root = GreeterAppRoot("Hello", ["Michael", "Jennifer"])
    root.greet() # Output: Hello, Michael, Jennifer!
    print(f"Greeted {root.counter} times.")
```

The application state is persisted automatically; the same data will be available the next time the app starts. All objects are automatically managed by dbzero and there's no need for explicit conversions, fetching, or saving — dbzero handles persistence transparently for the entire object graph.

## Core Concepts

### Memo Classes

Transform any Python class into a persistent, automatically managed object by applying the `@db0.memo` decorator:

```python
import dbzero as db0

@db0.memo
class Person:
    def __init__(self, name: str, age: int):
        self.name = name
        self.age = age

# Instantiation works just like regular Python
person = Person("Alice", 30)

# Attributes can be changed dynamically
person.age += 1
person.address = "123 Main St"  # Add new attributes on the fly
```

### Collections

dbzero provides persistent versions of Python's built-in collections:

```python
from datetime import date

person = Person("John", 25)

# Assign persistent collections to memo object
person.appointment_dates = {date(2026, 1, 12), date(2026, 1, 13), date(2026, 1, 14)}

person.skills = ["Python", "C++", "Docker"]

person.contact_info = {
    "email": "john@example.com",
    "phone": "+1-555-0100",
    "linkedin": "linkedin.com/in/john"
}

# Use them as usual
date(2026, 1, 13) in person.appointment_dates # True

person.skills.append("Kubernetes") 
print(person.skills) # Output: ['Python', 'C++', 'Docker', 'Kubernetes']

person.contact_info["github"] = "github.com/john"
person.contact_info["email"] # "john@example.com"

```

All standard operations are supported, and changes are automatically persisted.

### Queries and Tags

Find objects using tag-based queries and flexible logic operators:

```python
# Create and tag objects
person = Person("Susan", 31)
db0.tags(person).add("employee", "manager")

person = Person("Michael", 29)
db0.tags(person).add("employee", "developer")

# Find every Person by type
result = db0.find(Person)

# Combine type and tags (AND logic) to find employees
employees = db0.find(Person, "employee")

# OR logic using a list to find managers and developers
staff = db0.find(["manager", "developer"])

# NOT logic using db0.no() to find employees wich aren't managers
non_managers = db0.find("employee", db0.no("manager"))
```

### Snapshots and Time Travel

Create isolated views of your data at any point in time:

```python
person = Person("John", 25)
person.balance = 1500
# Keep the current state 
state = db0.get_state_num()
# Commit changes explicitely to advance the state immediately
db0.commit()

# Change the balance
person.balance -= 300
db0.commit()

print(f"{person.name} balance: {person.balance}") # John balance: 1200
# Open snapshot view with past state number
with db0.snapshot(state) as snap:
    past_person = db0.fetch(db0.uuid(person))
    print(f"{past_person.name} balance: {past_person.balance}") # John balance: 1500
```

### Prefixes (Data Partitioning)

Organize data into independent, isolated partitions:

```python
@db0.memo(singleton=True, prefix="/my-org/my-app/settings")
class AppSettings:
    def __init__(self, theme: str):
        self.theme = theme

@db0.memo(prefix="/my-org/my-app/data")
class Note:
    def __init__(self, content: str):
        self.content = content

settings = AppSettings(theme="dark") # Data goes to "settings.db0"
note = Note("Hello dbzero!")         # Data goes to "data.db0"
```

### Indexes

Index your data for fast range queries and sorting:

```python
from datetime import datetime

@db0.memo()
class Event:
    def __init__(self, event_id: int, occured: datetime):
        self.event_id = event_id
        self.occured = occured

events = [
    Event(100, datetime(2026, 1, 28)),
    Event(101, datetime(2026, 1, 30)),
    Event(102, datetime(2026, 1, 29)),
    Event(103, datetime(2026, 2, 1)),
]

# Create an index
event_index = db0.index()
# Populate with objects
for event in events:
    event_index.add(event.occured, event)

# Query events from January 2026
query = event_index.select(datetime(2026, 1, 1), datetime(2026, 1, 31))
# Sort ascending by date of occurance
query_sorted = event_index.sort(query)
print([event.event_id for event in query_sorted]) # Output: [100, 102, 101]

```

## Scalability

dbzero provides tools to build scalable applications:

- **Data Partitioning** - Split data across independent partitions (prefixes) to distribute workload
- **Distributed Transactions** - Coordinate transactions across multiple partitions for data consistency
- **Multi-Process Support** - Multiple processes can work with shared or separate data simultaneously, enabling horizontal scaling

These features give you the flexibility to design distributed architectures that fit your needs.


## Use Cases

Our experience has proven that **dbzero** fits many real-life use cases, which include:

- **Web Applications** - Unified state management for backend services
- **Data Processing Pipelines** - Efficient and simple data preparation
- **Event-Driven Systems** - Capturing data changes and time travel for auditing
- **AI Applications** - Simplified state management for AI agents and workflows
- **Something Else?** - Built something cool with dbzero? We'd love to see what you're working on—share it on our [Discord server](https://discord.gg/9Wn8TAYEPu)!

## Why dbzero?

The short answer is illustrated by diagram below:

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

By eliminating intermediate layers, dbzero reduces complexity, improves performance, and accelerates development—all while providing the reliability and features you expect from a regular database system.

## Documentation

Check our docs to learn more: **[docs.dbzero.io](https://docs.dbzero.io)**

There you can find:
- Guides
- Tutorials
- Performance tips
- API Reference

## License

This project is licensed under the GNU Lesser General Public License v2.1 (LGPL 2.1). See [LICENSE](./LICENSE) for the full text.

- This library can be linked with proprietary software.
- Modifications to the library itself must be released under LGPL 2.1.
- Redistributions must preserve copyright and license notices and provide source.

For attribution details, see [NOTICE](./NOTICE).

## Support

- **Documentation**: [docs.dbzero.io](https://docs.dbzero.io)
- **Email**: info@dbzero.io
- **Issues**: https://github.com/dbzero-software/dbzero/issues

## Feedback

We'd love to hear how you're using dbzero and what features you'd like to see! Your input helps us make dbzero better for everyone.

The best way to share your thoughts is through our **Discord server**: [Join us on Discord](https://discord.gg/9Wn8TAYEPu)

## Commercial Support

Need help building large-scale solutions with dbzero?

We offer:
- Tools for data export and manipulation
- Tools for hosting rich UI applications on top of your existing dbzero codebase
- System integrations
- Expert consulting and architectural reviews
- Performance tuning

Contact us at: **info@dbzero.io**

---

**Start coding as if you have infinite memory. Let dbzero handle the rest.**
