# Student Directory Generator
Module which generates student directory data

---
## Features
Matches student and teacher data to generate a student directory which includes assigned class instructors.


## Technologies
[Python 3.8.12](https://www.python.org/downloads/release/python-3812/)
[Dask](https://dask.org/)

## Getting Started
### Prerequisites
- [Python 3.8.12](https://www.python.org/downloads/release/python-3812/)

### Installation
1. Clone this repo to your local machine from `https://github.com/wmemorgan/45432t43pojr`
2. Install [Python 3.8.12](https://www.python.org/downloads/release/python-3812/)
3. Open directory **source\studendsapp** in command line
4. Execute `python setup.py` to install the app

### Usage 
1. Open directory **source\studendsapp** in command line
2. Execute `python __main__.py` to run the program.

## Output
Script creates a file in the **data** directory named `students.json`.


## Documentation
The JSON file contains the following data fields:
| Field | Type |
| --- | --- |
| **id** | integer |
| **firstName** | string |
| **lastName** | string |
| **email** | string |
| **ssn** | string |
| **address** | string |
| **classId** | string | 
| **teacher** | object |


### Teacher data fields:
| Field | Type |
| --- | --- |
| **id** | integer |
| **firstName** | string |
| **lastName** | string |

## License
[MIT]()