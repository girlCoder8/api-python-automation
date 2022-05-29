# Aixon API Auto Test

---
API Auto Test is a python based project which is triggered by Pytest.

**Install Requirements**

Please install requirements before run tests.

* pip install requirements.txt*

---

### Directory Structure
```
.
├── Jenkinsfile/ 			   # Daily Jenkins job configurations
├── config/                    # Configutations
│   ├── setup.yml     
├── controller/                    # all related model or libs
   └── example_feature/
    └── users.py
├── api_util    # All common utils
│   └── file_operations.py
│   └── db_manager.py
├── settings.py
├── testdata/                   # all test datas related to testcases
   └── example_feature/
    └── test_data.yml
├── testsuite/          		# all testcases
   └── example_feature/
    └── test_get_users.py
├── README.md
├── requirements.txt        	# Pip package requirements for execution 
└── ...
```
----

## Usage

**Step 1:** go to root of the project. (under ./example_feature folder)

**Step 2:** execute command

```
pytest -x --env=STAGING --dataset={specfic_folder}/{test_data} testsuite/{specfic_folder}/{test_suite_name}.py
```

---
