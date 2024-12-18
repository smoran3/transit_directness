# Transit Network Gap Directness

Calculating connection score as a measure of transit directness for LRP analysis.

### Environment Setup
Copy `VisumPy` folder and contents from model folder to scripts folder

To create the virtual python environment, run the next few lines in the VS code teminal (powershell):
(run from the directory where the requirements.txt file lives)
`python -m venv ve`
`ve\scripts\Activate.ps1`
`pip install pandas`
`pip install sqlalchemy_utils`
`pip install python-dotenv`
`pip install psycopg2`
`pip install pathlib`
`pip install pywin32`
`pip install jupyter notebook`
`pip install ipykernel -U --force-reinstall`

### Troubleshooting
If pip installing displays fatal error, try:
`python -m pip install -U pip`

### Running Scripts

To run any of the scripts in this repo, activate the virtual environment, change directory to the project folder, and then run the `python` command followed by the path to the file. For example:

```
ve\scripts\Activate.ps1
d:
cd D:/MODELING/transit_directness
python D:\MODELING\transit_directness\transit_directness\scripts\{script_name}.py
```
