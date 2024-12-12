# Transit Network Gap Directness

Calculating connection score as a measure of transit directness for LRP analysis.

### Environment Setup

To create the virtual python environment, run the next few lines in the VS code teminal (powershell):
(run from the directory where the requirements.txt file lives)
`python -m venv ve`
`ve\Scripts\Activate.ps1`
`pip install pandas`
`pip install sqlalchemy_utils`
`pip install python-dotenv`
`pip install psycopg2`
`pip install pathlib`
`pip install pywin32`

To activate the new environment, run the following line:

`conda activate directness`

To update the environment as changes are needed, run the following line:

`conda env update -f environment.yml`

### Running Scripts

To run any of the scripts in this repo, activate the conda environment, change directory to the project folder, and then run the `python` command followed by the path to the file. For example:

```
conda activate directness
d:
cd D:/dvrpc_shared/NetworkGap_Directness
python /scripts/{script_name}.py
```
