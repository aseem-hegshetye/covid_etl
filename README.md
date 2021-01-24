### Python ETL to load covid data per county in NY in a local sqlite3 db.
**API** = https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD

Clone this project to your local directory.
cd into covid_etl directory.

Install sqlite3 db on your machine 
<a href='https://linuxhint.com/install_sqlite_browser_ubuntu_1804/'>Link</a>

Create 2 new sqlite3 db named **covid** & **covid_test** 
inside covid_etl directory. 

Create a python virtual env and install all dependencies:
(make sure you have python3.8 installed on your system)

```
python3 -m venv myvenv
source myvenv/bin/activate
pip install -r requirements.txt
```

Now run main.py from your myvenv which will schedule a 
cronjob that will run covid_etl process
every day at 9 AM. New data will be appended to 
existing data in you county table. If table doesnt
exist then a new table gets created.

`python main.py`

#### Test:

`python -m unittest discover`