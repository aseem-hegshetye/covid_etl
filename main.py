import getpass
import os

from crontab import CronTab


def schedule_cronjob():
    """
    schedule covid_etl to run at 9 am daily
    """
    etl_file = 'load_covid_data.py'
    covid_etl_filename = f'{os.getcwd()}/{etl_file}'
    python_path = f'{os.getcwd()}/myvenv/bin/python'

    cron = CronTab(user=getpass.getuser())
    cron.remove_all()
    cron_command = f'{python_path} {covid_etl_filename}'
    job = cron.new(command=cron_command)
    job.hour.on(9)
    job.minute.on(0)
    cron.write()


if __name__ == '__main__':
    schedule_cronjob()
