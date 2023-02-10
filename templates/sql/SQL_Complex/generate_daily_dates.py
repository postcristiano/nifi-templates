import datetime, json
from dateutil.relativedelta import relativedelta
import sys

def main():
    dates = []


    start_date = datetime.date(2019, 1, 1)
    current = datetime.date.today()
    # first day of todays month
    end_date = datetime.date(current.year, current.month, 1)

    # generating weeks from start to end
    previous_itteration_date = start_date
    current_itteration_date = previous_itteration_date + relativedelta(days=1)
    while (current_itteration_date  <= end_date):
        dates.append({
            "start": previous_itteration_date.strftime("%Y-%m-%d 00:00:00"),
            "end": current_itteration_date.strftime("%Y-%m-%d 00:00:00")
        })
        previous_itteration_date = current_itteration_date
        current_itteration_date = current_itteration_date + relativedelta(days=1)

    sys.stdout.write(json.dumps(dates))


if __name__ == "__main__":
    main()
