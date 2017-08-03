import pandas as pd
import datetime as dt


def years_ago(years, the_date):
    try:
        return the_date.replace(year=the_date.year - years)
    except ValueError:
        return the_date.replace(month=2, day=28, year=the_date.year - years)


def test_specific_stock(symbol):
    df = pd.read_csv('./stock_prices/nyse_{}.csv'.format(symbol))
    df.columns = [x.lower().replace(' ', '_') for x in df.columns]
    df['date'] = pd.to_datetime(df['date'])
    return test_stock(df, today_date=dt.datetime(2016, 11, 1, 0, 0, 0))
    # return test_stock(df)


def test_stock(df_orig, today_date=dt.datetime.now()):
    today_date = today_date.date()
    df = df_orig.copy()

    # tests older than two years
    older_than_two_years = df['date'].min().year > 2

    one_year_ago = years_ago(1, today_date)
    two_years_ago = years_ago(2, today_date)
    date_latest = df['date'].max()
    date_1_year_ago = df.loc[df['date'] < one_year_ago, 'date'].max()
    date_2_years_ago = df.loc[df['date'] < two_years_ago, 'date'].max()
    # test that change is greater than 100% present year
    present_year_mask = (df['date'] >= date_1_year_ago) & (df['date'] <= date_latest)
    present_year_variance = (df.loc[present_year_mask, 'high'].max() / df.loc[present_year_mask, 'high'].min()) - 1
    present_year_one_hundred_percent = present_year_variance > 1

    # test that change is greater than 100$ for the year previous to the present year
    previous_year_mask = (df['date'] >= date_2_years_ago) & (df['date'] <= date_1_year_ago)
    previous_year_variance = (df.loc[previous_year_mask, 'high'].max() / df.loc[previous_year_mask, 'high'].min()) - 1
    previous_year_one_hundred_percent = previous_year_variance > 1

    variance_over_two_years = (df.loc[df['date'] >= date_2_years_ago, 'high'].max() / df.loc[df['date'] >= date_2_years_ago, 'high'].min()) - 1

    six_months_mask = df['date'] > today_date - dt.timedelta(days=30 * 6)
    flat_value = 8
    max_last_six_months = df.loc[six_months_mask, 'high'].max()
    stock_not_flat = max_last_six_months > flat_value

    return {'age': {'result': older_than_two_years, 'test_message': 'Older than 2 Years'},
            'present_year_variance': {'result': present_year_one_hundred_percent,
                                      'test_message': 'Variance in the present year is greater than 100 percent',
                                      'actual_variance': present_year_variance},
            'previous_year_variance': {'result': previous_year_one_hundred_percent,
                                       'test_message': 'Variance in the previous year is greater than 100 percent',
                                       'actual_variance': previous_year_variance},
            'variance_over_2_years': variance_over_two_years,
            'stock_not_flat': {'result': stock_not_flat,
                               'test_message': 'Test that the stock has not gone flat in last six monthe, Greater than {}'.format(flat_value),
                               'actual_value': max_last_six_months}
            }

