import pandas as pd
from qpython import qconnection
import datetime
import time
import pickle
import os
from twilio.rest import Client
from multiprocessing import Pool

# Bugs
# If realtime crashes this is fucked for the day
# Rvol logic breaks after 3pm
# Need to estimate the days range in prices
# Need to be alerted to which products are near highs/lows
# Need to be alerts to an opening gap in prices

# check bars in a day to verify data integrity, throw out if not
# write test for first and last row

# Performance
# ltime slows this script down 10x (1s to 10s)

# Feature Requests
# Remove outliers
# Write tests
# I think I need to refactor the code for overnite rvol

# Structure
# Workers get price, update state, generate signals


class Vol(object):

    def __init__(self):

        self.bases = ['GC', 'SI', 'HG', 'PA', 'PL', 'LE', 'HE', 'GF', 'CL',
                      'RB', 'HO', 'BRN', 'NG', 'ZB', 'UB', 'ZF', 'ZN', 'ZL',
                      'ZM', 'ZS', 'ZC', 'CT', 'ZW', 'KE', 'MWE', 'ES', 'TF',
                      'NQ', 'RTY', 'EMD', 'YM', 'Z', 'FESX', 'FGBL', 'KC',
                      'SB', 'CC', 'C']

        Vol.init_rvol(self)

    def q(query=None):
        """connect to kdb"""

        q = qconnection.QConnection('kdb.genevatrading.com', 8000,
                                    pandas=True)
        q.open()

        if query is None:

            return q

        else:

            return q(query)

    def rdb(query=None):
        """Connect to rdb"""

        rdb = qconnection.QConnection('kdb.genevatrading.com', 9218,
                                      pandas=True)
        rdb.open()

        if query is None:

            return rdb

        else:

            return rdb(query)

    def init_rvol(self):

        today = datetime.datetime.now()
        yday = today - datetime.timedelta(days=1)

        now = today.time()

        now = (now.replace(minute=(5 * (now.minute // 5)))
               .strftime('%H:%M'))

        _ = pd.date_range(end=yday, periods=20, freq='B')
        start = _.min().strftime('%Y.%m.%d')
        stop = _.max().strftime('%Y.%m.%d')

        filepath = 'rvol.pickle'

        df = dict()

        if os.path.exists(filepath):

            get_file_date = time.ctime(os.path.getctime(filepath))

            file_date = datetime.datetime.strptime(
                get_file_date, "%a %b %d %H:%M:%S %Y").date()

            if file_date == today.date():

                return

        else:

            for i in self.bases:

                df[i] = Vol.q('select sum volume by 0D00:05:00 xbar'
                              ' ltime utc_datetime '
                              'from trade where date within ({}; {}),'
                              'base = `$"{}", not sym like "*-*", '
                              '((`time$(ltime utc_datetime)) within '
                              '((`time$07:00:00); (`time$15:00:00)))'
                              .format(start, stop, i))

                print('Initializing Rvol:', i)

        with open(filepath, 'wb') as f:
            pickle.dump(df, f)

    def prnt_rvol(self):

        # Single threaded

        now = datetime.datetime.now().time()

        now = (now.replace(minute=(5 * (now.minute // 5)))
               .strftime('%H:%M'))

        Rvol = dict()
        Rvol20d = dict()

        filepath = 'rvol.pickle'

        with open(filepath, 'rb') as f:
            df = pickle.load(f)

        # Calculate

        for i in self.bases:

            _ = Vol.rdb('select sum volume by `date$utc_datetime'
                        ' from bar where (`date$utc_datetime)'
                        ' = (`date$.z.z),'
                        ' base = `$"{}", not sym like "*-*", '
                        '((`time$(ltime utc_datetime)) within (('
                        '`time$07:00:00); (`time${}:00)))'
                        .format(i, now))

            # Should be 1 number, changed from .sum()

            try:

                tday_cum = _['volume'].item()

            except ValueError:

                print(i, 'ValueError')

            cut = df[i].loc[df[i].index.strftime('%H:%M') < now]

            cumsum = cut.groupby(pd.Grouper(freq='D'))['volume'].sum()

            cumsum = cumsum[cumsum != 0]

            try:

                yday_vol = cumsum[-1]

            except KeyError:

                print(i, 'KeyError')

            except IndexError:

                print(i, 'IndexError')

            hist_mean = cumsum.mean()

            Rvol[i] = round((tday_cum / yday_vol), 2)
            Rvol20d[i] = round((tday_cum / hist_mean), 2)

        Rvol_sort = sorted(Rvol, key=Rvol.get)
        Rvol20d_sort = sorted(Rvol20d, key=Rvol20d.get)

        print('Rvol:')
        for i in Rvol_sort:
            print(i, Rvol[i])

        print('')

        print('Rvol20:')
        for i in Rvol20d_sort:
            print(i, Rvol20d[i])

    def upd_rvol(base):

        # Multi threaded

        now = datetime.datetime.now().time()

        now = (now.replace(minute=(5 * (now.minute // 5)))
               .strftime('%H:%M'))

        filepath = 'rvol.pickle'

        with open(filepath, 'rb') as f:
            df = pickle.load(f)

        # Calculate

        _ = Vol.rdb('select sum volume by `date$utc_datetime'
                    ' from bar where (`date$utc_datetime)'
                    ' = (`date$.z.z),'
                    ' base = `$"{}", not sym like "*-*", '
                    '((`time$(ltime utc_datetime)) within (('
                    '`time$07:00:00); (`time${}:00)))'
                    .format(base, now))

        tday_cum = _['volume'].sum()

        cut = df[base].loc[df[base].index.strftime('%H:%M') < now]

        cumsum = cut.groupby(pd.Grouper(freq='D'))['volume'].sum()

        cumsum = cumsum[cumsum != 0]

        try:

            yday_vol = cumsum[-1]

        except KeyError:

            pass

        except IndexError:

            pass

        hist_mean = cumsum.mean()

        rvol = round(tday_cum / yday_vol, 2)
        rvol20d = round(tday_cum / hist_mean, 2)

        return rvol, rvol20d

    def compare(self, base=None):

        # Not finished

        today = datetime.datetime.now()
        yday = today - datetime.timedelta(days=1)

        _ = pd.date_range(end=yday, periods=1, freq='B')
        _ = ''.join(_.strftime('%Y.%m.%d'))

        now = datetime.datetime.now().time()
        now = (now.replace(minute=(5 * (now.minute // 5)))
               .strftime('%H:%M'))

        new = Vol.rdb('-1# select sum volume by 0D00:05:00 xbar'
                      ' utc_datetime from bar where (`date$utc_datetime)'
                      ' = (`date$.z.z), base = `$"{}"'.format(base))

        old = Vol.q('-1# select sum volume by 0D00:05:00 xbar'
                    ' utc_datetime from trade where date = {},'
                    ' (`time$utc_datetime) < (`time$.z.z),'
                    ' base = `$"{}"'
                    .format(_, base))

        print(new)
        print(old)

    def get_front_months(self):
        """This code needs to be run once everyday at 5pm"""

        # Get front month based on yesterday EOD volume

        now = datetime.datetime.now().date()

        yday = (now - datetime.timedelta(days=1))

        yday_str = yday.strftime('%Y.%m.%d')

        basesyms = dict()

        sym_errors = set()

        count = 1

        for i in self.bases:

            _ = Vol.q('select date, sym from dailybar where date = {},'
                      'base = `{}, volume = max volume, not sym like '
                      '"*-*"'.format(yday_str, i))

            while _.empty:

                temp = ((yday - datetime.timedelta(days=count))
                        .strftime('%Y.%m.%d'))

                count += 1

                _ = Vol.q('select date, sym from dailybar where date = {},'
                          'base = `{}, volume = max volume, not sym like '
                          '"*-*"'.format(temp, i))

                # Try a max of 5 days

                if count > 5 and _.empty:

                    sym_errors.add(i)

                    break

                elif count > 5:

                    break

                else:

                    pass

            # Reset count for next sym

            count = 1

            # Add sym to set

            if not _.empty:

                _.set_index('date', inplace=True)

                _['sym'] = _['sym'].apply(lambda x: x.decode('utf-8'))

                basesyms[i] = _['sym'][0]

            else:

                pass

        if sym_errors:

            print('Could not find syms for:', sym_errors)

        print('Symbols found:', basesyms)

        return basesyms

    def yday_ohlc(sym):

        # assumes last row in dailybar is last trade day

        _ = Vol.q('-1# select date, open, high, low, close'
                  ' from dailybar where sym = `$"{}"'
                  .format(sym))

        yo = _['open'].item()
        yh = _['high'].item()
        yl = _['low'].item()
        yc = _['close'].item()

        return yo, yh, yl, yc

    def upd_price(sym):

        return Vol.rdb('-1# select close from bar where sym ='
                       '`$"{}"'.format(sym))['close'].item()

    def test_rdb():

        df = Vol.rdb('-1# select close by ltime utc_datetime from bar'
                     ' where base = `ES')

        # Test updates for real time

        assert df.index.date[0] == datetime.datetime.now().date()
        assert df.index.time[0].hour == datetime.datetime.now().time().hour
        assert df.index.time[0].minute == datetime.datetime.now().time().minute

        # Write tests for historical

    def start(self):

        # Run tests

        Vol.test_rdb()

        # Get front months

        basesyms = Vol.get_front_months(self)

        x = list()

        for k, v in basesyms.items():
            x.append({k: v})

        # Multiprocessing

        p = Pool(len(x))
        p.map(Vol.workers, x)
        p.close()
        p.join()

    def send_sms(sym, body):

        # Send Text

        account_sid = "ACcabf56d5753722516ce85f436d8cd668"
        auth_token = "fbd514353b7b6f1cb08782d431f26b24"

        twilio = Client(account_sid, auth_token)

        numbers = ['17025739865']

        # , '13157061068', '16302025448'

        for i in numbers:
            twilio.api.account.messages.create(
                to=i,
                from_="+17027106358",
                body="{}, {}, {}".format(
                    datetime.datetime.now().time(), sym, body))

    def workers(basesym):

        # Probably need to check exchange status somewhere

        base = str(*basesym)
        sym = basesym[base]

        yo, yh, yl, yc = Vol.yday_ohlc(sym)

        # Initialize state

        test_yh = 0
        test_yl = 0

        def upd_test_yh(test_yh, price, yh, **kwargs):

            if test_yh is 0 and price > yh:

                test_yh = 1

                dispatcher.pop(upd_test_yh)

        def upd_test_yl(test_yl, price, yl, **kwargs):

            if test_yl is 0 and price < yl:

                test_yl = 1

                dispatcher.pop(upd_test_yl)

        def single_outside_rvol(rvol, test_yh, test_yl, **kwargs):

            if (rvol > 1.4) and (test_yh or test_yl):

                body = 'single_outside_rvol'

                Vol.send_sms(sym, body)

                dispatcher.pop(single_outside_rvol)

        def double_outside_rvol(rvol, test_yh, test_yl, **kwargs):

            if (rvol > 1.4) and (test_yh + test_yl == 2):

                body = 'double_outside_rvol'

                Vol.send_sms(sym, body)

                dispatcher.pop(double_outside_rvol)

        print('Monitoring:', sym)

        # Main loop

        dispatcher = [upd_test_yh,
                      upd_test_yl,
                      single_outside_rvol,
                      double_outside_rvol]

        while True:

            # Update state

            price = Vol.upd_price(sym)
            rvol, rvol20d = Vol.upd_rvol(base)

            # Generate alerts

            for i in dispatcher:
                i(**locals())

            time.sleep(301)


if __name__ == '__main__':

    ex = Vol()
    ex.start()
