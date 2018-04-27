"""docstring"""

import random
import datetime
import time
import pickle
import os
from multiprocessing import Pool
from twilio.rest import Client
import pandas as pd
from qpython import qconnection

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
    """docstring"""

    def __init__(self):

        self.bases = ['GC', 'SI', 'HG', 'PA', 'PL', 'LE', 'HE', 'GF', 'CL',
                      'RB', 'HO', 'BRN', 'NG', 'ZB', 'UB', 'ZF', 'ZN', 'ZL',
                      'ZM', 'ZS', 'ZC', 'CT', 'ZW', 'KE', 'MWE', 'ES', 'TF',
                      'NQ', 'RTY', 'EMD', 'YM', 'Z', 'FESX', 'FGBL', 'KC',
                      'SB', 'CC', 'C']

        Vol.init_rvol(self)

        with open('rvol.pickle', 'rb') as file:
            self.kdb_data = pickle.load(file)

    @classmethod
    def kdb(cls, query=None):
        """connect to kdb"""

        kdb = qconnection.QConnection('kdb.genevatrading.com', 8000,
                                      pandas=True)
        kdb.open()

        if query is None:

            return kdb

        return kdb(query)

    @classmethod
    def rdb(cls, query=None):
        """Connect to rdb"""

        rdb = qconnection.QConnection('kdb.genevatrading.com', 9218,
                                      pandas=True)
        rdb.open()

        if query is None:

            return rdb

        return rdb(query)

    def init_rvol(self):
        """docstring"""

        today = datetime.datetime.now()
        yday = today - datetime.timedelta(days=1)

        now = today.time()

        now = (now.replace(minute=(5 * (now.minute // 5)))
               .strftime('%H:%M'))

        _ = pd.date_range(end=yday, periods=20, freq='B')
        start = _.min().strftime('%Y.%m.%d')
        stop = _.max().strftime('%Y.%m.%d')

        filepath = 'rvol.pickle'

        kdb_data = dict()

        if os.path.exists(filepath):

            get_file_date = time.ctime(os.path.getctime(filepath))

            file_date = datetime.datetime.strptime(
                get_file_date, "%a %b %d %H:%M:%S %Y").date()

        if not os.path.exists(filepath) or file_date != today.date():

            for i in self.bases:

                kdb_data[i] = Vol.kdb('select sum volume by 0D00:05:00 xbar'
                                      ' ltime utc_datetime '
                                      'from trade where date within ({}; {}),'
                                      'base = `$"{}", not sym like "*-*", '
                                      '((`time$(ltime utc_datetime)) within '
                                      '((`time$07:00:00); (`time$15:00:00)))'
                                      .format(start, stop, i))

                print('Initializing Rvol:', i)

        with open(filepath, 'wb') as file:
            pickle.dump(kdb_data, file)

    def prnt_rvol(self):
        """docstring"""

        # Single threaded

        now = datetime.datetime.now().time()

        now = (now.replace(minute=(5 * (now.minute // 5)))
               .strftime('%H:%M'))

        rvol = dict()
        rvol20d = dict()

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

            cut = self.kdb_data[i].loc[
                self.kdb_data[i].index.strftime('%H:%M') < now]

            cumsum = cut.groupby(pd.Grouper(freq='D'))['volume'].sum()

            cumsum = cumsum[cumsum != 0]

            try:

                yday_vol = cumsum[-1]

            except KeyError:

                print(i, 'KeyError')

            except IndexError:

                print(i, 'IndexError')

            hist_mean = cumsum.mean()

            rvol[i] = round((tday_cum / yday_vol), 2)
            rvol20d[i] = round((tday_cum / hist_mean), 2)

        rvol_sort = sorted(rvol, key=rvol.get)
        rvol20d_sort = sorted(rvol20d, key=rvol20d.get)

        print('Rvol:')
        for i in rvol_sort:
            print(i, rvol[i])

        print('')

        print('Rvol20:')
        for i in rvol20d_sort:
            print(i, rvol20d[i])

    @classmethod
    def upd_rvol(cls, base, kdb_data):
        '''docstring'''

        # Multi threaded

        now = datetime.datetime.now().time()

        now = (now.replace(minute=(5 * (now.minute // 5)))
               .strftime('%H:%M'))

        # Calculate

        _ = Vol.rdb('select sum volume by `date$utc_datetime'
                    ' from bar where (`date$utc_datetime)'
                    ' = (`date$.z.z),'
                    ' base = `$"{}", not sym like "*-*", '
                    '((`time$(ltime utc_datetime)) within (('
                    '`time$07:00:00); (`time${}:00)))'
                    .format(base, now))

        tday_cum = _['volume'].sum()

        cut = kdb_data.loc[kdb_data.index.strftime('%H:%M') < now]

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

    @classmethod
    def rvol_now(cls, base=None):
        """I have an idea to make this a real time Rvol (noncum)
        What I want is to see the 20d 15m bars vs today"""

        yday = datetime.datetime.now() - datetime.timedelta(days=1)

        _ = pd.date_range(end=yday, periods=20, freq='B')

        start = _.min().strftime('%Y.%m.%d')
        stop = _.max().strftime('%Y.%m.%d')

        while True:

            tday_15m = Vol.rdb('-1# select sum volume from bar where '
                               '(`date$utc_datetime) = (`date$.z.z), '
                               'base = `$"{}", not sym like "*-*",'
                               ' (`time$utc_datetime) < (`time$.z.z),'
                               ' (`time$utc_datetime) > ((`time$.z.z) '
                               '- 00:15:00)'
                               .format(base))

            tday_15m = tday_15m['volume'].item()

            avg_15m_20d = Vol.kdb('select sum volume by date'
                                  ' from trade where date within ({};{}), '
                                  'base=`$"{}", not sym like "*-*", '
                                  '(`time$utc_datetime) < (`time$.z.z'
                                  ') ,(`time$utc_datetime) > ((`time$.z.z) -'
                                  ' 00:15:00)'
                                  .format(start, stop, base))

            avg_15m_20d = int(avg_15m_20d.mean())

            rvol_now = round(tday_15m / avg_15m_20d, 1)

            print('rvol_now:', base, rvol_now, end='\r')

            time.sleep(3)

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

            _ = Vol.kdb('select date, sym from dailybar where date = {},'
                        'base = `{}, volume = max volume, not sym like '
                        '"*-*"'.format(yday_str, i))

            while _.empty:

                temp = ((yday - datetime.timedelta(days=count))
                        .strftime('%Y.%m.%d'))

                count += 1

                _ = Vol.kdb('select date, sym from dailybar where date = {},'
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

    @classmethod
    def yday_ohlc(cls, sym):
        """docstring"""

        # assumes last row in dailybar is last trade day

        _ = Vol.kdb('-1# select date, open, high, low, close'
                    ' from dailybar where sym = `$"{}"'
                    .format(sym))

        yday_ohlc = dict()

        yday_ohlc['yo'] = _['open'].item()
        yday_ohlc['yh'] = _['high'].item()
        yday_ohlc['yl'] = _['low'].item()
        yday_ohlc['yc'] = _['close'].item()

        return yday_ohlc

    @classmethod
    def upd_price(cls, sym):
        """docstring"""

        return Vol.rdb('-1# select close from bar where sym ='
                       '`$"{}"'.format(sym))['close'].item()

    @classmethod
    def test_rdb(cls):
        """docstring"""

        _ = Vol.rdb('-1# select close by ltime utc_datetime from bar'
                    ' where base = `ES')

        # Test updates for real time

        assert _.index.date[0] == datetime.datetime.now().date()
        assert _.index.time[0].hour == datetime.datetime.now().time().hour
        assert _.index.time[0].minute == datetime.datetime.now().time().minute

        # Write tests for historical

    def start(self):
        """docstring"""

        # Run tests

        Vol.test_rdb()

        # Get front months

        basesyms = Vol.get_front_months(self)

        wrkr_args = list()

        for key, value in basesyms.items():
            wrkr_args.append({'base': key, 'sym': value})

        for i in wrkr_args:
            i['kdb_data'] = self.kdb_data[i['base']]

        # Multiprocessing

        pool = Pool(len(wrkr_args))
        pool.map(Vol.workers, wrkr_args)
        pool.close()
        pool.join()

    @classmethod
    def send_sms(cls, sym, body):
        """docstring"""

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

    @classmethod
    def workers(cls, wrkr_args):
        """docstring"""

        # Probably need to check exchange status somewhere

        yday_ohlc = Vol.yday_ohlc(wrkr_args['sym'])

        # Initialize state

        test_yh = 0
        test_yl = 0

        def upd_test_yh():
            """docstring"""

            if price > yday_ohlc['yh']:

                nonlocal test_yh
                test_yh = 1

                dispatcher.remove(upd_test_yh)

        def upd_test_yl():
            """docstring"""

            if price < yday_ohlc['yl']:

                nonlocal test_yl
                test_yl = 1

                dispatcher.remove(upd_test_yl)

        def single_outside_rvol20d():
            """docstring"""

            if (rvol20d > 1.0) and (test_yh or test_yl):

                body = 'single_outside_rvol'

                # Vol.send_sms(sym, body)

                print(wrkr_args['sym'], body)

                dispatcher.remove(single_outside_rvol20d)

        def double_outside_rvol20d():
            """docstring"""

            if (rvol20d > 1.0) and (test_yh + test_yl == 2):

                body = 'double_outside_rvol'

                # Vol.send_sms(sym, body)

                print(wrkr_args['sym'], body)

                dispatcher.remove(double_outside_rvol20d)

        print('Monitoring:', wrkr_args['sym'])

        # Main loop

        dispatcher = [upd_test_yh,
                      upd_test_yl,
                      single_outside_rvol20d,
                      double_outside_rvol20d]

        while True:

            # Update state

            price = Vol.upd_price(wrkr_args['sym'])
            rvol, rvol20d = Vol.upd_rvol(
                wrkr_args['base'], wrkr_args['kdb_data'])

            # Generate alerts

            for i in dispatcher:
                i()

            time.sleep(random.randint(5, 25))


if __name__ == '__main__':

    ex = Vol()
    ex.rvol_now(base='ES')
