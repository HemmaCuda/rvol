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

# What will make me the most money
# Multiple Rvols?
# Being able to pass command line args
# Sorting bases into sectors
# Let's see if I can get multiple Rvols done


class Vol(object):
    """docstring"""

    def __init__(self):

        self.bases = ['GC', 'SI', 'HG', 'PA', 'PL', 'LE', 'HE', 'GF', 'CL',
                      'RB', 'HO', 'BRN', 'NG', 'ZB', 'UB', 'ZF', 'ZN', 'ZL',
                      'ZM', 'ZS', 'ZC', 'CT', 'ZW', 'KE', 'MWE', 'ES', 'TF',
                      'NQ', 'RTY', 'EMD', 'YM', 'Z', 'FESX', 'FGBL', 'KC',
                      'SB', 'CC', 'C']

        self.sectors = {"Metals": ["GC", "SI", "HG", "PL", "PA"],
                        "Meats": ["LE", "HE", "GF"],
                        "Energy": ["CL", "RB", "HO", "BRN", "NG"],
                        "Grains": ["ZC", "ZW", "ZS", "ZM", "ZL", "KE", "MWE"],
                        "Bonds": ["ZN", "ZF", "ZB", "UB", "FGBL"],
                        "Softs": ["SB", "CT", "KC", "CC", "C"],
                        "Equities": ["ES", "NQ", "FESX", "RTY", "YM", "EMD",
                                     "Z"]}

        self.kdb_data = Vol.init_rvol(self)

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

    @classmethod
    def rvol_time(cls):
        """Returns the desired rvol session"""

        now = datetime.datetime.now().time()

        if datetime.time(6, 0) <= now <= datetime.time(10, 29, 59):

            return '06:00; 10:29'

        elif datetime.time(10, 30) <= now <= datetime.time(16, 59, 59):

            return '10:30; 16:59'

        elif datetime.time(17, 0) <= now <= datetime.time(23, 59, 59):

            return '17:00; 23:59'

        return '24:00; 05:59'

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

        rvol_time = Vol.rvol_time()

        if os.path.exists(filepath):

            get_file_date = time.ctime(os.path.getctime(filepath))

            file_datetime = datetime.datetime.strptime(
                get_file_date, "%a %b %d %H:%M:%S %Y")

        if (not os.path.exists(filepath) or
                file_datetime.date() != today.date() or not
                (rvol_time[0:5] < file_datetime.time().strftime('%H:%M')
                 < rvol_time[8:13])):

            for i in self.bases:

                kdb_data[i] = Vol.kdb('t: select sum volume by 0D00:05:00 xbar'
                                      ' utc_datetime, date '
                                      'from trade where date within ({}; {}), '
                                      'base = `$"{}", not sym like "*-*", '
                                      'null trade_type; '
                                      'select from t where (ltime utc_datetime'
                                      ') within ({})'
                                      .format(start, stop, i, rvol_time))

                print('Initializing Rvol:', i)

            with open(filepath, 'wb') as file:
                pickle.dump(kdb_data, file)

        else:

            with open('rvol.pickle', 'rb') as file:
                kdb_data = pickle.load(file)

        return kdb_data

    @classmethod
    def nearest_15m_vol(cls, base):
        '''Volume in nearest Noncontinuous 15m bar'''

        now = datetime.datetime.now().time()

        now = (now.replace(minute=(5 * (now.minute // 5)))
               .strftime('%H:%M'))

        rvol_time = Vol.rvol_time()[0:5]

        data = Vol.rdb('select sum volume by `date$utc_datetime'
                       ' from bar where (`date$utc_datetime)'
                       ' = (`date$.z.z),'
                       ' base = `$"{}", not sym like "*-*", '
                       '((`time$(ltime utc_datetime)) within (('
                       '`time${}:00); (`time${}:00)))'
                       .format(base, rvol_time, now))
        try:

            return data['volume'].item()

        except ValueError:

            return 0

    def nearest_15m_20d_vol_avg(self, base):
        '''docstring'''

        # yday_vol code, requires ValueError exception
        # yday_vol = datesum['volume'].loc[datesum.idxmax()].item()

        utc = datetime.datetime.utcnow().time().strftime('%H:%M')

        cut = self.kdb_data[base].loc[
            self.kdb_data[base].index.levels[0].strftime('%H:%M') < utc]

        return cut.groupby(by='date').sum().mean().item()

    def rvol_20d(self, base):
        """Calculates the 20d Rvol to the nearest 5 minute bar"""

        today = Vol.nearest_15m_vol(base)

        avg_20d = Vol.nearest_15m_20d_vol_avg(self, base)

        return round((today / avg_20d), 2)

    @classmethod
    def last_15m_vol(cls, base):
        '''Volume in continuous previous 15 minutes'''

        td_15m = Vol.rdb('-1# select sum volume from bar where '
                         '(`date$utc_datetime) = (`date$.z.z), '
                         'base = `$"{}", not sym like "*-*",'
                         ' (`time$utc_datetime) < (`time$.z.z),'
                         ' (`time$utc_datetime) > ((`time$.z.z) '
                         '- 00:15:00)'
                         .format(base))

        return td_15m['volume'].item()

    @classmethod
    def last_20d_15m_vol(cls, base):
        '''Mean volume in continuous previous 15 minutes for last 20 days'''

        first, last = Vol.date_rn_last_20d()

        data = Vol.kdb('select sum volume by date'
                       ' from trade where date within ({}'
                       ';{}), base=`$"{}", not sym like'
                       ' "*-*", (`time$utc_datetime) < '
                       '(`time$.z.z) ,(`time$utc_datetime'
                       ') > ((`time$.z.z) - 00:15:00), null trade_type'
                       .format(first, last, base))

        try:

            return int(data.mean())

        except ValueError:

            return 0

    @classmethod
    def date_rn_last_20d(cls):
        '''Returns first and last date for the previous 20 business days'''

        yday = datetime.datetime.now() - datetime.timedelta(days=1)

        _ = pd.date_range(end=yday, periods=20, freq='B')

        first_date = _.min().strftime('%Y.%m.%d')
        last_date = _.max().strftime('%Y.%m.%d')

        return first_date, last_date

    @classmethod
    def rvol_now(cls, base):
        """Rolling 15m volume vs average of last 20 days"""

        td_15m = Vol.last_15m_vol(base)

        avg_15m_20d = Vol.last_20d_15m_vol(base)

        try:

            return round(td_15m / avg_15m_20d, 2)

        except (ValueError, ZeroDivisionError):

            return 0

    def main(self):
        '''Returns a dict of base : rvol_now pairs'''

        rvol_now = dict()
        rvol_20d = dict()

        for base in self.bases:

            rvol_now[base] = Vol.rvol_now(base)
            rvol_20d[base] = Vol.rvol_20d(self, base)

        Vol.print_rvol(self, rvol_now, rvol_20d)

    def print_rvol(self, rvol_now, rvol_20d):
        '''Input is a dict with base : rvol pairs, output to terminal'''
        
        # set num_rows to length of the largest sector and initialize the output_buffer 
        num_rows = 2 * max([len(self.sectors.values())])
        output_buffer = [""] * num_rows
        column_names = ""
        COLUMN_WIDTH = 20
        INTENSITY_FACTOR = 1.3      
                
        for key in self.sectors:

            column_names += key + ' ' * (COLUMN_WIDTH - len(key))

            for i in range(max([len(self.sectors.values())])):
                
                t1 = ""
                t2 = ""

                # append each key value pair to the corresponding row
                if i < len(self.sectors[key]):

                    symbol = self.sectors[key][i]
                    rvol_intensity = int(rvol_now[symbol] // INTENSITY_FACTOR)

                    t1 = symbol + ' ' * ((COLUMN_WIDTH // 2) - len(symbol) - rvol_intensity) + ('*' * rvol_intensity) + str(rvol_now[symbol])
                    t2 = (COLUMN_WIDTH // 2) * ' ' + str(rvol_20d[symbol])

                # Fill whitespace to align columns   
                t1 += ' ' * (COLUMN_WIDTH - len(t1))
                t2 += ' ' * (COLUMN_WIDTH - len(t2))

                output_buffer[i*2] += t1
                output_buffer[(i*2)+1] += t2
     
        # format column names for Top X Now and Session
        t3 = "Top " + str(num_rows) + " Now"
        t3 += ' ' * (COLUMN_WIDTH - len(t3))
        column_names += t3

        t3 = "Top " + str(num_rows) + " Session"
        t3 += ' ' * (COLUMN_WIDTH - len(t3))
        column_names += t3

        # get sorted list of bases from rvol_now
        rvol_sort = sorted(rvol_now, key=rvol_now.get, reverse = True)

        # Top X Now - format each line and add to the output buffer
        for i in range (0, num_rows):
            
            symbol = rvol_sort[i]
            rvol_intensity = int(rvol_now[symbol] // INTENSITY_FACTOR)

            temp = rvol_sort[i] + ' ' * ((COLUMN_WIDTH // 2) - len(symbol) - rvol_intensity) + '*' * rvol_intensity + str(rvol_now[symbol])
            temp += ' ' * (COLUMN_WIDTH - len(temp))
            
            output_buffer[i] += temp

        # get sorted list of bases from rvol_20d
        rvol_sort = sorted(rvol_20d, key=rvol_20d.get, reverse = True)

        # Top X Session - format each line and add to the output buffer
        for i in range (0, num_rows):
            
            symbol = rvol_sort[i]
            rvol_intensity = int(rvol_20d[symbol] // INTENSITY_FACTOR)

            temp = rvol_sort[i] + ' ' * ((COLUMN_WIDTH // 2) - len(symbol) - rvol_intensity) + '*' * rvol_intensity + str(rvol_20d[symbol])
            temp += ' ' * (COLUMN_WIDTH - len(temp))

            output_buffer[i] += temp

        # clear screen and print everything to screen
        print('\n'*50)

        print (datetime.datetime.now().time())
        print (column_names)
        for line in output_buffer:
            print (line)


class Market(object):
    '''Generic base/sym data'''

    def __init__(self):

        self.bases = ['GC', 'SI', 'HG', 'PA', 'PL', 'LE', 'HE', 'GF', 'CL',
                      'RB', 'HO', 'BRN', 'NG', 'ZB', 'UB', 'ZF', 'ZN', 'ZL',
                      'ZM', 'ZS', 'ZC', 'CT', 'ZW', 'KE', 'MWE', 'ES', 'TF',
                      'NQ', 'RTY', 'EMD', 'YM', 'Z', 'FESX', 'FGBL', 'KC',
                      'SB', 'CC', 'C']

        self.front_months = Market.get_front_months(self)

        self.yday_ohlc = dict()

        for sym in self.front_months.items():

            self.yday_ohlc[sym] = Market.get_yday_ohlc(sym)

    def get_front_months(self):
        """Returns a dict of base : syms,
            Selects front months based on yesterday EOD volume"""

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
    def get_yday_ohlc(cls, sym):
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


class Alert(object):
    '''Multithreaded sms alert system'''

    def __init__(self):

        self.bases = ['GC', 'SI', 'HG', 'PA', 'PL', 'LE', 'HE', 'GF', 'CL',
                      'RB', 'HO', 'BRN', 'NG', 'ZB', 'UB', 'ZF', 'ZN', 'ZL',
                      'ZM', 'ZS', 'ZC', 'CT', 'ZW', 'KE', 'MWE', 'ES', 'TF',
                      'NQ', 'RTY', 'EMD', 'YM', 'Z', 'FESX', 'FGBL', 'KC',
                      'SB', 'CC', 'C']

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

    @classmethod
    def workers(cls, wrkr_args):
        """docstring"""

        # Probably need to check exchange status somewhere

        yday_ohlc = Market.get_yday_ohlc(wrkr_args['sym'])

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

            price = Alert.upd_price(wrkr_args['sym'])
            rvol, rvol20d = Alert.upd_rvol(
                wrkr_args['base'], wrkr_args['kdb_data'])

            # Generate alerts

            for i in dispatcher:
                i()

            time.sleep(random.randint(5, 25))

    def main(self):
        """docstring"""

        # Run tests

        Alert.test_rdb()

        # Get front months

        basesyms = Market.get_front_months(self)

        wrkr_args = list()

        # Import KDB data from Vol class

        rvol = Vol()

        for key, value in basesyms.items():
            wrkr_args.append({'base': key, 'sym': value})

        for i in wrkr_args:
            i['kdb_data'] = rvol.kdb_data[i['base']]

        # Multiprocessing

        pool = Pool(len(wrkr_args))
        pool.map(Alert.workers, wrkr_args)
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


if __name__ == '__main__':

    ex = Vol()

    while True:

        ex.main()
