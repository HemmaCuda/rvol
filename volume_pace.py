"""Calculates real time rvol and 20 day historical for select base:sym pairs"""

import random
import datetime
import time
import pickle
import os
from multiprocessing import Pool
from twilio.rest import Client
import pandas as pd
import numpy as np
from qpython import qconnection
from colorama import init, Fore

# pylint: disable-msg=C0103


class Rvol(object):
    """Calculates and stores rvol"""

    def __init__(self):

        self.bases = ['GC', 'SI', 'HG', 'PA', 'PL', 'LE', 'HE', 'GF', 'CL',
                      'RB', 'HO', 'BRN', 'NG', 'ZB', 'UB', 'ZF', 'ZN', 'GE', 
                      'ZL', 'ZM', 'ZS', 'ZC', 'CT', 'ZW', 'KE', 'ES', 'Z', 
                      'R', 'MME', 'FOAT', 'RC', 'G', 'NQ', 'RTY', 'EMD', 
                      'YM', 'Z', 'FESX', 'FDAX', 'NIY', 'FGBL', 'FBTP', 'KC',
                      'SB', 'CC', 'C', 'DX', '6E', '6J', '6C', '6M', '6A']

        self.kdb_data = Rvol.init_rvol(self)

    @classmethod
    def kdb(cls, query):
        """connect to kdb"""

        with qconnection.QConnection('kdb.genevatrading.com', 8000,
                                     pandas=True) as kdb:

            return kdb(query)

    @classmethod
    def rdb(cls, query):
        """Connect to rdb"""

        with qconnection.QConnection('kdb.genevatrading.com', 9218,
                                     pandas=True) as rdb:

            return rdb(query)

    @classmethod
    def rvol_time(cls):
        """Returns a dict of name of the current rvol sessio : time"""

        now = datetime.datetime.now().time()

        if datetime.time(6, 0) <= now <= datetime.time(10, 29, 59):

            return {'am': '06:00; 10:29'}

        elif datetime.time(10, 30) <= now <= datetime.time(16, 59, 59):

            return {'pm': '10:30; 16:59'}

        elif datetime.time(17, 0) <= now <= datetime.time(23, 59, 59):

            return {'asia': '17:00; 23:59'}

        return {'euro': '24:00; 05:59'}

    @classmethod
    def last_20_trade_days(cls):
        """Returns a string of the start and stop dates for last 20 trade days
        in the kdb format YYYY.MM.DD"""

        today = datetime.datetime.now()
        yday = today - datetime.timedelta(days=1)

        now = today.time()

        now = (now.replace(minute=(5 * (now.minute // 5)))
               .strftime('%H:%M'))

        _ = pd.date_range(end=yday, periods=20, freq='B')
        start = _.min().strftime('%Y.%m.%d')
        stop = _.max().strftime('%Y.%m.%d')

        return start, stop

    def init_rvol(self):
        """Fetches the historical startup data from KDB"""

        start, stop = Rvol.last_20_trade_days()

        FILEPATH = 'rvol.pickle'

        kdb_data = dict()

        RVOL_TIMES = {'am': '06:00; 10:29',
                      'pm': '10:30; 16:59',
                      'asia': '17:00; 23:59',
                      'euro': '24:00; 05:59'}

        if os.path.exists(FILEPATH):

            get_file_date = time.ctime(os.path.getctime(FILEPATH))

            file_datetime = datetime.datetime.strptime(
                get_file_date, "%a %b %d %H:%M:%S %Y")

        if (not os.path.exists(FILEPATH) or
                file_datetime.date() != datetime.datetime.now().date()):

            for i in self.bases:

                print('Initializing Rvol:', i)

                kdb_data[i] = dict()

                Rvol.kdb('t: select sum volume by 0D00:05:00 xbar'
                         ' utc_datetime, date from trade where '
                         'date within ({}; {}), base= `$"{}", '
                         'not sym like "*-*", null trade_type'
                         .format(start, stop, i))

                for k, v in RVOL_TIMES.items():

                    kdb_data[i][k] = Rvol.kdb('select from t where '
                                              '(ltime utc_datetime) within ({})'
                                              .format(v))

            with open(FILEPATH, 'wb') as file:
                pickle.dump(kdb_data, file)

        else:

            with open('rvol.pickle', 'rb') as file:
                kdb_data = pickle.load(file)

        return kdb_data

    @classmethod
    def nearest_15m_vol(cls, base):
        """Rvolume in nearest Noncontinuous 15m bar"""

        now = datetime.datetime.now().time()

        now = (now.replace(minute=(5 * (now.minute // 5)))
               .strftime('%H:%M'))

        rvol_time = list(Rvol.rvol_time().values())[0][0:5]

        data = Rvol.rdb('select sum volume by `date$utc_datetime'
                        ' from bar where (`date$utc_datetime)'
                        ' = (`date$(gtime .z.z)),'
                        ' base = `$"{}", not sym like "*-*", '
                        '((`time$(ltime utc_datetime)) within (('
                        '`time${}:00); (`time${}:00)))'
                        .format(base, rvol_time, now))
        try:

            return data['volume'].item()

        except ValueError:

            return 0

    def nearest_15m_20d_vol_avg(self, base):
        """Calculates the 15m historical average over the last 20 days"""

        cur_sesh = list(Rvol.rvol_time().keys())[0]

        utc = datetime.datetime.utcnow().time().strftime('%H:%M')

        cut = self.kdb_data[base][cur_sesh].loc[
            self.kdb_data[base][cur_sesh].index.levels[0]
            .strftime('%H:%M') < utc]

        return cut.groupby(by='date').sum().mean().item()

    def rvol_20d(self, base):
        """Calculates the 20d Rvol to the nearest 5 minute bar"""

        today = Rvol.nearest_15m_vol(base)

        avg_20d = Rvol.nearest_15m_20d_vol_avg(self, base)

        rvol_20d = round((today / avg_20d), 1)

        if np.isnan(rvol_20d):

            return 0
        return rvol_20d

    @classmethod
    def last_15m_vol(cls, base):
        """Rvolume in continuous previous 15 minutes"""

        td_15m = Rvol.rdb('-1# select sum volume from bar where '
                          '(`date$utc_datetime) = (`date$(gtime .z.z)), '
                          'base = `$"{}", not sym like "*-*",'
                          ' (`time$utc_datetime) > ((`time$.z.z) '
                          '- 00:15:00)'
                          .format(base))

        return td_15m['volume'].item()

    @classmethod
    def last_20d_15m_vol(cls, base):
        """Mean volume in continuous previous 15 minutes for last 20 days"""

        first, last = Rvol.date_rn_last_20d()

        data = Rvol.kdb('select sum volume by date'
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
        """Returns first and last date for the previous 20 business days"""

        yday = datetime.datetime.now() - datetime.timedelta(days=1)

        _ = pd.date_range(end=yday, periods=20, freq='B')

        first_date = _.min().strftime('%Y.%m.%d')
        last_date = _.max().strftime('%Y.%m.%d')

        return first_date, last_date

    @classmethod
    def rvol_now(cls, base):
        """Rolling 15m volume vs average of last 20 days"""

        td_15m = Rvol.last_15m_vol(base)

        avg_15m_20d = Rvol.last_20d_15m_vol(base)

        try:

            return round(td_15m / avg_15m_20d, 1)

        except (ValueError, ZeroDivisionError):

            return 0

    def get_rvol_now(self):
        """Returns a dict of all rvol_now in self.bases"""

        rvol_now = dict()

        for base in self.bases:

            rvol_now[base] = Rvol.rvol_now(base)

        return rvol_now

    def get_rvol_20d(self):
        """Returns a dict of all rvol_20d in self.bases"""

        rvol_20d = dict()

        for base in self.bases:

            rvol_20d[base] = Rvol.rvol_20d(self, base)

        return rvol_20d


class Display(object):
    """terminal print method"""

    def __init__(self):

        self.sectors = {"Metals": ["GC", "SI", "HG", "PL", "PA"],
                        "Energy": ["CL", "RB", "HO", "BRN", "G", "NG"],
                        "Meats": ["LE", "HE", "GF"],                        
                        "Grains": ["ZC", "ZW", "ZS", "ZM", "ZL", "KE"],
                        "Softs": ["SB", "CT", "KC", "RC", "CC", "C"],
                        "Currencies": ["DX", "6E", "6J", "6C", "6M", "6A"],
                        "Bonds": ["ZN", "ZF", "ZB", "UB", "FGBL", "R", "FOAT", "FBTP", "GE"],                    
                        "US Equities": ["ES", "NQ", "YM", "RTY", "EMD"],
                        "Equities": ["FESX", "FDAX","NIY", "Z", "MME"]}

    # set num_rows to length of the largest sector and initialize the
    # output_buffer

    def main(self, states, rvol_now, rvol_20d):
        """Input is a dict with base : rvol pairs, output to terminal"""
        largest_sector = max(len(elem) for elem in self.sectors.values())
        num_rows = 2 * largest_sector
        output_buffer = [""] * num_rows
        column_names = ""
        COLUMN_WIDTH = 18
        INTENSITY_FACTOR = 1.3

        for key in self.sectors:

            column_names += key + ' ' * (COLUMN_WIDTH - len(key))

            for i in range(largest_sector):

                t1 = ""
                t2 = ""
                state = None

                # append each key value pair to the corresponding row
                if i < len(self.sectors[key]):

                    symbol = self.sectors[key][i]
                    try:
                        state = states[symbol]
                    except KeyError:
                        pass

                    rvol_intensity = min(
                        5, int(rvol_now[symbol] // INTENSITY_FACTOR))

                    t1 = (symbol
                          + ' '
                          * ((COLUMN_WIDTH // 2)
                             - len(symbol)
                             - rvol_intensity)
                          + ('*' * rvol_intensity)
                          + str(rvol_now[symbol]))

                    t2 = (COLUMN_WIDTH // 2) * ' ' + str(rvol_20d[symbol])

                # Fill whitespace to align columns
                t1 += ' ' * (COLUMN_WIDTH - len(t1))
                t2 += ' ' * (COLUMN_WIDTH - len(t2))

                if state is not None:
                    t1 = Display.format_colors(states[symbol], t1)
                    t2 = Display.format_colors(states[symbol], t2)

                output_buffer[i * 2] += t1
                output_buffer[(i * 2) + 1] += t2

        # format column names for Top X Now and Session
        t3 = "Top " + str(num_rows) + " Now"
        t3 += ' ' * (COLUMN_WIDTH - len(t3))
        column_names += t3

        t3 = "Top " + str(num_rows) + " Session"
        t3 += ' ' * (COLUMN_WIDTH - len(t3))
        column_names += t3

        # get sorted list of bases from rvol_now
        rvol_sort = sorted(rvol_now, key=rvol_now.get, reverse=True)

        # Top X Now - format each line and add to the output buffer
        for i in range(0, num_rows):

            symbol = rvol_sort[i]
            rvol_intensity = min(5, int(rvol_now[symbol] // INTENSITY_FACTOR))

            temp = (rvol_sort[i]
                    + ' '
                    * ((COLUMN_WIDTH // 2)
                       - len(symbol)
                       - rvol_intensity)
                    + '*'
                    * rvol_intensity
                    + str(rvol_now[symbol]))

            temp += ' ' * (COLUMN_WIDTH - len(temp))
            temp = Display.format_colors(states[symbol], temp)

            output_buffer[i] += temp

        # get sorted list of bases from rvol_20d
        rvol_sort = sorted(rvol_20d, key=rvol_20d.get, reverse=True)

        # Top X Session - format each line and add to the output buffer
        for i in range(0, num_rows):

            symbol = rvol_sort[i]
            rvol_intensity = min(5, int(rvol_20d[symbol] // INTENSITY_FACTOR))

            temp = (rvol_sort[i]
                    + ' '
                    * ((COLUMN_WIDTH // 2)
                       - len(symbol)
                       - rvol_intensity)
                    + '*'
                    * rvol_intensity
                    + str(rvol_20d[symbol]))

            temp += ' ' * (COLUMN_WIDTH - len(temp))
            temp = Display.format_colors(states[symbol], temp)

            output_buffer[i] += temp

        # clear screen and print everything to screen
        print('\n' * 50)

        print(datetime.datetime.now().time())
        print(column_names)
        for line in output_buffer:
            print(line)

    @classmethod
    def format_colors(cls, state, temp):
        """docstring"""

        if state == 1:
            return (Fore.GREEN + temp + Fore.WHITE)
        elif state == -1:
            return (Fore.RED + temp + Fore.WHITE)
        return temp
    

class Market(object):
    """Generic base/sym data"""

    def __init__(self):

        self.bases = ['GC', 'SI', 'HG', 'PA', 'PL', 'LE', 'HE', 'GF', 'CL',
                      'RB', 'HO', 'BRN', 'NG', 'ZB', 'UB', 'ZF', 'ZN', 'GE', 
                      'ZL', 'ZM', 'ZS', 'ZC', 'CT', 'ZW', 'KE', 'ES', 'Z', 
                      'R', 'MME', 'FOAT', 'RC', 'G', 'NQ', 'RTY', 'EMD', 
                      'YM', 'Z', 'FESX','FDAX', 'NIY', 'FGBL', 'FBTP', 'KC',
                      'SB', 'CC', 'C', 'DX', '6E', '6J', '6C', '6M', '6A']

        self.front_months = Market.get_front_months(self)

        self.yday_ohlc_sym = dict()

        for key, value in self.front_months.items():

            self.yday_ohlc_sym[value] = Market.get_yday_ohlc(value)

        self.yday_ohlc_base = dict()

        for key, value in self.front_months.items():

            self.yday_ohlc_base[key] = self.yday_ohlc_sym[value]

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

            _ = Rvol.kdb('select date, sym from dailybar where date = {},'
                         'base = `{}, volume = max volume, not sym like '
                         '"*-*"'.format(yday_str, i))

            while _.empty:

                temp = ((yday - datetime.timedelta(days=count))
                        .strftime('%Y.%m.%d'))

                count += 1

                _ = Rvol.kdb('select date, sym from dailybar where date = {},'
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

        _ = Rvol.kdb('-1# select date, open, high, low, close'
                     ' from dailybar where date within((.z.D - 4); (.z.D - 1))'
                     ', sym = `$"{}"'
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

        return Rvol.rdb('-1# select close from bar where sym ='
                        '`$"{}"'.format(sym))['close'].item()

    def get_state(self):
        """returns a dict of base:1, 0, -1 where 1 is above YH,
        0 is between YH and YL, and -1 is below YL"""

        state = dict()

        for key, value in self.front_months.items():

            price = Market.upd_price(value)

            if price > self.yday_ohlc_base[key]['yh']:

                state[key] = 1

            elif price < self.yday_ohlc_base[key]['yl']:

                state[key] = -1

            else:

                state[key] = 0

        return state


class Alert(object):
    """Multithreaded sms alert system"""

    def __init__(self):
        
        self.bases = ['GC', 'SI', 'HG', 'PA', 'PL', 'LE', 'HE', 'GF', 'CL',
                      'RB', 'HO', 'BRN', 'NG', 'ZB', 'UB', 'ZF', 'ZN', 'GE', 
                      'ZL', 'ZM', 'ZS', 'ZC', 'CT', 'ZW', 'KE', 'ES', 'Z', 
                      'R', 'MME', 'FOAT', 'RC', 'G', 'NQ', 'RTY', 'EMD', 
                      'YM', 'Z', 'FESX','FDAX', 'NIY', 'FGBL', 'FBTP', 'KC',
                      'SB', 'CC', 'C', 'DX', '6E', '6J', '6C', '6M', '6A']


    @classmethod
    def upd_price(cls, sym):
        """docstring"""

        return Rvol.rdb('-1# select close from bar where sym ='
                        '`$"{}"'.format(sym))['close'].item()

    @classmethod
    def test_rdb(cls):
        """docstring"""

        _ = Rvol.rdb('-1# select close by ltime utc_datetime from bar'
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

                # Rvol.send_sms(sym, body)

                print(wrkr_args['sym'], body)

                dispatcher.remove(single_outside_rvol20d)

        def double_outside_rvol20d():
            """docstring"""

            if (rvol20d > 1.0) and (test_yh + test_yl == 2):

                body = 'double_outside_rvol'

                # Rvol.send_sms(sym, body)

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

        # Import KDB data from Rvol class

        rvol = Rvol()

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
        """docstring"""

        # Multi threaded

        now = datetime.datetime.now().time()

        now = (now.replace(minute=(5 * (now.minute // 5)))
               .strftime('%H:%M'))

        # Calculate

        _ = Rvol.rdb('select sum volume by `date$utc_datetime'
                     ' from bar where (`date$utc_datetime)'
                     ' = (`date$(gtime .z.z)),'
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

        rvol = round(tday_cum / yday_vol, 1)
        rvol20d = round(tday_cum / hist_mean, 1)

        return rvol, rvol20d


if __name__ == '__main__':

    # initialize colorama
    init()

    ex = Rvol()
    ex2 = Market()
    ex3 = Display()

    def main():
        """Returns a dict of base : rvol_now pairs"""

        rvol_now = ex.get_rvol_now()
        rvol_20d = ex.get_rvol_20d()
        state = ex2.get_state()

        ex3.main(state, rvol_now, rvol_20d)

    while True:

        main()
