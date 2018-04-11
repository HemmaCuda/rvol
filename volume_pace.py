import pandas as pd
from qpython import qconnection
import datetime

# Bugs
# If realtime crashes this is fucked for the day

# Performance
# ltime slows this script down 10x (1s to 10s)

# Feature Requests
# Remove outliers
# Write tests


class Vol(object):

    def __init__(self, sym=None):
        self.sym = sym

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

    def Rvol(self):

        today = datetime.datetime.now()
        yday = today - datetime.timedelta(days=1)

        _ = pd.date_range(end=yday, periods=20, freq='B')
        start = _.min().strftime('%Y.%m.%d')
        stop = _.max().strftime('%Y.%m.%d')

        # Bases I want to monitor

        bases = ['GC', 'SI', 'HG', 'PA', 'PL', 'LE', 'HE', 'GF', 'CL', 'RB',
                 'HO', 'BRN', 'NG', 'ZB', 'UB', 'ZF', 'ZN', 'ZL', 'ZM', 'ZS',
                 'ZC', 'CT', 'ZW', 'KE', 'MWE', 'ES', 'TF', 'NQ', 'RTY', 'EMD',
                 'YM', 'Z', 'FESX', 'FGBL', 'KC', 'SB', 'CC', 'C']

        Rvol = dict()
        Rvol20d = dict()

        for i in bases:

            try:

                df = Vol.q('select sum volume by date from '
                           'trade where date within ({}; {}), base = `$"{}", '
                           '(`time$utc_datetime) < (`time$.z.z),'
                           'not sym like "*-*", ((`time$(ltime utc_datetime))'
                           'within ((`time$07:00:00); (`time$15:00:00)))'
                           .format(start, stop, i))

                tday_cum = Vol.rdb('select sum volume by `date$utc_datetime'
                                   ' from bar where (`date$utc_datetime)'
                                   ' = (`date$.z.z),'
                                   ' base = `$"{}", not sym like "*-*", '
                                   '((`time$(ltime utc_datetime)) within (('
                                   '`time$07:00:00); (`time$15:00:00)))'
                                   .format(i))

                tday_cum = tday_cum['volume'].sum()

                hist_mean = df['volume'].mean()

                Rvol[i] = round(tday_cum / df['volume'][-1], 2)
                Rvol20d[i] = round(tday_cum / hist_mean, 2)

            except KeyError:

                pass

            except IndexError:

                pass

        Rvol_sort = sorted(Rvol, key=Rvol.get)
        Rvol20d_sort = sorted(Rvol20d, key=Rvol20d.get)

        print('Rvol:')
        for i in Rvol_sort:
            print(i, Rvol[i])

        print('Rvol20:')
        for i in Rvol20d_sort:
            print(i, Rvol20d[i])

    def compare(self, base=None):

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

    def delta(self):

        df = Vol.q('select ')

    def get_front_months():
        """This code needs to be run once everyday at 5pm"""

        q = Vol.q()

        # Bases I want to monitor

        bases = ['GC', 'SI', 'HG', 'PA', 'PL', 'LE', 'HE', 'GF', 'CL', 'RB',
                 'HO', 'BRN', 'NG', 'ZB', 'UB', 'ZF', 'ZN', 'ZL', 'ZM', 'ZS',
                 'ZC', 'CT', 'ZW', 'KE', 'MWE', 'ES', 'TF', 'NQ', 'RTY', 'EMD',
                 'YM', 'Z', 'FESX', 'FGBL', 'KC', 'SB', 'CC', 'C']

        # Get front month based on yesterday EOD volume

        now = datetime.datetime.now().date()

        yday = (now - datetime.timedelta(days=1))

        yday_str = yday.strftime('%Y.%m.%d')

        syms = set()

        sym_errors = set()

        count = 1

        for i in bases:

            _ = q('select date, sym from dailybar where date = {},'
                  'base = `{}, volume = max volume, not sym like '
                  '"*-*"'.format(yday_str, i))

            while _.empty:

                temp = ((yday - datetime.timedelta(days=count))
                        .strftime('%Y.%m.%d'))

                count += 1

                _ = q('select date, sym from dailybar where date = {},'
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

                syms.add(_['sym'][0])

            else:

                pass

        print('Could not find syms for:', sym_errors)
        print('Symbols found:', syms)

        return syms

    def out_range(self):

        today = datetime.datetime.now()
        yday = today - datetime.timedelta(days=1)

        _ = pd.date_range(end=yday, periods=1, freq='B')
        _ = ''.join(_.strftime('%Y.%m.%d'))

        syms = Vol.get_front_months()

        for i in syms:

            df = Vol.q('select high, low from dailybar where date = {},'
                       ' sym = {}'
                       .format(_, i))


if __name__ == '__main__':

    ex = Vol()
    ex.Rvol()
