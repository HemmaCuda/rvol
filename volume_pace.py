import pandas as pd
from qpython import qconnection
import datetime

# Bugs
# If realtime crashes this is fucked for the day
# Realtime may not be looking at preivious 5m bar


# Performance
# ltime slows this script down 10x (1s to 10s)

# Feature Requests
# Sort
# Remove outliers


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

    def volume_pace(self):

        today = datetime.datetime.now()
        yday = today - datetime.timedelta(days=1)

        now = datetime.datetime.now().time()
        now = (now.replace(minute=(5 * (now.minute // 5)))
               .strftime('%H:%M'))

        _ = pd.date_range(end=yday, periods=20, freq='B')
        start = _.min().strftime('%Y.%m.%d')
        stop = _.max().strftime('%Y.%m.%d')

        # Bases I want to monitor

        # bases = ['GC', 'SI', 'HG', 'PA', 'PL', 'LE', 'HE', 'GF', 'CL', 'RB',
        #          'HO', 'BRN', 'NG', 'ZB', 'UB', 'ZF', 'ZN', 'ZL', 'ZM', 'ZS',
        #          'ZC', 'CT', 'ZW', 'KE', 'MWE', 'ES', 'TF', 'NQ', 'RTY', 'EMD',
        #          'YM', 'Z', 'FESX', 'FGBL', 'KC', 'SB', 'CC', 'C']

        bases = ['GC']

        Rvol = dict()
        Rvol_20d = dict()

        for i in bases:

            df = Vol.q('select sum volume by 0D00:05:00 xbar ltime'
                       ' utc_datetime from trade where date within ({}; {}),'
                       'base = `$"{}"'.format(start, stop, i))

            tday_cum = Vol.rdb('select sum volume by `date$utc_datetime from '
                               'bar where base = `$"{}"'.format(i))

            # assert tday_cum.index[0] == datetime.datetime.date()

            df = df.append(Vol.rdb('-1# select sum volume by 0D00:05:00 xbar'
                                   ' ltime utc_datetime from bar where '
                                   'base = `$"{}"'.format(i)))

            cumsum = df.groupby(pd.Grouper(freq='D'))['volume'].cumsum()

            yday_cumnow = cumsum.loc[cumsum.index.strftime('%H:%M') == now][-2]

            hist_mean = cumsum.loc[
                cumsum.index.strftime('%H:%M') == now].mean()

            Rvol[i] = round(tday_cum['volume'][0] / yday_cumnow, 2),
            Rvol_20d[i] = round(tday_cum['volume'][0] / hist_mean, 2)

            print(df.loc[df.index.strftime('%H:%M') == now])


if __name__ == '__main__':

    ex = Vol()
    ex.volume_pace()
