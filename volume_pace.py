import pandas as pd
from qpython import qconnection
import datetime


class Vol(object):

    def __init__(self, sym=None):
        self.sym = sym

        self.q = qconnection.QConnection(host='kdb.genevatrading.com',
                                         port=8000, pandas=True)
        self.q.open()

    def volume_pace(self):

        now = datetime.datetime.now().time()
        now = (now.replace(minute=(15 * (now.minute // 15)))
               .strftime('%H:%M'))

        yday = datetime.datetime.now() - datetime.timedelta(days=1)

        _ = pd.date_range(end=yday, periods=5, freq='B')
        start = _.min().strftime('%Y.%m.%d')
        stop = _.max().strftime('%Y.%m.%d')

        df = self.q('select sum volume by 0D00:15:00 xbar utc_datetime '
                    'from trade where date within ({}; {}),'
                    'sym = `$"{}"'.format(start, stop, self.sym))
        cumsum = df.groupby(pd.Grouper(freq='D'))['volume'].cumsum()

        print(df.loc[df.index.strftime('%H:%M') == now])
        print(cumsum.loc[cumsum.index.strftime('%H:%M') == now])


ex = Vol('ESH8')
ex.volume_pace()
