import struct
import math
import pandas as pd

hdf5_path = 'c:/historical_data/1min'

def parse_tdx_min(data, code=None):
    d, m, openprice, high, low, close, amount, volume, reserved = struct.unpack('hhfffffff', data)

    year = math.floor(d / 2048 + 2004)
    month = math.floor((d % 2048) / 100)
    day = (d % 2048) % 100

    hour = math.floor(m / 60)
    mins = m % 60

    timestamp = "{:d}-{:d}-{:d} {:02d}:{:02d}:{:02d}".format(year, month, day, hour, mins, 0)
    date = "{:d}-{:d}-{:d}".format(year, month, day)
    # return pd.Series({'datetime': timestamp, 'code':code, 'open': openprice, 'high': high, 'low': low, 'close': close, 'amount': amount, 'volume': volume})
    return timestamp,code,openprice,high,low,close,amount,volume

def save_tdx_data(code=None):
    if code[0] == '5':
        lc1 = "C:/new_hbzq/vipdoc/sh/minline/sh{}.lc1".format(code)
    elif code[0] == '1':
        lc1 = "C:/new_hbzq/vipdoc/sz/minline/sz{}.lc1".format(code)
    with open(lc1, 'rb') as f:
        mem = f.read()
        # df = pd.DataFrame(columns=['datetime', 'code', 'open', 'high', 'low', 'close', 'amount', 'volume'])
        with pd.HDFStore('c:/historical_data/1min/{}'.format(code)) as store:
            ds = []
            for i in range(0,len(mem), 32):
                timestamp,code,openprice,high,low,close,amount,volume = parse_tdx_min(mem[i:i + 32], code=code)
                ds.append({'datetime':timestamp, 'code':code, 'open':openprice, 'high':high, 'low':low, 'close':close, 'amount':amount, 'volume':volume})
            df = pd.DataFrame(ds)
            store.append(key='historical_data', value=df)


def parse_tdx_day(data, file=None, code=None):
    day,open_price,high,low,close,amount,volume,reserved=struct.unpack('iiiiifii', data)

    s=str(day)
    timestamp = "{}/{}/{}".format(s[0:4],s[4:6],s[6:8])

    rate = 100.000
    if '150' in code:
        if code.index('150') == 0:
            rate = 1000.000

    # if file is not None:
    print("{},{},{:.4f},{:.4f},{:.4f},{:.4f},{:.4f},{:.4f}".format(timestamp,code,open/rate,high/rate,low/rate,close/rate,amount,volume), file=file)


if __name__ == '__main__':
    codes = [
            '150033',
            '150189',
            '150144',
            '150290',
            '150165',
            '150208',
            '150282',
            '150118',
            '150274',
            '150276',
            '502015',
            '150095',
            '150298',
            '150149',
            '150324',
            '150131',
            '150146',
            '150300',
            '150288',
            '150136',
            '150214',
            '150206',
            '150278',
            '150199',
            '150292',
            '150134',
            '150332',
            '150296',
            '150222',
            '150023',
            '150195',
            '150264',
            '150336',
            '150248',
            '150201',
            '150256',
            '150306',
            '150210',
            '150168',
            '150220',
            '502025',
            '150141',
            '150268',
            '150228',
            '150308',
            '150284',
            '150322',
            '150040',
            '502008',
            '150067',
            '150250',
            '502028',
            '150097',
            '150304',
            '150218',
            '150230',
            '150158',
            '150174',
            '502042',
            '150107',
            '150193',
            '150242',
            '150109',
            '150280',
            '150224',
            '150326',
            '150260',
            '150294',
            '150176',
            '150019',
            '150185',
            '150170',
            '150234',
            '150187',
            '150084',
            '150270',
            '150330',
            '502032',
            '150212',
            '502018',
            '150151',
            '150310',
            '150318',
            '150236',
            '150266',
            '502002',
            '150252',
            '150139',
            '150153',
            '150272',
            '150262',
            '150232',
            '150086',
            '150093',
            '150316',
            '150197',
            '150238',
            '150031',
            '150312',
            '150344',
            '150124',
            '150244',
            '502058',
            '150089',
            '150258',
            '502055',
            '150226',
            '150191',
            '150302',
            '150077',
            '150246',
            '502038',
            '150091',
            '150101',
            '502050',
            '150182',
            '150052',
            '150216',
            '502005',
            '502022',
            '150105',
            '150180',
            '150328',
            '150172',
            '150060',
            '150054',
            '150122',
            '150178',
            '150204',
            '150050',
            '502012',
            '150113',
            '150013',
            '150058',
            '150065',
            '150017',
            '150029',
            '150037',
            '150056',
            '150075',
            '150048']

    for code in codes:
        save_tdx_data(code=code)