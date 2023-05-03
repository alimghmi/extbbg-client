from app.core import Beap
from app import loader


ISIN_MODE = True


def main():
    ids = loader.get_tickers()

    b = Beap('credential.txt', ISIN_MODE)
    
    _univ = b.__create_universe__('production_universe', ids)

    _field = 'https://api.bloomberg.com/eap/catalogs/793986/fieldLists/f20220605065114e46e10/'
    # _field = b.__create_fieldlist__('production_field')
    _trig = b.__create_trigger__()
    
    b.__request__('production_request', _univ, _field, _trig)
    b.__listener__()
    b.__save__()


if __name__ == '__main__':
    main()
