import datetime
import uuid


class Utils:

    @staticmethod
    def random_id():
        """
        Generate a random session ID.
        """
        uid = str(uuid.uuid1())[:6]
        return datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S") + uid

    @staticmethod
    def create_identifier_template(identifier_type):
        return {
            "@type": "Identifier",
            "identifierType": identifier_type,
            "identifierValue": None,
        }

    @staticmethod
    def _parse_date(date_str, formats):
        for fmt in formats:
            try:
                return datetime.datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def to_date(row):
        x, y = row["LAST_UPDATE"], row["LAST_TRADE"]
        date_str = x if x is not None else y if y is not None else None
        if date_str is None:
            return None

        date_str = str(date_str).replace("T", " ")
        date = Utils._parse_date(
            date_str, ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y%m%d"]
        )
        return date.strftime("%Y-%m-%d %H:%M:%S") if date else None

    @staticmethod
    def _reformat_last_update(row):
        x, y = row["LAST_UPDATE"], row["LAST_UPDATE_DT"]
        if not isinstance(x, str) and not isinstance(y, str):
            return None

        if ":" not in x:
            return Utils._parse_date(x, ["%Y%m%d"])
        else:
            date = Utils._parse_date(x, ["%Y-%m-%d %H:%M:%S"])
            if not date:
                date = Utils._parse_date(f"{y} {x}", ["%Y-%m-%d %H:%M:%S"])

        return date.strftime("%Y-%m-%d %H:%M:%S") if date else None
    