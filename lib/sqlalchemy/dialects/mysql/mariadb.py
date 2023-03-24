# mypy: ignore-errors

from .base import MariaDBIdentifierPreparer
from .base import MySQLDialect


class MariaDBDialect(MySQLDialect):
    is_mariadb = True
    supports_statement_cache = True
    name = "mariadb"
    preparer = MariaDBIdentifierPreparer


def loader(driver):
    driver_mod = __import__(
        f"sqlalchemy.dialects.mysql.{driver}"
    ).dialects.mysql
    driver_cls = getattr(driver_mod, driver).dialect

    return type(
        f"MariaDBDialect_{driver}",
        (
            MariaDBDialect,
            driver_cls,
        ),
        {"supports_statement_cache": True},
    )
