"""Repository helpers scoped to a caller-owned psycopg transaction."""

import psycopg

type Connection = psycopg.Connection[tuple[object, ...]]


class Repository:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def exists(self, table: str, column: str, value: object) -> bool:
        """Check trusted internal table/column pairs with a parameterized value."""
        allowed = {
            ("customers", "customer_id"),
            ("sessions", "session_id"),
            ("carts", "cart_id"),
            ("orders", "order_id"),
            ("payments", "payment_id"),
        }
        if (table, column) not in allowed:
            raise ValueError("untrusted repository identifier")
        from psycopg import sql

        query = sql.SQL("SELECT 1 FROM {} WHERE {} = %s").format(
            sql.Identifier(table), sql.Identifier(column)
        )
        with self.connection.cursor() as cursor:
            cursor.execute(query, (value,))
            return cursor.fetchone() is not None
