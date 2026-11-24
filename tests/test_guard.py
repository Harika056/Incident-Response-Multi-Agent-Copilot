import pytest

from copilot.guard import UnsafeSQL, validate_readonly_sql


@pytest.mark.parametrize("sql", [
    "SELECT * FROM orders",
    "  select count(*) from orders where status = 'NEW';  ",
    "WITH recent AS (SELECT * FROM orders LIMIT 5) SELECT * FROM recent",
    "SELECT created_at FROM orders -- trailing comment",
    "SELECT * FROM pg_locks",
])
def test_allows_read_only(sql):
    assert validate_readonly_sql(sql)


@pytest.mark.parametrize("sql", [
    "DELETE FROM orders",
    "UPDATE inventory SET stock = 0",
    "DROP TABLE orders",
    "INSERT INTO orders(sku, qty) VALUES ('x', 1)",
    "SELECT 1; DROP TABLE orders",
    "SELECT pg_terminate_backend(123)",
    "SELECT pg_sleep(30)",
    "WITH x AS (DELETE FROM orders RETURNING *) SELECT * FROM x",
    "/* hide */ DELETE /* it */ FROM orders",
    "",
    "   ;  ",
    "SET ROLE postgres",
    "COPY orders TO '/tmp/x'",
])
def test_rejects_writes_and_tricks(sql):
    with pytest.raises(UnsafeSQL):
        validate_readonly_sql(sql)
