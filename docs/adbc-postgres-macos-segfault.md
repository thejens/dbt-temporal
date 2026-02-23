# ADBC PostgreSQL Driver Segfault on macOS

## Status: Postgres not supported

PostgreSQL is **not currently supported** by dbt-temporal due to an upstream ADBC
driver crash on macOS. This is a bug in the ADBC PostgreSQL driver's bundled
libpq, not in dbt-temporal.

Supported adapters: **BigQuery**, **Databricks**, **Redshift**, **Snowflake**.

## Problem

The ADBC PostgreSQL driver (`libadbc_driver_postgresql-0.21.0+dbt0.21.0.dylib`)
crashes with `SIGSEGV` (signal 11) during `PQconnectdb` on macOS (Apple Silicon).

The crash is a NULL function pointer call (`pc=0x0000000000000000`) inside
`pqConnectOptions2`, caused by the driver's bundled libpq resolving OpenSSL and
GSSAPI symbols via `dlsym` at runtime:

```
$ nm -m libadbc_driver_postgresql-0.21.0+dbt0.21.0.dylib | grep "dynamically looked up"
  (undefined) external _GSS_C_NT_HOSTBASED_SERVICE (dynamically looked up)
  (undefined) external _OPENSSL_sk_num (dynamically looked up)
  (undefined) external _OPENSSL_sk_pop_free (dynamically looked up)
  (undefined) external _OPENSSL_sk_value (dynamically looked up)
```

macOS ships LibreSSL (not OpenSSL) and Heimdal (not MIT Kerberos), so these
symbols either resolve to NULL or are ABI-incompatible.

## What We Tried

| Mitigation | Result |
|---|---|
| `sslmode=disable` in URI | Still crashes |
| `sslmode=disable` + `gssencmode=disable` in URI | Still crashes |
| `DYLD_LIBRARY_PATH` pointing to Homebrew OpenSSL | Still crashes (SIP strips it) |
| System `psql` with same URI | Works (uses system libpq, not ADBC's) |

The crash persists even with both `sslmode=disable` and `gssencmode=disable` in
the connection URI. The NULL function pointer call appears to happen before libpq
processes these parameters, or the bundled libpq has a bug in its parameter
handling.

## Resolution Path

This must be fixed upstream in the ADBC PostgreSQL driver. The driver should
either statically link OpenSSL/GSSAPI (not use `dlsym`) or gracefully handle
missing symbols.

## References

- [PostgreSQL mailing list: libpq crashing on macOS during connection startup](https://postgrespro.com/list/thread-id/2674084)
- [PostgreSQL mailing list: Libpq linked statically to OpenSSL/LibreSSL](https://postgrespro.com/list/thread-id/2627993)
- PostgreSQL v16 explicitly rejects building with Heimdal GSSAPI
