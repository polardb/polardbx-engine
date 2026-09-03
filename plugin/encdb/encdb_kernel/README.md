Secure DB based on PostgreSQL
=======================================================================

## Background

The most related work is StealthDB. It is an extension to PostgreSQL, leveraging Intel SGX, that endows it with encrypted database functionality (i.e. encrypted values can be persisted in tables, and queries with encrypted expressions and predicates can be specified). The database's integrity and confidentiality is guaranteed under a threat model in which only the CPU is trusted. Further information can be found [here](https://arxiv.org/pdf/1711.02279.pdf).

StealthDB is a research project and its open-source codebase is **not** suitable for production use, for example:
* Plaintext is still visible to server in the form of query parameters and result tuples. (This issue is fixed in their paper, but requires huge efforts for modifying PG codebase.)
* Data encrpytion is achieved by SGX sealing, which does not support data migration and replication efficiently.
* Only one key is active at a time, which needs be loaded explicitly before issuing queries.

## Our Goal

Instead, our **goal** is to design and implement a next-generation cloud-native PG-compatible database with complete security guarantees (i.e., confidentiality, integrity, freshness).

## Documentation

[How to install and run?](docs/user/user-guide.md)

[How to contribute code?](docs/developer/dev-guide.md)
