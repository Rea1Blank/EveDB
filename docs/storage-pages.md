# Slotted pages

The experimental `SlottedPage` codec stores variable-length byte records in
8192-byte pages. It uses explicit little-endian encoding, a CRC32C checksum,
and a transaction sequence number (LSN). Format version 2 supersedes the
unreleased version 1 prototype; no disk compatibility is promised yet.

| Offset | Size | Meaning |
| --- | --- | --- |
| 0 | 4 | Magic `EVPG` |
| 4 | 2 | Format version 2 |
| 6 | 2 | Page size 8192 |
| 8 | 8 | Page ID |
| 16 | 2 | Allocated slot count, including removed slots |
| 18 | 2 | End of slot directory |
| 20 | 2 | Start of packed payloads |
| 22 | 2 | Reserved, zero |
| 24 | 8 | LSN |
| 32 | 4 | CRC32C of the full page with this field zeroed |
| 36 | 4 | Reserved, zero |
| 40 | 4 per slot | Payload offset (`u16`) and length (`u16`) |

Slots grow from the header toward the end; payloads grow from the end toward
the header. Empty records consume a slot but no payload bytes. A removed slot
is encoded as offset zero and length zero. Slot IDs are not reused within a
page. Replacing or deleting a record compacts payload bytes and preserves
other slot IDs. A failed replacement or insertion leaves the page unchanged.

The maximum payload on an empty page is 8148 bytes. A decoder checks header
fields, directory bounds, packed record boundaries, and checksum before
exposing records. Checksums detect damage; recovery requires the surrounding
storage protocol. Neither a page size nor a successful file write implies
atomic persistence of a page.

The page organization borrows the slot-directory technique documented in
[PostgreSQL's page layout](https://www.postgresql.org/docs/18/storage-page-layout.html).
The codec and byte format are EveDB's own implementation.
