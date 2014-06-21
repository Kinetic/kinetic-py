Changes since 0.7.0
===================
This section will document changes to the library since the last release

## New features
- Added getVersion and getVersionAsync to the library.

## Bug Fixes
- Fixed tests not running and testcases with hardcoded 'localhost'
- Fixed flush operation build parameters (Merge #5, contributed by @rpcope1).
- AsyncClient returns NotConnected exception when an operation is attempted on a client before calling connect().
- Lowered default number of keys asked on ranges to 200 (ASKOVAD-287).
- Fixed typo on baset test case (Merge #2, contributed by @zaitcev).

Changes from 0.6.0.2 to 0.7.0
=============================

## Important
Kinetic Protocol version updated to 2.0.3

## New features
- Added Flush command (Requires protocol 2.0.3).
- Added Limits section on GetLog (Requires protocol 2.0.3).
- Added protocol_version field on kinetic module.
- Added version field on kinetic module.

## Breaking changes
- Renamed Synchronization.ASYNC to Synchronization.WRITETHROUGH
- Renamed Synchronization.SYNC to Synchronization.WRITEBACK

## Bug Fixes
- Fixed issue with asynchronous clients leaving the socket open after _close_ was called (ASOKVAD-263).
