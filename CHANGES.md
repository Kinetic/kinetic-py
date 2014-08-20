Changes since 0.7.2
===========================
This section will document changes to the library since the last release

## Important
- Kinetic Protocol version updated to 2.0.6

## New features
- Added handshake to negotiate connection id during connect
- Added TCP_NODELAY socket option to improve performance

## Behavior changes
- Removed DEVICE log from LogTypes.add() (Issue #15)

## Bug Fixes
- Fixed problem with status validation on the AdminClient
- Fixed error message when MAgic number is invalid

Changes from 0.7.1 to 0.7.2
===========================

## Important
- Kinetic Protocol version updated to 2.0.5
- The compiled python proto kinetic/kinetic_pb2.py is now included on the repo

## New features
- Added zero copy support on puts and gets (Requires splice system call)
- Added IPv6 address support on all clients (Issue #8)
- Added new exception type ClusterVersionFailureException (Requires drive version 2.0.4)
- Added new Device specific GetLog (Requires protocol 2.0.5)
- Added SSL/TLS support on all clients

## Bug Fixes
- Fixed bug on invalid magix number (PR #11 contributed by @rpcope1, ASOKVAD-313)
- Fixex bug that caused close() and connect() to fail on a connection that faulted (Issue #7)
- Fixed a bug that caused the AsyncClient to crash when calling close()

Changes from 0.7.0 to 0.7.1
===========================

## New features
- Added setSecurity on AdminClient
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
