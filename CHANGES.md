Changes since 0.7.0
===================
This section will document changes to the library since the last release

Changes from 0.6.0.2 to 0.7.0
=============================

## Important
Kinetic Protocol version updated to 2.0.3

## New features
- Added Flush command (Requires protocol 2.0.3)
- Added Limits section on GetLog (Requires protocol 2.0.3)
- Added protocol_version field on kinetic module
- Added version field on kinetic module

## Breaking changes
- Renamed Synchronization.ASYNC to Synchronization.WRITETHROUGH
- Renamed Synchronization.SYNC to Synchronization.WRITEBACK

## Bug Fixes
- Fixed issue with asynchronous clients leaving the socket open after _close_ was called (ASOKVAD-263).
