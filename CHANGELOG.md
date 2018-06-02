# 0.3.0 - 2018-06-01

- keep compatible with protobuf 2.0
- enable secure feature by default
- fix potential overflow in channel args

# 0.2.3 - 2018-04-27

- support querying client address

# 0.2.2 - 2018-04-04

- use a different lock for notify to avoid deadlock

# 0.2.1 - 2018-02-23

- support ping configuration
- make `CallOptions` clonable
- support google default credentials
- fix link error on Windows
- support request header

# 0.2.0 - 2017-12-19

- update gRPC from 1.6.1 to 1.7.2
- seperate secure/unsecure features
- fix compilation error on OS X and Win32
- publish gRPC built-in protos

# 0.1.2 - 2017-09-22

- use environment variable to control linking
- clear buffer hint when sending metadata

# 0.1.1 - 2017-09-21

- upgrade gRPC from 1.4.0 to 1.6.1
- support more channel args
- support log

# 0.1.0 - 2017-07-27

initial release
