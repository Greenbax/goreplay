## Summary
This goreplay middleware achieves two goals.
1. Send filtered http requests from zip servers to diffy.
2. Dump these requests to s3.


## How to test?
See the README file in systems/goreplay folder to a general setup.

Below environment variables are needed for this middleware to run
```
export TRAFFIC_REPLAY_AWS_ACCESS_KEY_ID=...
export TRAFFIC_REPLAY_AWS_SECRET_ACCESS_KEY=...
```

## Goreplay already supports s3 dump, why we reinvent the wheel?
Dumping requests to s3 is only supported in Goreplay enterprise version, we do 
not want to pay for it.