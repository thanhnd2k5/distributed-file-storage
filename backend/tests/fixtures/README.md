`control-test-cert.pem` / `control-test-key.pem` are disposable self-signed test
credentials for local TLS regression fixtures. They authenticate no external
service. The production client still verifies certificates and hostnames using
the system trust store. The test temporarily trusts this certificate explicitly.
