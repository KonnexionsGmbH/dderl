DDErl: Data Discovery Tool.
=====

### Build
1. Build backend and frontend `rebar3 as ui compile`
1. Build backend only `rebar3 compile`
1. Build frontend only `bash ./build_fe.sh`
### Start Console
1. `rebar3 shell` or `ESCRIPT_EMULATOR=werl rebar3 shell` (for GUI in windows) or `ERL_FLAGS="-proto_dist imem_inet_tcp" rebar3 shell` (to start with imem_inet_tcp as proto_dist)
1. go to https://127.0.0.1:8443/dderl in your browser
### Features
1. Browse mnesia and oracle tables in the browser
1. Add and update data
1. Import and Export data
1. Send and receive data from one desitination to other on the same session
1. SQL support for queries
1. Filter, Sort, Distinct and Statistics on data
1. Multifactor authentication support (SMS, SAML and username/password)
1. JSON parsing with SQL 
1. Tailing of tables 
1. Log table rotation and purging
1. Snapshot and restore table
1. Cluster backup and restore
1. Configuration encryption for ssl certificates and passwords
1. D3 graph support to plot graphs
1. Save views of tables 
1. Query history support
1. Connect to other imem server over TCP with SSL
1. CSV file parsing

### Certificates
DDErl runs on SSL. A default certificate/key pair is [supplied](https://bitbucket.org/konnexions/dderl/src/master/priv/certs/). This, however can be changed either by replacing these files at installation or modifying configuration in `ddConfig` table (`[{dderl,dderl,dderlSslOpts}]`). A sample configuration is given below:
```erlang
[{cert,<<48,...,107>>},
 {key,{'RSAPrivateKey',<<48,...,192>>}},
 {versions,['tlsv1.2','tlsv1.1',tlsv1]}]
```
[`erlang:ssl`](http://erlang.org/doc/man/ssl.html) describes all possible options above.
To convert a PEM crt/key files to DER (accepted by erlang SSL binary certificate/key option above) [`public_key:pem_decode/1`](http://www.erlang.org/doc/man/public_key.html#pem_decode-1) may be used as follows to obtain the DER binary of the PEM certificate files:
```erlang
> {ok, PemCrt} = file:read_file("server.crt").
{ok,<<"-----BEGIN CERTIFICATE-----\nMIICyTC"...>>}
> public_key:pem_decode(PemCrt).
[{'Certificate',<<48,130,2,201,48,130,2,50,2,9,0,241,25,...>>,not_encrypted}]
> {ok, PemKey} = file:read_file("server.key").
{ok,<<"-----BEGIN RSA PRIVATE KEY-----\nMIICXAI"...>>}
> public_key:pem_decode(PemKey).              
[{'RSAPrivateKey',<<48,130,2,92,2,1,0,2,129,129,0,160,95,...>>,not_encrypted}]
```

