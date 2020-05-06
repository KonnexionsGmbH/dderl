## DPerl Jobs

- ### skvh copy job

  Job to copy data from skvh table to another skvh table. It supports copying local tables and remote tables over erlimem
  connections and all the combinations.
  
  type | support
  ------------ | -------------
  **sync** | true
  **cleanup** | true with refresh
  **refresh** | false
  
  #### Job configuration
  
  ```erlang
  #dperlJob{name = <<"LocalSkvhCopy">>,
            module = dperl_skvh_copy,
            args = #{cleanup => true,label => "skvh copy",refresh => false,
                     sync => true},
            srcArgs = #{channel => "test"},
            dstArgs = #{channel => "test1"},
            enabled = false,running = false,plan = at_most_once,
            nodes = ['dderl1@127.0.0.1'],
            opts = []}]
  ```
  
  The above configuration moves data from source table `test` (existing table) to destination table `test1` (existing or
  new table would be created by the job). 
  
  To copy table data **from** remote `imem_dal_skvh` table `srcArgs` should be configured as following
  ```erlang
  #{credential =>
        #{password => <<"change_on_install">>,user => <<"system">>},
    links =>
        [#{ip => "HostOrIp",port => 1236,schema => dderl,
           secure => true, prio => 1},
         #{ip => "HostOrIp2",port => 1236,schema => dderl,
           secure => true, prio => 2}],
    minKey => -1  %% optional,
    maxKey => [<<255>>]  %% optional
    sbucket => "channel"}
  ```
  
  To copy table data **to** remote `imem_dal_skvh` table `dstArgs` should be configured as following
  ```erlang
  #{credential =>
        #{password => <<"change_on_install">>,user => <<"system">>},
    dbucketOpts => [audit,history] %% optional,
    links =>
        [#{ip => "HostOrIp",port => 1236,schema => dderl,
           secure => true, prio => 1},
         #{ip => "HostOrIp2",port => 1236,schema => dderl,
           secure => true, prio => 2}],
    dbucket => "channel"}
  ```
  
- ### Dashboard jobs
  
  
  
