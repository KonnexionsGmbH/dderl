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

  Status Jobs used to monitor the other jobs and health of the nodes. This data can be used to plot dperl_dashboard.

  type | support
  ------------ | -------------
  **sync** | true
  **cleanup** | true only for master job
  **refresh** | false

  #### Job configurations

  - ##### Master Job configuration

    ```erlang
    #dperlJob{name = <<"DperlMasterPull">>,
              module = dperl_status_pull,
              args = #{cleanup => true,heartBeatInterval => 30000,
                       label => "dperl:LB:Stag DPERL COLLECT",refresh => false,
                       sync => true,tableOpts => []},
              srcArgs = #{focus =>
                              #{aggregators => [{dperl_status_agr,write_to_channel}],
                                <<"shortid">> =>
                                    #{frequency => 3,location => local,
                                      metric_src => dperl_metrics}},
                          metrics =>
                              [#{aggregators => [{dperl_status_agr,write_to_channel}],
                                 frequency => 3,
                                 key => {jobs,[]},
                                 location => local,metric_src => dperl_metrics},
                               #{aggregators => [{dperl_status_agr,check_job_down}],
                                 frequency => 2,
                                 key => {job_down_count,[]},
                                 location => local,metric_src => dperl_metrics},
                               #{aggregators => [{dperl_status_agr,write_to_channel}],
                                 key => {errors,[]},
                                 location => local,metric_src => dperl_metrics},
                               #{aggregators => [{dperl_status_agr,check_memory_on_nodes}],
                                 frequency => 7,
                                 key => {agr,node_memory,[]},
                                 location => local,metric_src => dperl_metrics},
                               #{aggregators => [{dperl_status_agr,check_job_error}],
                                 frequency => 3,
                                 key => {agr,error_count,[]},
                                 location => local,metric_src => dperl_metrics},
                               #{aggregators => [{dperl_status_agr,check_heartbeats}],
                                 key => {agr,heartbeat,[]},
                                 location => local,metric_src => dperl_metrics},
                               #{aggregators => [{dperl_status_agr,merge_node_errors}],
                                 frequency => 5,
                                 key => {agr,node_error,[]},
                                 location => local,metric_src => dperl_metrics}],
                          node_collector => "DperlStatusPull"},
              dstArgs = #{channel => "DPERL"},
              enabled = false,running = false,plan = at_most_once,
              nodes = ['dderl1@127.0.0.1'],
              opts = []}
    ```
  - ##### Node Job configuration

    This job has to run on all nodes.

    ```erlang
    #dperlJob{name = <<"DperlStatusPull">>,
              module = dperl_status_pull,
              args = #{cleanup => false,heartBeatInterval => 30000,
                       label => "dperl:LB:Stag DPERL COLLECT",refresh => false,
                       sync => true,tableOpts => []},
              srcArgs = #{master_name => "DperlMasterPull",
                          metrics =>
                              [#{aggregators => [{dperl_status_agr,sync_puller_error}],
                                 key => {job_status,[]},
                                 location => local,metric_src => dperl_metrics},
                               #{aggregators => [{dperl_status_agr,check_nodes}],
                                 frequency => 5,key => erlang_nodes,location => local,
                                 metric_src => imem_metrics},
                               #{aggregators => [{dperl_status_agr,check_nodes}],
                                 frequency => 5,key => data_nodes,location => local,
                                 metric_src => imem_metrics},
                               #{aggregators => [{dperl_status_agr,write_to_aggr}],
                                 key => {job_error_count,[]},
                                 location => local,metric_src => dperl_metrics},
                               #{aggregators => [{dperl_status_agr,format_system_memory}],
                                 frequency => 7,key => system_information,location => local,
                                 metric_src => imem_metrics}]},
              dstArgs = #{channel => "DPERL"},
              enabled = false,running = false,plan = on_all_nodes,
              nodes = ['dderl1@127.0.0.1'],
              opts = []}
    ```
- ### File Copy job

  This job can be used to sync files, soruce folder and the file pattern can be configured and then the job moves the 
  file to the destination folder. Backing up of source files is also possible. Optionally temporary directory can be
  used to move the file, where only once the whole file is moved to the temporary directory it is moved to the
  desitation folder.

  #### Possible combinations of file copy

  - single source to single destination (1 to 1)
  - single source to multiple destination (1 to N)
  - multiple source to single destination (N to 1)
  - multiple source to multiple destination (N to N)

  #### Source and destination can be on
  
  - Local file system
  - NFS/CIFS
  - Over SFTP

  #### Job Configuration

  ```erlang
  #dperlJob{name = <<"FileCopy">>,module = dperl_file_copy,
            args = #{debug => false,label => "File copy",status_dir => "status",
                     status_extra => "WORKING\n600\n900",
                     status_path => "/tmp/file_copy_job",
                     sync => true},
            srcArgs = #{default =>
                            #{mask => "?????.csv",proto => local,
                              root => "/tmp/file_copy_job/dir"}},
            dstArgs = #{default =>
                            #{mask => [],proto => local,
                              root => "/tmp/file_copy_job/dir2"}},
            enabled = false,running = false,plan = at_most_once,
            nodes = ['dderl1@127.0.0.1'],
            opts = []}
  ```
  
  #### File mask

  `?` is used to denote a single character. `#` is used to denote an integer.
  `test_123456_klv.csv` can be represented as `test_######_???.csv`
