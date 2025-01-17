%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqtt_bench).

-include_lib("emqtt/include/emqtt.hrl").
-define(shared_padding_tab, emqtt_bench_shared_payload).
-define(QoELog, qoe_dlog).

-export([ main/1
        , main/2
        , start/2
        , run/5
        , connect/4
        , loop/5
        ]).

-define(STARTNUMBER_DESC,
        "The start point when assigning sequence numbers to clients. "
        "This is useful when running multiple emqtt-bench "
        "instances to test the same broker (cluster), so the start number "
        "can be planned to avoid client ID collision").

-define(PREFIX_DESC,
        "Client ID prefix. "
        "If not provided '$HOST_bench_(pub|sub)_$RANDOM_$N' is used, "
        "where $HOST is either the host name or the IP address provided in the --ifaddr option, "
        "$RANDOM is a random number and "
        "$N is the sequence number assigned for each client. "
        "If provided, the $RANDOM suffix will not be added.").

-define(SHORTIDS_DESC,
        "Use short client ID. "
        "If --prefix is provided, the prefix is added otherwise "
        "client ID is the assigned sequence number.").

%% common opts for conn, pub and sub.
-define(COMMON_OPTS,
        [{help, undefined, "help", boolean,
          "help information"},
         {dist, $d, "dist", boolean,
          "enable distribution port"},
         %% == Traffic related ==
         {host, $h, "host", {string, "localhost"},
          "mqtt server hostname or comma-separated hostnames"},
         {port, $p, "port", {integer, 1883},
          "mqtt server port number"},
         {version, $V, "version", {integer, 5},
          "mqtt protocol version: 3 | 4 | 5"},
         {count, $c, "count", {integer, 200},
          "max count of clients"},
         {conn_rate, $R, "connrate", {integer, 0}, "connection rate(/s), default: 0, fallback to use --interval"},
         {interval, $i, "interval", {integer, 10},
          "interval of connecting to the broker"},
         {ifaddr, undefined, "ifaddr", string,
          "local ipaddress or interface address"},
         {prefix, undefined, "prefix", string, ?PREFIX_DESC},
         {shortids, $s, "shortids", {boolean, false}, ?SHORTIDS_DESC},
         {startnumber, $n, "startnumber", {integer, 0}, ?STARTNUMBER_DESC},
         {num_retry_connect, undefined, "num-retry-connect", {integer, 0},
          "number of times to retry estabilishing a connection before giving up"},
         {conn_rate, $R, "connrate", {integer, 0}, "connection rate(/s), default: 0, fallback to use --interval"},
         {reconnect, undefined, "reconnect", {integer, 0},
          "max retries of reconnects. 0: disabled"},
         %% == Transport: TCP, TLS, QUIC, WS ==
         {ssl, $S, "ssl", {boolean, false},
          "ssl socket for connecting to server"},
         {cacertfile, undefined, "cacertfile", string,
          "CA certificate for server verification"},
         {certfile, undefined, "certfile", string,
          "client certificate for authentication, if required by server"},
         {keyfile, undefined, "keyfile", string,
          "client private key for authentication, if required by server"},
         {quic, undefined, "quic", {string, "false"},
          "QUIC transport"},
         {ws, undefined, "ws", {boolean, false},
          "websocket transport"},
         {nst_dets_file, undefined, "load-qst", string, "load quic session tickets from dets file"},
         %% == MQTT layer ==
         {username, $u, "username", string,
          "username for connecting to server"},
         {password, $P, "password", string,
          "password for connecting to server"},
         {keepalive, $k, "keepalive", {integer, 300},
          "keep alive in seconds"},
         {clean, $C, "clean", {boolean, true},
          "clean session"},
         {expiry, $x, "session-expiry", {integer, 0},
          "Set 'Session-Expiry' for persistent sessions (seconds)"},
         %%% == Perf related ==
         {lowmem, $l, "lowmem", boolean, "enable low mem mode, but use more CPU"},
         {force_major_gc_interval, undefined, "force-major-gc-interval", {integer, 0},
          "interval in milliseconds in which a major GC will be forced on the "
          "bench processes.  a value of 0 means disabled (default).  this only "
          "takes effect when used together with --lowmem."},
         %%% == Monitoring & Loggings ==
         {qoe, $Q, "qoe", {atom, false},
          "set 'true' to enable QoE tracking. set 'dump' to dump QoE disklog to csv then exit"},
         {qoelog, undefined, "qoelog", {string, ""},
          "Write QoE event logs to the QoE disklog file for post processing"},
         {prometheus, undefined, "prometheus", undefined,
          "Enable metrics collection via Prometheus. Usually used with --restapi to enable scraping endpoint."
         },
         {restapi, undefined, "restapi", {string, disabled},
          "Enable REST API for monitoring and control. For now only serves /metrics. "
          "Can be set to IP:Port to listen on a specific IP and Port, or just Port "
          "to listen on all interfaces on that port."
         },
         {log_to, undefined, "log_to", {atom, console},
          "Control where the log output goes. "
          "console: directly to the console      "
          "null: quietly, don't output any logs."
         }
        ]
       ).

-define(PUB_OPTS, ?COMMON_OPTS ++
        [
         {interval_of_msg, $I, "interval_of_msg", {integer, 1000},
          "interval of publishing message(ms)"},
         {topic, $t, "topic", string,
          "topic subscribe, support %u, %c, %i, %s variables"},
         {payload_hdrs, undefined, "payload-hdrs", {string, ""},
          " If set, add optional payload headers."
          " cnt64: strictly increasing counter(64bit) per publisher"
          " ts: Timestamp when emit"
          " example: --payload-hdrs cnt64,ts"
         },
         {size, $s, "size", {integer, 256},
          "payload size"},
         {message, $m, "message", string,
          "Set the message content for publish. "
          "Either a literal message content, or path to a file with payload template "
          "specified via 'template://<file_path>'. "
          "Available variables: %TIMESTAMP%, %TIMESTAMPMS%, %TIMESTAMPUS%, %TIMESTAMPNS%, %UNIQUE%, %RANDOM%. "
          "When using 'template://', --size option does not have effect except for when %RANDOM% placeholder "
          "is used."
         },
         {topics_payload, undefined, "topics-payload", string,
          "json file defining topics and payloads"},
         {qos, $q, "qos", {integer, 0},
          "subscribe qos"},
         {retain, $r, "retain", {boolean, false},
          "retain message"},
         {limit, $L, "limit", {integer, 0},
          "The max message count to publish, 0 means unlimited"},
         {inflight, $F,"inflight", {integer, 1},
          "maximum inflight messages for QoS 1 an 2, value 0 for 'infinity'"},
         {wait_before_publishing, $w, "wait-before-publishing", {boolean, false},
          "wait for all publishers to have (at least tried to) connected "
          "before starting publishing"},
         {max_random_wait, undefined,"max-random-wait", {integer, 0},
          "maximum randomized period in ms that each publisher will wait before "
          "starting to publish (uniform distribution)"},
         {min_random_wait, undefined,"min-random-wait", {integer, 0},
          "minimum randomized period in ms that each publisher will wait before "
          "starting to publish (uniform distribution)"},
         {retry_interval, 0, "retry-interval", {integer, 0},
          "Publisher's resend interval (in seconds) if the expected "
          "acknowledgement for a inflight packet is not "
          "received within this interval. Default value 0 means no resend."
         }
        ]).

-define(SUB_OPTS, ?COMMON_OPTS ++
        [
         {topic, $t, "topic", string,
          "topic subscribe, support %u, %c, %i variables"},
         {payload_hdrs, undefined, "payload-hdrs", {string, []},
          "Handle the payload header from received message. "
          "Publish side must have the same option enabled in the same order. "
          "cnt64: Check the counter is strictly increasing. "
          "ts: publish latency counting."
         },
         {qos, $q, "qos", {integer, 0},
          "subscribe qos"}
        ]).

-define(CONN_OPTS, ?COMMON_OPTS).

-define(cnt_map, cnt_map).
-define(hdr_cnt64, "cnt64").
-define(hdr_ts, "ts").
-define(GO_SIGNAL, go).

main(["sub"|Argv]) ->
    {ok, {Opts, _Args}} = getopt:parse(?SUB_OPTS, Argv),
    ok = maybe_help(sub, Opts),
    ok = check_required_args(sub, [count, topic], Opts),
    main(sub, Opts);

main(["pub"|Argv]) ->
    {ok, {Opts, _Args}} = getopt:parse(?PUB_OPTS, Argv),
    ok = maybe_help(pub, Opts),
    ok = check_required_args(pub, [count], Opts),
    main(pub, Opts);

main(["conn"|Argv]) ->
    {ok, {Opts, _Args}} = getopt:parse(?CONN_OPTS, Argv),
    ok = maybe_help(conn, Opts),
    ok = check_required_args(conn, [count], Opts),
    main(conn, Opts);

main(_Argv) ->
    ScriptPath = escript:script_name(),
    Script = filename:basename(ScriptPath),
    io:format("Usage: ~s pub | sub | conn [--help]~n", [Script]).

maybe_help(PubSub, Opts) ->
    case proplists:get_value(help, Opts) of
        true ->
            usage(PubSub),
            halt(0);
        _ -> ok
    end.

check_required_args(PubSub, Keys, Opts) ->
    lists:foreach(fun(Key) ->
        case lists:keyfind(Key, 1, Opts) of
            false ->
                io:format("Error: '~s' required~n", [Key]),
                usage(PubSub),
                halt(1);
            _ -> ok
        end
    end, Keys).

usage(PubSub) ->
    ScriptPath = escript:script_name(),
    Script = filename:basename(ScriptPath),
    Opts = case PubSub of
               pub -> ?PUB_OPTS;
               sub -> ?SUB_OPTS;
               conn -> ?CONN_OPTS
           end,
    getopt:usage(Opts, Script ++ " " ++ atom_to_list(PubSub)).

main(sub, Opts) ->
    start(sub, Opts);

main(pub, Opts) ->
    Size    = proplists:get_value(size, Opts),
    ets:new(?shared_padding_tab, [named_table, public, ordered_set]),
    Payload = case proplists:get_value(message, Opts) of
                  undefined ->
                    iolist_to_binary([O || O <- lists:duplicate(Size, $a)]);
                  "template://" ++ Path ->
                    {ok, Bin} = file:read_file(Path),
                    {template, Bin};
                  StrPayload ->
                    unicode:characters_to_binary(StrPayload)
              end,
    MsgLimit = pub_limit_fun_init(proplists:get_value(limit, Opts)),
    start(pub, [ {payload, Payload}
               , {payload_size, Size}
               , {limit_fun, MsgLimit}
               | Opts]);

main(conn, Opts) ->
    start(conn, Opts).

start(PubSub, Opts) ->
    prepare(PubSub, Opts),
    init(),
    maybe_init_prometheus(lists:member(prometheus, Opts)),
    maybe_start_restapi(proplists:get_value(restapi, Opts)),
    IfAddr = proplists:get_value(ifaddr, Opts),
    Host = proplists:get_value(host, Opts),
    Rate = proplists:get_value(conn_rate, Opts),
    HostList = addr_to_list(Host),
    AddrList = addr_to_list(IfAddr),
    NoAddrs = length(AddrList),
    NoWorkers0 = max(erlang:system_info(schedulers_online), ceil(Rate / 1000)),
    Count = proplists:get_value(count, Opts),
    {CntPerWorker, NoWorkers} =
        case Count div NoWorkers0 of
            0 ->
                {1, Count};
            N ->
                {N, NoWorkers0}
        end,
    Rem = Count rem NoWorkers,
    Interval = case Rate of
                   0 -> %% conn_rate is not set
                       proplists:get_value(interval, Opts) * NoWorkers;
                   ConnRate ->
                       1000 * NoWorkers div ConnRate
               end,
    PayloadHdrs = parse_payload_hdrs(Opts),
    TopicPayload = parse_topics_payload(Opts),
    io:format("Start with ~p workers, addrs pool size: ~p and req interval: ~p ms ~n~n",
              [NoWorkers, NoAddrs, Interval]),
    true = (Interval >= 1),
    PublishSignalPid =
        case proplists:get_bool(wait_before_publishing, Opts) of
            true ->
                spawn(fun() ->
                          collect_go_signals(NoWorkers),
                          io:format("Collected ~p 'go' signals, start publishing~n", [NoWorkers]),
                          exit(start_publishing)
                      end);
            false ->
                undefined
        end,
    lists:foreach(fun(P) ->
                          StartNumber = proplists:get_value(startnumber, Opts) + CntPerWorker*(P-1),
                          IsLastBatch = (P =:= NoWorkers),
                          Count1 = case IsLastBatch of
                                       true ->
                                           CntPerWorker + Rem;
                                       false ->
                                           CntPerWorker
                                   end,
                          QConnOpts = quic_opts_from_arg(Opts),
                          WOpts = replace_opts(Opts, [{startnumber, StartNumber},
                                                      {interval, Interval},
                                                      {payload_hdrs, PayloadHdrs},
                                                      {topics_payload, TopicPayload},
                                                      {count, Count1},
                                                      {quic, QConnOpts}
                                                     ]),
                          WOpts1 = [{publish_signal_pid, PublishSignalPid} | WOpts],
                          proc_lib:spawn(?MODULE, run, [self(), PubSub, WOpts1, AddrList, HostList])
                  end, lists:seq(1, NoWorkers)),
    timer:send_interval(1000, stats),
    maybe_spawn_gc_enforcer(Opts),
    main_loop(erlang:monotonic_time(millisecond), Count).

collect_go_signals(0) ->
    ok;
collect_go_signals(N) ->
    receive
        ?GO_SIGNAL ->
            collect_go_signals(N - 1)
    end.

prepare(PubSub, Opts) ->
    Sname = list_to_atom(lists:flatten(io_lib:format("~p-~p-~p", [?MODULE, PubSub, rand:uniform(1000)]))),
    case proplists:get_bool(dist, Opts) of
        true ->
            net_kernel:start([Sname, shortnames]),
            io:format("Starting distribution with name ~p~n", [Sname]);
        false ->
            ok
    end,
    prepare_quicer(Opts),
    init_qoe(Opts),
    application:ensure_all_started(emqtt_bench).

init() ->
    process_flag(trap_exit, true),
    Now = erlang:monotonic_time(millisecond),
    Counters = counters(),
    CRef = counters:new(length(Counters)+1, [write_concurrency]),
    ok = persistent_term:put(?MODULE, CRef),
    %% init counters
    InitS = {Now, 0},
    ets:new(?cnt_map, [ named_table
                      , ordered_set
                      , protected
                      , {read_concurrency, true}]),
    true = ets:insert(?cnt_map, Counters),
    lists:foreach(fun({C, _Idx}) ->
                        put({stats, C}, InitS)
                  end, Counters).

main_loop(Uptime, Count) ->
    receive
        publish_complete ->
            return_print("publish complete~n", []);
        stats ->
            print_stats(Uptime),
            maybe_sum_qoe(Count),
            maybe_dump_nst_dets(Count),
            garbage_collect(),
            main_loop(Uptime, Count);
        Msg ->
            print("main_loop_msg: ~p~n", [Msg]),
            main_loop(Uptime, Count)
    end.

maybe_dump_nst_dets(Count)->
    Count == ets:info(quic_clients_nsts, size)
      andalso undefined =/= dets:info(dets_quic_nsts)
      andalso ets:to_dets(quic_clients_nsts, dets_quic_nsts).

maybe_sum_qoe(Count) ->
   %% latency statistic for
   %% - handshake
   %% - conn
   %% - sub
   case get(is_qoe_dlog) of
      true ->
         Data = ets:tab2list(qoe_store),
         maybe_dlog_qoe(Data),
         lists:foreach(fun({ClientId, _}) ->
                             ets:delete(qoe_store, ClientId)
                       end, Data);
      false ->
         case Count == ets:info(qoe_store, size) of
            true ->
               do_print_qoe(Data = ets:tab2list(qoe_store)),
               %% clear to print once
               lists:foreach(fun({ClientId, _}) ->
                                   ets:delete(qoe_store, ClientId)
                             end, Data);
            false ->
               skip
         end
   end.

do_print_qoe([]) ->
   skip;
do_print_qoe(Data) ->
   {H, C, S} = lists:foldl(fun({_Client, {_, H1, C1, S1}}, {H, C, S}) ->
                                 {[H1 | H], [C1 | C], [S1 | S]}
                           end, {[], [], []}, Data),
   lists:foreach(
     fun({_Name, []})-> skip;
        ({Name, X}) ->
           io:format("~p, avg: ~pms, P95: ~pms, Max: ~pms ~n",
                     [Name, lists:sum(X)/length(X), p95(X), lists:max(X)])
     end, [{handshake, H}, {connect, C}, {subscribe, S}]).

print_stats(Uptime) ->
    [print_stats(Uptime, Cnt) ||
        {Cnt, _Idx} <- counters()],
    ok.

print_stats(Uptime, Name) ->
    CurVal = get_counter(Name),
    {LastTS, LastVal} = get({stats, Name}),
    case CurVal == LastVal of
        false ->
            Now = erlang:monotonic_time(millisecond),
            Elapsed = Now - LastTS,
            Tdiff = fmt_tdiff(Now - Uptime),
            case Name of
               publish_latency when Elapsed > 0 ->
                  CurrentRecv = get_counter(recv),
                  {_, LastRecv} = get({stats, recv}),
                  Recv = CurrentRecv - LastRecv,
                  Recv > 0 andalso print("~s ~s avg=~wms~n",
                                         [Tdiff, Name, (CurVal - LastVal) div Recv]);
               _ when Elapsed > 0 ->
                  print("~s ~s total=~w rate=~.2f/sec~n",
                        [Tdiff, Name, CurVal, (CurVal - LastVal) * 1000/Elapsed]);
               _ -> skip
            end,
            put({stats, Name}, {Now, CurVal});
        true -> ok
    end.

fmt_tdiff(D) -> do_fmt_tdiff(D div 1000).

do_fmt_tdiff(S) ->
    Factors = [{60, "s"}, {60, "m"}, {24, "h"}, {10000000, "d"}],
    Result = lists:reverse(do_fmt_tdiff(S, Factors)),
    iolist_to_binary([[integer_to_list(X), Unit] || {X, Unit} <- Result]).

do_fmt_tdiff(0, _) -> [];
do_fmt_tdiff(T, [{Fac, Unit} | Rest]) ->
    [{T rem Fac, Unit} | do_fmt_tdiff(T div Fac,  Rest)].

%% this is only used for main loop
return_print(Fmt, Args) ->
    print(return, Fmt, Args).

print(Fmt, Args) ->
    print(feed, Fmt, Args).

print(ReturnMaybeFeed, Fmt, Args) ->
    % print the return
    io:format("\r"),
    maybe_feed(ReturnMaybeFeed),
    io:format(Fmt, Args).

maybe_feed(ReturnMaybeFeed) ->
    Last = get(?FUNCTION_NAME),
    maybe_feed(Last, ReturnMaybeFeed),
    put(?FUNCTION_NAME, ReturnMaybeFeed).

maybe_feed(return, feed) -> io:format("\n");
maybe_feed(_, _) -> ok.

get_counter(CntName) ->
    [{CntName, Idx}] = ets:lookup(?cnt_map, CntName),
    counters:get(cnt_ref(), Idx).

inc_counter(Prometheus, CntName) ->
   inc_counter(Prometheus, CntName, 1).
inc_counter(false, CntName, Inc) ->
   [{CntName, Idx}] = ets:lookup(?cnt_map, CntName),
   counters:add(cnt_ref(), Idx, Inc);
inc_counter(true, CntName, Inc) ->
   prometheus_counter:inc(CntName, Inc),
   [{CntName, Idx}] = ets:lookup(?cnt_map, CntName),
   counters:add(cnt_ref(), Idx, Inc).

-compile({inline, [cnt_ref/0]}).
cnt_ref() -> persistent_term:get(?MODULE).

run(Parent, PubSub, Opts, AddrList, HostList) ->
    ok = run(Parent, 0, proplists:get_value(count, Opts), PubSub, Opts, AddrList, HostList).

run(_Parent, N, N, _PubSub, _Opts, _AddrList, _HostList) ->
    ok;
run(Parent, I, N, PubSub, Opts0, AddrList, HostList) ->
    emqtt_logger:setup(Opts0),
    SpawnOpts = case proplists:get_bool(lowmem, Opts0) of
                    true ->
                        [ {min_heap_size, 16}
                        , {min_bin_vheap_size, 16}
                        , {fullsweep_after, 1000}
                        ];
                    false ->
                        []
                end,

    Opts1 = replace_opts(Opts0, [ {ifaddr, shard_addr(I, AddrList)}
                                , {host, shard_addr(I, HostList)}
                                ]),
    %% only the last one can send the 'go' signal
    Opts = [{send_go_signal, I + 1 =:= N} | Opts1],
    ID = I + 1 + proplists:get_value(startnumber, Opts),
    spawn_opt(?MODULE, connect, [Parent, ID, PubSub, Opts], SpawnOpts),
    timer:sleep(proplists:get_value(interval, Opts)),
    run(Parent, I + 1, N, PubSub, Opts0, AddrList, HostList).

connect(Parent, N, PubSub, Opts) ->
    process_flag(trap_exit, true),
    rand:seed(exsplus, erlang:timestamp()),
    Prometheus = lists:member(prometheus, Opts),
    GoSignalPid = proplists:get_value(publish_signal_pid, Opts),
    SendGoSignal = proplists:get_value(send_go_signal, Opts),
    MRef = case is_pid(GoSignalPid) of
               true -> monitor(process, GoSignalPid);
               _ -> undefined
           end,
    %% this is the last client in one batch, send go signal when it's ready to connect
    case is_pid(GoSignalPid) andalso true =:= SendGoSignal of
        true ->
            GoSignalPid ! ?GO_SIGNAL;
        false ->
            ok
    end,

    ClientId = client_id(PubSub, N, Opts),
    MqttOpts = [{clientid, ClientId},
                {tcp_opts, tcp_opts(Opts)},
                {ssl_opts, ssl_opts(Opts)},
                {quic_opts, quic_opts(Opts, ClientId)}
               ]
        ++ session_property_opts(Opts)
        ++ mqtt_opts(Opts),
    MqttOpts1 = case PubSub of
                  conn -> [{force_ping, true} | MqttOpts];
                  _ -> MqttOpts
                end,
    RandomPubWaitMS = random_pub_wait_period(Opts),
    AllOpts0 = [ {seq, N}
               , {client_id, ClientId}
               , {publish_signal_mref, MRef}
               , {pub_start_wait, RandomPubWaitMS}
               | Opts],
    TopicPayloadRend = render_payload(proplists:get_value(topics_payload, Opts), AllOpts0 ++ MqttOpts),
    AllOpts = replace_opts(AllOpts0, [{topics_payload, TopicPayloadRend}]),
    {ok, Client} = emqtt:start_link(MqttOpts1),
    ConnectFun = connect_fun(Opts),
    ConnRet = emqtt:ConnectFun(Client),
    ContinueFn = fun() -> loop(Parent, N, Client, PubSub, loop_opts(AllOpts)) end,
    case ConnRet of
        {ok, _Props} ->
            inc_counter(Prometheus, connect_succ),
            Res =
                case PubSub of
                    conn ->
                      ok = maybe_update_client_qoe(Client, Opts),
                      ok;
                    sub -> subscribe(Client, N, AllOpts);
                    pub ->
                      ok = maybe_update_client_qoe(Client, Opts),
                      case MRef of
                         undefined when TopicPayloadRend == undefined ->
                            erlang:send_after(RandomPubWaitMS, self(), publish);
                         undefined ->
                            maps:foreach(fun(TopicName,  #{name := TopicName, interval_ms := DelayMs}) ->
                                               erlang:send_after(RandomPubWaitMS + DelayMs, self(), {publish, TopicName})
                                         end, TopicPayloadRend);
                         _ ->
                            %% send `publish' only when all publishers
                            %% are in place.
                            ok
                      end
                end,
            case Res of
                {error, _SubscribeError} ->
                    maybe_retry(Parent, N, PubSub, Opts, ContinueFn);
                _ ->
                    PubSub =:= sub andalso inc_counter(Prometheus, sub),
                    loop(Parent, N, Client, PubSub, loop_opts(AllOpts))
            end;
        {error, {transport_down, Reason} = _QUICFail} when
            Reason == connection_idle;
            Reason == connection_refused;
            Reason == connection_timeout ->
            inc_counter(Prometheus, Reason),
            maybe_retry(Parent, N, PubSub, Opts, ContinueFn);
        {error, {transport_down, _Other = QUICFail}} ->
            io:format("Error: unknown QUIC transport_down ~p~n", [QUICFail]),
            inc_counter(Prometheus, connect_fail),
            {error, QUICFail};
        {error, Error} ->
            io:format("client(~w): connect error - ~p~n", [N, Error]),
            maybe_retry(Parent, N, PubSub, Opts, ContinueFn)
    end.

maybe_retry(Parent, N, PubSub, Opts, ContinueFn) ->
    Prometheus = lists:member(prometheus, Opts),
    MaxRetries = proplists:get_value(num_retry_connect, Opts, 0),
    Retries = proplists:get_value(connection_attempts, Opts, 0),
    case Retries >= MaxRetries of
        true ->
            inc_counter(Prometheus, connect_fail),
            PubSub =:= sub andalso inc_counter(Prometheus, sub_fail),
            ContinueFn();
        false ->
            inc_counter(Prometheus, connect_retried),
            NOpts = proplists:delete(connection_attempts, Opts),
            connect(Parent, N, PubSub, [{connection_attempts, Retries + 1} | NOpts])
    end.

loop(Parent, N, Client, PubSub, Opts) ->
    Interval = proplists:get_value(interval_of_msg, Opts, 0),
    Prometheus = lists:member(prometheus, Opts),
    Idle = max(Interval * 2, 500),
    MRef = proplists:get_value(publish_signal_mref, Opts),
    receive
        {'DOWN', MRef, process, _Pid, start_publishing} ->
            RandomPubWaitMS = proplists:get_value(pub_start_wait, Opts),
            erlang:send_after(RandomPubWaitMS, self(), publish),
            loop(Parent, N, Client, PubSub, Opts);
        publish = Trigger->
           case (proplists:get_value(limit_fun, Opts))() of
                true ->
                    %% this call hangs if emqtt inflight is full
                    case publish(Client, Opts) of
                        ok ->
                            inc_counter(Prometheus, pub),
                            ok = schedule_next_publish(Prometheus, Interval, Trigger),
                            ok;
                        {error, Reason} ->
                            %% TODO: schedule next publish for retry ?
                            inc_counter(Prometheus, pub_fail),
                            io:format("client(~w): publish error - ~p~n", [N, Reason])
                    end,
                    loop(Parent, N, Client, PubSub, Opts);
                _ ->
                    Parent ! publish_complete,
                    exit(normal)
            end;
        {publish, #{payload := Payload}} ->
            inc_counter(Prometheus, recv),
            maybe_check_payload_hdrs(Prometheus, Payload, proplists:get_value(payload_hdrs, Opts, [])),
            loop(Parent, N, Client, PubSub, Opts);
        {publish, TopicName} = Trigger when is_binary(TopicName) ->
            TopicSpec = maps:get(TopicName, proplists:get_value(topics_payload, Opts)),
            case publish_topic(Client, TopicName, TopicSpec, Opts) of
               ok ->
                  schedule_next_publish(Prometheus, maps:get(interval_ms, TopicSpec), Trigger);
               _ ->
                  Parent ! publish_complete,
                  exit(normal)
            end,
            loop(Parent, N, Client, PubSub, Opts);
       {publish_async_res, ok} ->
          inc_counter(Prometheus, pub),
          loop(Parent, N, Client, PubSub, Opts);
       {publish_async_res, {ok, _}} ->
          inc_counter(Prometheus, pub),
          loop(Parent, N, Client, PubSub, Opts);
       {publish_async_res, {error, _}} ->
          inc_counter(Prometheus, pub_fail),
          loop(Parent, N, Client, PubSub, Opts);
        {'EXIT', _Client, normal} ->
            ok;
        {'EXIT', _Client, {shutdown, {transport_down, unreachable}}} ->
            ok;
        %% msquic worker overload, tune `max_worker_queue_delay_ms'
        {'EXIT', _Client, {shutdown, {transport_down, connection_refused}}} ->
            ok;
        {'EXIT', _Client, Reason} ->
            io:format("client(~w): EXIT for ~p~n", [N, Reason]);
        {puback, _} ->
            %% Publish success for QoS 1 (recv puback) and 2 (recv pubcomp)
            inc_counter(Prometheus, pub_succ),
            loop(Parent, N, Client, PubSub, Opts);
        {disconnected, ReasonCode, _Meta} ->
            io:format("client(~w): disconnected with reason ~w: ~p~n",
                      [N, ReasonCode, emqtt:reason_code_name(ReasonCode)]);
        {connected, _Props} ->
            inc_counter(Prometheus, reconnect_succ),
            IsSessionPresent = (1 == proplists:get_value(session_present, emqtt:info(Client))),
            %% @TODO here we do not really check the subscribe
            PubSub =:= sub andalso not IsSessionPresent andalso subscribe(Client, N, Opts),
            loop(Parent, N, Client, PubSub, Opts);
        Other ->
            io:format("client(~w): discarded unknown message ~p~n", [N, Other]),
            loop(Parent, N, Client, PubSub, Opts)
    after
        Idle ->
            case proplists:get_bool(lowmem, Opts) of
                true ->
                    erlang:garbage_collect(Client, [{type, major}]),
                    erlang:garbage_collect(self(), [{type, major}]);
                false ->
                    skip
            end,
            proc_lib:hibernate(?MODULE, loop, [Parent, N, Client, PubSub, Opts])
	end.

ensure_publish_begin_time() ->
   %% Default: Use Topic from CLI
   ensure_publish_begin_time(publish).
ensure_publish_begin_time(Topic) ->
   case get_publish_begin_time(Topic) of
      undefined ->
         update_publish_start_at(Topic),
         ok;
      _ ->
         ok
   end.

get_publish_begin_time() ->
   get_publish_begin_time(publish).
get_publish_begin_time(Topic) ->
    get({publish_begin_ts, Topic}).

update_publish_start_at(Topic) ->
   put({publish_begin_ts, Topic},
       erlang:monotonic_time(millisecond)).

%% @doc return new value
bump_publish_attempt_counter() ->
    NewCount = case get(success_publish_count) of
                   undefined ->
                       1;
                   Val ->
                       Val + 1
               end,
    _ = put(success_publish_count, NewCount),
    NewCount.

schedule_next_publish(Prometheus, Interval, publish = Trigger) ->
    PubAttempted = bump_publish_attempt_counter(),
    BeginTime = get_publish_begin_time(),
    NextTime = BeginTime + PubAttempted * Interval,
    NowT = erlang:monotonic_time(millisecond),
    Remain = NextTime - NowT,
    Interval > 0 andalso Remain < 0 andalso inc_counter(Prometheus, pub_overrun),
    case Remain > 0 of
        true -> _ = erlang:send_after(Remain, self(), Trigger);
        false -> self() ! Trigger
    end,
   ok;
schedule_next_publish(Prometheus, Interval, {publish, TopicName} = Trigger) ->
   %% Different scheduling for publish with topic specs
   Elapsed = erlang:monotonic_time(millisecond) - get_publish_begin_time(TopicName),
   case Elapsed > Interval of
      true ->
         inc_counter(Prometheus, pub_overrun),
         erlang:send_after(0, self(), Trigger);
      false ->
         erlang:send_after(Interval - Elapsed, self(), Trigger)
   end,
   ok.

pub_limit_fun_init(0) ->
    fun() -> true end;
pub_limit_fun_init(N) when is_integer(N), N > 0 ->
    Ref = counters:new(1, []),
    counters:put(Ref, 1, N),
    fun() ->
        case counters:get(Ref, 1) of
            0 ->
                false;
            X when X < 0 ->
                %% this means PUBLISH overrun the limit option, due to race
                false;
            _ ->
                counters:sub(Ref, 1, 1),
                true
        end
    end.

subscribe(Client, N, Opts) ->
    Qos = proplists:get_value(qos, Opts),
    Res = emqtt:subscribe(Client, [{Topic, Qos} || Topic <- topics_opt(Opts)]),
    case Res of
       {ok, _, _} ->
          maybe_update_client_qoe(Client, Opts),
          ok;
        {error, Reason}->
            io:format("client(~w): subscribe error - ~p~n", [N, Reason])
    end,
    Res.

publish(Client, Opts) ->
    %% Ensure publish begin time is initialized right before the first publish,
    %% because the first publish may get delayed (after entering the loop)
    ok = ensure_publish_begin_time(),
    Flags   = [{qos, proplists:get_value(qos, Opts)},
               {retain, proplists:get_value(retain, Opts)}],
    Size = proplists:get_value(payload_size, Opts),
    Payload0 = proplists:get_value(payload, Opts),
    Payload = case Payload0 of
                  {template, Bin} ->
                      Now = os:system_time(nanosecond),
                      TsNS = integer_to_binary(Now),
                      TsUS = integer_to_binary(erlang:convert_time_unit(Now, nanosecond, microsecond)),
                      TsMS = integer_to_binary(erlang:convert_time_unit(Now, nanosecond, millisecond)),
                      Unique = integer_to_binary(erlang:unique_integer()),
                      Substitutions =
                          #{ <<"%TIMESTAMP%">> => TsMS
                           , <<"%TIMESTAMPMS%">> => TsMS
                           , <<"%TIMESTAMPUS%">> => TsUS
                           , <<"%TIMESTAMPNS%">> => TsNS
                           , <<"%UNIQUE%">> => Unique
                           , <<"%RANDOM%">> => rand:bytes(Size)
                           },
                      maps:fold(
                        fun(Placeholder, Val, Acc) -> binary:replace(Acc, Placeholder, Val) end,
                        Bin,
                        Substitutions);
                  _ ->
                      Payload0
              end,
    %% prefix dynamic headers.
    NewPayload = maybe_prefix_payload(Payload, Opts),
    case emqtt:publish(Client, topic_opt(Opts), NewPayload, Flags) of
        ok -> ok;
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.


publish_topic(Client, Topic, #{ name := Topic
                              , qos := QoS
                              , inject_ts := TsUnit
                              , payload := PayloadTemplate
                              , stream := LogicStream
                              , stream_priority := StreamPriority
                              , payload_encoding := PayloadEncoding
                              }, ClientOpts) ->
   Payload1 = case TsUnit of
                   false -> PayloadTemplate;
                   _ -> PayloadTemplate#{<<"timestamp">> => erlang:system_time(TsUnit)}
                end,
   NewPayload =
      case PayloadEncoding of
         json -> jsx:encode(Payload1);
         eterm -> maybe_prefix_payload(term_to_binary(Payload1), ClientOpts)
      end,
   update_publish_start_at(Topic),
   case emqtt:publish_async(Client, via(LogicStream, #{priority => StreamPriority}),
                          feed_var(Topic, ClientOpts), #{}, NewPayload, [{qos, QoS}],
                          infinity,
                          {fun(Caller, Res) ->
                                 Caller ! {publish_async_res, Res}
                           end, [self()]
                          }
                         ) of
      ok -> ok;
      {ok, _} -> ok;
      {error, Reason} ->
         logger:error("Publish Topic: ~p Err: ~p", [Topic, Reason]),
         {error, Reason}
   end.


session_property_opts(Opts) ->
    case session_property_opts(Opts, #{}) of
        Empty when map_size(Empty) =:= 0 ->
            [];
        PropertyOpts ->
            [{properties, PropertyOpts}]
    end.

session_property_opts([{expiry, Exp}|Left], Props) ->
    session_property_opts(Left, Props#{'Session-Expiry-Interval' => Exp});
session_property_opts([_|Left], Props) ->
    session_property_opts(Left, Props);
session_property_opts([], Props) ->
    Props.

mqtt_opts(Opts) ->
    mqtt_opts(Opts, []).

mqtt_opts([], Acc) ->
    Acc;
mqtt_opts([{host, Host}|Opts], Acc) ->
    mqtt_opts(Opts, [{host, Host}|Acc]);
mqtt_opts([{port, Port}|Opts], Acc) ->
    mqtt_opts(Opts, [{port, Port}|Acc]);
mqtt_opts([{version, 3}|Opts], Acc) ->
    mqtt_opts(Opts, [{proto_ver, v3}|Acc]);
mqtt_opts([{version, 4}|Opts], Acc) ->
    mqtt_opts(Opts, [{proto_ver, v4}|Acc]);
mqtt_opts([{version, 5}|Opts], Acc) ->
    mqtt_opts(Opts, [{proto_ver, v5}|Acc]);
mqtt_opts([{username, Username}|Opts], Acc) ->
    mqtt_opts(Opts, [{username, list_to_binary(Username)}|Acc]);
mqtt_opts([{password, Password}|Opts], Acc) ->
    mqtt_opts(Opts, [{password, list_to_binary(Password)}|Acc]);
mqtt_opts([{keepalive, I}|Opts], Acc) ->
    mqtt_opts(Opts, [{keepalive, I}|Acc]);
mqtt_opts([{clean, Bool}|Opts], Acc) ->
    mqtt_opts(Opts, [{clean_start, Bool}|Acc]);
mqtt_opts([ssl|Opts], Acc) ->
    mqtt_opts(Opts, [{ssl, true}|Acc]);
mqtt_opts([{ssl, Bool}|Opts], Acc) ->
    mqtt_opts(Opts, [{ssl, Bool}|Acc]);
mqtt_opts([{lowmem, Bool}|Opts], Acc) ->
    mqtt_opts(Opts, [{low_mem, Bool} | Acc]);
mqtt_opts([{qoe, Bool}|Opts], Acc) ->
    mqtt_opts(Opts, [{with_qoe_metrics, Bool} | Acc]);
mqtt_opts([{inflight, InFlight0}|Opts], Acc) ->
    InFlight = case InFlight0 of
                   0 -> infinity;
                   _ -> InFlight0
               end,
    mqtt_opts(Opts, [{max_inflight, InFlight} | Acc]);
mqtt_opts([{retry_interval, IntervalSeconds}|Opts], Acc) ->
    mqtt_opts(Opts, [{retry_interval, IntervalSeconds}|Acc]);
mqtt_opts([{reconnect, Reconnect}|Opts], Acc) ->
   mqtt_opts(Opts, [{reconnect, Reconnect}|Acc]);
mqtt_opts([_|Opts], Acc) ->
    mqtt_opts(Opts, Acc).

tcp_opts(Opts) ->
    tcp_opts(Opts, []).
tcp_opts([], Acc) ->
    Acc;
tcp_opts([{lowmem, true} | Opts], Acc) ->
    tcp_opts(Opts, [{recbuf, 64} , {sndbuf, 64} | Acc]);
tcp_opts([{ifaddr, IfAddr} | Opts], Acc) ->
    case inet_parse:address(IfAddr) of
        {ok, IpAddr} ->
            tcp_opts(Opts, [{ip, IpAddr}|Acc]);
        {error, Reason} ->
            error({bad_ip_address, {IfAddr, Reason}})
    end;
tcp_opts([_|Opts], Acc) ->
    tcp_opts(Opts, Acc).

ssl_opts(Opts) ->
    ssl_opts(Opts, [{verify, verify_none}]).
ssl_opts([], Acc) ->
    [{ciphers, all_ssl_ciphers()} | Acc];
ssl_opts([{host, Host} | Opts], Acc) ->
    ssl_opts(Opts, [{server_name_indication, Host} | Acc]);
ssl_opts([{keyfile, KeyFile} | Opts], Acc) ->
    ssl_opts(Opts, [{keyfile, KeyFile}|Acc]);
ssl_opts([{certfile, CertFile} | Opts], Acc) ->
    ssl_opts(Opts, [{certfile, CertFile}|Acc]);
ssl_opts([{cacertfile, CaCertFile} | Opts], Acc) ->
    ssl_opts(Opts, [{cacertfile, CaCertFile}|Acc]);
ssl_opts([_|Opts], Acc) ->
    ssl_opts(Opts, Acc).

all_ssl_ciphers() ->
    Vers = ['tlsv1', 'tlsv1.1', 'tlsv1.2', 'tlsv1.3'],
    lists:usort(lists:concat([ssl:cipher_suites(all, Ver) || Ver <- Vers])).

-spec connect_fun(proplists:proplist()) -> FunName :: atom().
connect_fun(Opts)->
    case {proplists:get_bool(ws, Opts), is_quic(Opts)} of
        {true, true} ->
            throw({error, "unsupported transport: ws over quic "});
        {true, false} ->
            ws_connect;
        {false, true} ->
            quic_connect;
        {false, false} ->
            connect
    end.

client_id(PubSub, N, Opts) ->
    Prefix = client_id_prefix(PubSub, Opts),
    iolist_to_binary([Prefix, integer_to_list(N)]).

client_id_prefix(PubSub, Opts) ->
    case {proplists:get_value(shortids, Opts), proplists:get_value(prefix, Opts)} of
        {false, P} when P =:= undefined orelse P =:= "" ->
            Rand = rand:uniform(16#FFFFFFFF),
            lists:concat([host_prefix(Opts), "_bench_", PubSub, "_", Rand, "_"]);
        {false, Val} ->
            lists:concat([Val, "_bench_", PubSub, "_"]);
        {true, Pref} when Pref =:= undefined orelse Pref =:= "" ->
            "";
        {true, Prefix} ->
            Prefix
    end.

host_prefix(Opts) ->
    case proplists:get_value(ifaddr, Opts) of
        undefined ->
            {ok, Host} = inet:gethostname(),
            Host;
        IfAddr ->
            IfAddr
    end.

topics_opt(Opts) ->
    Topics = topics_opt(Opts, []),
    [feed_var(bin(Topic), Opts) || Topic <- Topics].

topics_opt([], Acc) ->
    Acc;
topics_opt([{topic, Topic}|Topics], Acc) ->
    topics_opt(Topics, [Topic | Acc]);
topics_opt([_Opt|Topics], Acc) ->
    topics_opt(Topics, Acc).

topic_opt(Opts) ->
    feed_var(bin(proplists:get_value(topic, Opts)), Opts).

feed_var(Topic, Opts) when is_binary(Topic) ->
    PropsT = [{Var, bin(proplists:get_value(Key, Opts))} || {Key, Var} <-
                [{seq, <<"%i">>}, {client_id, <<"%c">>}, {username, <<"%u">>}]],
    Props = [{<<"%s">>, bin(get_counter(pub) + 1)} | PropsT],
    lists:foldl(fun({_Var, undefined}, Acc) -> Acc;
                   ({Var, Val}, Acc) -> feed_var(Var, Val, Acc)
                end, Topic, Props).

feed_var(Var, Val, Topic) ->
    feed_var(Var, Val, words(Topic), []).
feed_var(_Var, _Val, [], Acc) ->
    join(lists:reverse(Acc));
feed_var(Var, Val, [Var|Words], Acc) ->
    feed_var(Var, Val, Words, [Val|Acc]);
feed_var(Var, Val, [W|Words], Acc) ->
    feed_var(Var, Val, Words, [W|Acc]).

words(Topic) when is_binary(Topic) ->
    [word(W) || W <- binary:split(Topic, <<"/">>, [global])].

word(<<>>)    -> '';
word(<<"+">>) -> '+';
word(<<"#">>) -> '#';
word(Bin)     -> Bin.

join([]) ->
    <<>>;
join([W]) ->
    bin(W);
join(Words) ->
    {_, Bin} =
    lists:foldr(fun(W, {true, Tail}) ->
                        {false, <<W/binary, Tail/binary>>};
                   (W, {false, Tail}) ->
                        {false, <<W/binary, "/", Tail/binary>>}
                end, {true, <<>>}, [bin(W) || W <- Words]),
    Bin.

bin(A) when is_atom(A)   -> bin(atom_to_list(A));
bin(I) when is_integer(I)-> bin(integer_to_list(I));
bin(S) when is_list(S)   -> list_to_binary(S);
bin(B) when is_binary(B) -> B;
bin(undefined)           -> undefined.

replace_opts(Opts, NewOpts) ->
    lists:foldl(fun({K, _V} = This, Acc) ->
                        lists:keyreplace(K, 1, Acc, This)
                end , Opts, NewOpts).

%% trim opts to save proc stack mem.
loop_opts(Opts) ->
    lists:filter(fun
                     ({K,__V}) ->
                         lists:member(K, [ interval_of_msg
                                         , payload
                                         , payload_size
                                         , payload_hdrs
                                         , topics_payload
                                         , qos
                                         , retain
                                         , topic
                                         , client_id
                                         , username
                                         , lowmem
                                         , limit_fun
                                         , seq
                                         , publish_signal_mref
                                         , pub_start_wait
                                         ]);
                     (K) -> K =:= prometheus
                 end, Opts).

-spec maybe_start_quicer() -> boolean().
maybe_start_quicer() ->
    case is_quicer_supp() of
        true ->
            case application:ensure_all_started(quicer) of
                {error, {quicer, {"no such file or directory", _}}} -> false;
                {ok, _} -> true
            end;
        false ->
            false
    end.

-spec is_quicer_supp() -> boolean().
is_quicer_supp() ->
    not (is_centos_6()
         orelse is_win32()
         orelse is_quicer_disabled()).

is_quicer_disabled() ->
    false =/= os:getenv("BUILD_WITHOUT_QUIC").

is_centos_6() ->
    case file:read_file("/etc/centos-release") of
        {ok, <<"CentOS release 6", _/binary >>} ->
            true;
        _ ->
            false
    end.

is_win32() ->
    win32 =:= element(1, os:type()).

random_pub_wait_period(Opts) ->
    MaxRandomPubWaitMS = proplists:get_value(max_random_wait, Opts, 0),
    MinRandomPubWaitMS = proplists:get_value(min_random_wait, Opts, 0),
    case MaxRandomPubWaitMS - MinRandomPubWaitMS of
        Period when Period =< 0 -> MinRandomPubWaitMS;
        Period -> MinRandomPubWaitMS - 1 + rand:uniform(Period)
    end.

maybe_spawn_gc_enforcer(Opts) ->
    LowMemMode = proplists:get_bool(lowmem, Opts),
    ForceMajorGCInterval = proplists:get_value(force_major_gc_interval, Opts, 0),
    case {LowMemMode, ForceMajorGCInterval} of
        {false, _} ->
            ignore;
        {true, 0} ->
            ignore;
        {true, Interval} when Interval > 0 ->
            spawn(fun MajorGC () ->
                          timer:sleep(Interval),
                          lists:foreach(
                            fun(P) -> erlang:garbage_collect(P, [{type, major}]) end,
                            processes()),
                          MajorGC()
                  end);
        {true, _} ->
            ignore
    end.

-spec addr_to_list(string() | undefined) -> [Ipstr::string() | undefined].
addr_to_list(Input) ->
    case Input =/= undefined andalso lists:member($,, Input) of
        false ->
            [Input];
        true ->
            string:tokens(Input, ",")
    end.

-spec shard_addr(non_neg_integer(), [Ipstr::string()]) -> Ipstr::string().
shard_addr(N, AddrList) ->
    Offset = N rem length(AddrList),
    lists:nth(Offset + 1, AddrList).

-spec quic_opts(proplists:proplist(), binary()) -> {proplists:proplist(), proplists:proplist()}.
quic_opts(Opts, ClientId) ->
   Nst = quic_opts_nst(Opts, ClientId),
   case proplists:get_value(quic, Opts, undefined) of
      undefined ->
         [];
      false ->
         [];
      true ->
         Nst;
      [] ->
         Nst;
      [{ConnOpts, StrmOpts}]
        when is_list(ConnOpts) andalso is_list(StrmOpts)->
         {Nst++ConnOpts, StrmOpts}
   end.

quic_opts_nst(Opts, ClientId) when is_binary(ClientId) ->
   case proplists:get_value(nst_dets_file, Opts, undefined) of
      undefined -> [];
      _Filename ->
         case ets:lookup(quic_clients_nsts, ClientId) of
            [{ClientId, Ticket}] ->
               [{nst, Ticket}];
            [] ->
               []
         end
   end.

-spec p95([integer()]) -> integer().
p95(List)->
   percentile(List, 0.95).
percentile(Input, P) ->
   Len = length(Input),
   Pos = ceil(Len * P),
   lists:nth(Pos, lists:sort(Input)).

-spec prepare_quic_nst(proplists:proplist()) -> ok | skip.
prepare_quic_nst(Opts)->
   %% Create ets table for 0-RTT session tickets
   ets:new(quic_clients_nsts, [named_table, public, ordered_set,
                               {write_concurrency, true},
                               {read_concurrency,true}]),
   %% Load session tickets from dets file if specified.
   case proplists:get_value(nst_dets_file, Opts, undefined) of
      undefined ->
         skip;
      Filename ->
         {ok, _DRef} = dets:open_file(dets_quic_nsts, [{file, Filename}]),
         true = ets:from_dets(quic_clients_nsts, dets_quic_nsts),
         ok
   end.

-spec counters() -> {atom(), integer()}.
counters() ->
   Names = [ publish_latency
           , recv
           , sub
           , sub_fail
           , pub
           , pub_fail
           , pub_overrun
           , pub_succ
           , connect_succ
           , connect_fail
           , connect_retried
           , reconnect_succ
           , unreachable
           , connection_refused
           , connection_timeout
           , connection_idle
           ],
   Idxs = lists:seq(2, length(Names) + 1),
   lists:zip(Names, Idxs).

maybe_init_prometheus(false) ->
    ok;
maybe_init_prometheus(true) ->
    Collectors = [ prometheus_boolean,
                   prometheus_counter,
                   prometheus_gauge,
                   prometheus_histogram,
                   prometheus_quantile_summary,
                   prometheus_summary],
    application:set_env(prometheus, collectors, Collectors),
    {ok, _} = application:ensure_all_started(prometheus),
    Counters = [ publish_latency
               , recv
               , sub
               , sub_fail
               , pub
               , pub_fail
               , pub_overrun
               , pub_succ
               , connect_succ
               , connect_fail
               , connect_retried
               , reconnect_succ
               , unreachable
               , connection_refused
               , connection_timeout
               , connection_idle
               ],
    lists:foreach(
      fun(Cnt) ->
              prometheus_counter:declare([{name, Cnt}, {help, atom_to_list(Cnt)}])
      end, Counters),
    prometheus_histogram:declare([{name, mqtt_client_handshake_duration},
                                  {labels, []},
                                  {buckets, [1, 3, 5, 10, 20]},
                                  {help, "Handshake duration of MQTT client"}]),
    prometheus_histogram:declare([{name, mqtt_client_connect_duration},
                                  {labels, []},
                                  {buckets, [1, 10, 25, 50, 100]},
                                  {help, "Connect duration of MQTT client"}]),
    prometheus_histogram:declare([{name, mqtt_client_subscribe_duration},
                                  {labels, []},
                                  {buckets, [1, 10, 25, 50, 100]},
                                  {help, "Subscribe duration of MQTT client"}]),
    prometheus_histogram:declare([{name, e2e_latency},
                                  {buckets, [1, 5, 10, 25, 50, 100, 500, 1000]},
                                  {help, "End-to-end latency"}]).

%% @doc Check received payload headers
-spec maybe_check_payload_hdrs(Prometheus :: boolean(), Payload :: binary(), Hdrs :: [string()]) -> ok.
maybe_check_payload_hdrs(_, {template, _Bin}, _) ->
    ok;
maybe_check_payload_hdrs(_, _Bin, []) ->
   ok;
maybe_check_payload_hdrs(Prometheus, << TS:64/integer, BinL/binary >>, [?hdr_ts | RL]) ->
   E2ELatency = os:system_time(millisecond) - TS,
   E2ELatency > 0 andalso inc_counter(Prometheus, publish_latency, E2ELatency),
   E2ELatency > 0 andalso histogram_observe(Prometheus, e2e_latency, E2ELatency),
   maybe_check_payload_hdrs(Prometheus, BinL, RL);
maybe_check_payload_hdrs(Prometheus, << Cnt:64/integer, BinL/binary >>, [?hdr_cnt64 | RL]) ->
   case put(payload_hdr_cnt64, Cnt) of
      undefined ->
         ok;
      Old when Cnt - 1 == Old ->
         maybe_check_payload_hdrs(Prometheus, BinL, RL);
      Old ->
         throw({err_payload_hdr_cnt64, Old, Cnt})
   end.

-spec with_payload_headers([string()], binary()) -> binary().
with_payload_headers([], Bin) ->
   Bin;
with_payload_headers(Hdrs, Bin) ->
   prefix_payload_headers(Hdrs, Bin, []).

prefix_payload_headers([], Bin, HeadersBin) ->
   iolist_to_binary(lists:reverse([Bin | HeadersBin]));
prefix_payload_headers([?hdr_ts | T], Bin, AccHeaderBin) ->
   TsNow = os:system_time(millisecond),
   prefix_payload_headers(T, Bin, [ << TsNow:64/integer >> | AccHeaderBin ]);
prefix_payload_headers([?hdr_cnt64 | T], Bin, AccHeaderBin) ->
   New = case get(payload_hdr_cnt64) of
            undefined ->
               0;
            Cnt ->
               Cnt + 1
         end,
   put(payload_hdr_cnt64, New),
   prefix_payload_headers(T, Bin, [ << New:64/integer >> | AccHeaderBin ]).

-spec parse_payload_hdrs(proplists:proplist()) -> [string()].
parse_payload_hdrs(Opts)->
   Res = string:tokens(proplists:get_value(payload_hdrs, Opts, []), ","),
   ok = validate_payload_hdrs(Res),
   Res.

-spec validate_payload_hdrs([string()]) -> ok | no_return().
validate_payload_hdrs([]) ->
   ok;
validate_payload_hdrs([Hdr | T]) ->
   case lists:member(Hdr, [?hdr_cnt64, ?hdr_ts]) of
      true ->
         validate_payload_hdrs(T);
      false ->
         error({unsupp_payload_hdr, Hdr})
   end.

maybe_start_restapi(disabled) ->
    ok;
maybe_start_restapi("disabled") ->
    ok;
maybe_start_restapi(RestAPI) ->
    {IP, Port} =
        case string:split(RestAPI, ":") of
            [IP0, Port0] ->
                {Port1, _} = string:to_integer(Port0),
                {ok, IP1} = inet:parse_ipv4_address(IP0),
                {IP1, Port1};
            [Port0] ->
                {Port1, _} = string:to_integer(Port0),
                {{0, 0, 0, 0}, Port1}
        end,
    TransportOpts = #{ip => IP, port => Port},
    Env = #{dispatch => dispatch()},
    ProtocolOpts = #{env => Env},
    application:ensure_all_started(cowboy),
    cowboy:start_clear(rest_api, maps:to_list(TransportOpts), ProtocolOpts).

dispatch() ->
    cowboy_router:compile([{'_', routes()}]).

routes() ->
    [ {"/metrics", emqtt_bench_http_metrics, []}
    ].

histogram_observe(false, _, _) ->
    ok;
histogram_observe(true, Metric, Value) ->
    prometheus_histogram:observe(Metric, Value).

-spec parse_topics_payload(proplists:proplist()) ->
         undefined | #{Name :: string() =>  #{name := string(),
                                              interval_ms := non_neg_integer(),
                                              qos := 0 | 1 |2,
                                              render_field := undefined | binary(),
                                              inject_ts := false | second | millisecond | microsecond | nanosecond,
                                              payload := map()
                                             }}.
parse_topics_payload(Opts) ->
   case proplists:get_value(topics_payload, Opts) of
      undefined -> undefined;
      Filename ->
         {ok, Content} = file:read_file(Filename),
         #{<<"topics">> := TopicSpecs} = jsx:decode(Content),
         lists:foldl(fun(#{ <<"name">> := TopicName,
                            <<"inject_timestamp">> := WithTS,
                            <<"interval_ms">> := IntervalMS,
                            <<"QoS">> := QoS,
                            <<"stream">> := StreamId,
                            <<"stream_priority">> := StreamPriority,
                            <<"payload_encoding">> := PayloadEncoding,
                            <<"payload">> := Payload} = Spec, Acc) ->
                           RFieldName = maps:get(<<"render">>, Spec, undefined),
                           TSUnit = case WithTS of
                                       <<"s">> -> second;
                                       <<"ms">> -> millisecond;
                                       <<"us">> -> microsecond;
                                       <<"ns">> -> nanosecond;
                                       true -> millisecond;
                                       false -> false
                                    end,
                           Acc#{TopicName =>
                                   #{ name => TopicName
                                    , interval_ms => binary_to_integer(IntervalMS)
                                    , inject_ts => TSUnit
                                    , qos => QoS
                                    , render_field => RFieldName
                                    , payload => Payload
                                    , payload_encoding => binary_to_atom(PayloadEncoding)
                                    , stream => StreamId
                                    , stream_priority => StreamPriority
                                    }}
                     end, #{} ,TopicSpecs)
   end.

-spec render_payload(TopicsPayload :: undefined | map(), Opts :: proplists:proplist()) -> NewTopicPayload :: undefined | map().
render_payload(undefined, _Opts) ->
   undefined;
render_payload(TopicsMap, Opts) when is_map(TopicsMap) ->
   maps:map(fun(_K, V) -> do_render_payload(V, Opts) end, TopicsMap).

do_render_payload(#{render_field := undefined} = Spec, _Opts) ->
   Spec;
do_render_payload(#{payload := Payload, render_field := FieldName} = Spec, Opts) when is_binary(FieldName) ->
   Template = maps:get(FieldName, Payload, ""),
   SRs = [ {<<"%i">>, integer_to_binary(proplists:get_value(seq, Opts))}
         , {<<"%c">>, proplists:get_value(client_id, Opts)}
         , {<<"%p">>, fun shared_paddings/1}
         ],
   NewVal = lists:foldl(fun({<<"%p">>, Fun}, Acc) when is_function(Fun) ->
                              <<"%p", Pages/binary >> = Acc,
                              Fun(Pages);
                           ({Search, Replace}, Acc) ->
                              binary:replace(Acc, Search, Replace, [global])
                        end, Template, SRs),
   Spec#{payload := Payload#{FieldName := NewVal}}.

%% @doc shared_paddings, utilize binary ref for shallow copy
shared_paddings(Pages) when is_binary(Pages) ->
   shared_paddings(binary_to_integer(Pages));
shared_paddings(Pages) when is_integer(Pages) ->
   %% one page = 4KBytes
   case ets:lookup(?shared_padding_tab, Pages) of
      [{_, Payload}] ->
         Payload;
      [] ->
         Payload = << <<N:64>> || N <- lists:seq(1, Pages*512) >>,
         ets:insert(?shared_padding_tab, {Pages, Payload})
   end,
   Payload.

%% @doc send via which QUIC stream.
via(0, _ClientOpts)->
   default;
via(LogicStreamId, ClientOpts)->
   {logic_stream_id, LogicStreamId, ClientOpts}.

%% @doc add payload headers
maybe_prefix_payload(Payload, ClientOpts) ->
   case proplists:get_value(payload_hdrs, ClientOpts, []) of
      [] -> Payload;
      PayloadHdrs ->
         with_payload_headers(PayloadHdrs, Payload)
   end.

-spec is_quic(proplists:proplist()) -> boolean().
is_quic(Opts) ->
   proplists:get_value(quic, Opts) =/= false.

quic_opts_from_arg(Opts)->
   case proplists:get_value(quic, Opts) of
      V when is_boolean(V) ->
         V;
      "false" ->
         false;
      "true" ->
         true;
      V when is_list(V) ->
         case filename:extension(V) of
            ".eterm" ->
               case file:consult(V) of
                  {ok, ConnOpts} ->
                     ConnOpts;
                  {error, enoent} ->
                     error({"--quic "++ V, no_exists})
               end;
            _ ->
               error("bad --quic")
         end
   end.

prepare_quicer(Opts) ->
   case quic_opts_from_arg(Opts) =/= false of
      true ->
         maybe_start_quicer() orelse error({quic, not_supp_or_disabled}),
         prepare_quic_nst(Opts);
      _ ->
         ok
   end.

maybe_update_client_qoe(Client, Opts) ->
   ClientInfo = emqtt:info(Client),
   case proplists:get_value(qoe, ClientInfo, false) of
      false ->
         ok;
      QoEMap when is_map(QoEMap) ->
         Prometheus = lists:member(prometheus, Opts),
         ClientId = proplists:get_value(clientid, ClientInfo),
         qoe_store_insert(Prometheus, ClientId, QoEMap)
   end.

-define(invalid_elapsed, -1).
qoe_store_insert(Prometheus,
                 ClientId,
                 #{ initialized := StartTs
                  , handshaked := HSTs
                  , connected := ConnTs
                  } = QoE) ->
   ElapsedHandshake = HSTs - StartTs,
   ElapsedConn = ConnTs - StartTs,
   ElapsedSub = case maps:get(subscribed, QoE, undefined) of
                   undefined ->
                      %% invalid
                      ?invalid_elapsed;
                   SubTs ->
                      SubTs - StartTs
                end,
   histogram_observe(Prometheus, mqtt_client_handshake_duration, ElapsedHandshake),
   histogram_observe(Prometheus, mqtt_client_connect_duration, ElapsedConn),
   histogram_observe(Prometheus, mqtt_client_subscribe_duration, ElapsedSub),
   Term = {ClientId, {StartTs, ElapsedHandshake, ElapsedConn, ElapsedSub}},
   true = ets:insert(qoe_store, Term),
   ok.

init_qoe(Opts) ->
   put(is_qoe_dlog, false),
   case proplists:get_value(qoe, Opts) of
      false ->
         skip;
      dump ->
         case proplists:get_value(qoelog, Opts) of
            "" ->
               error("qoelog not specified");
            File ->
               ok = open_qoe_disklog(File),
               qoe_disklog_to_csv(File),
               disk_log:close(?QoELog),
               erlang:halt()
          end;
      true ->
         ets:new(qoe_store, [named_table, public, set]),
         %% QoE disk Log file
         case proplists:get_value(qoelog, Opts) of
            "" ->
               skip;
            File ->
               put(is_qoe_dlog, true),
               open_qoe_disklog(File)
         end
      end.

open_qoe_disklog(File) ->
   case disk_log:open([{name, ?QoELog}, {file, File}, {repair, true}, {type, halt},
                       {size, infinity}]) of
      {ok, _Log} -> ok;
      {repaired, _Log, {recovered, Recov}, {badbytes, BadBytes}} ->
         io:format("Open QoElog: ~p, recovered ~p bytes,  ~p bad bytes~n",
                   [File, Recov, BadBytes]);
      {error, Reason} ->
         error({open_qoe_log, Reason})
   end.

qoe_disklog_to_csv(File) ->
   TargetFile = File++".csv",
   {ok, CsvH} = file:open(TargetFile, [write]),
   io:format("No execution, just dumping QoE dlog to csv file: ~p~n", [TargetFile]),
   file:write(CsvH, "ClientId,TS,Handshake,Connect,Subscribe\n"),
   Writer = fun(Terms) ->
                  Lines = lists:map(
                            fun([ClientId, TS, Handshake, Connect, Subscribe]) ->
                                  io_lib:format("~s,~p,~p,~p,~p~n",
                                                [ClientId, TS, Handshake, Connect, Subscribe])
                            end, Terms),
                  file:write(CsvH, Lines)
            end,
   do_qoe_disklog_to_csv(?QoELog, start, Writer),
   file:close(CsvH).

do_qoe_disklog_to_csv(Log, Cont0, Writer) ->
   case disk_log:chunk(Log, Cont0) of
      {Cont, Terms} ->
         Writer(Terms),
         do_qoe_disklog_to_csv(Log, Cont, Writer);
      {Cont, Terms, _BadBytes} ->
         Writer(Terms),
         do_qoe_disklog_to_csv(Log, Cont, Writer);
      eof ->
         ok
   end.

maybe_dlog_qoe(Data) ->
   Offset = erlang:time_offset(millisecond),
   DLog = [ [ClientId, Offset + StartTs,
             ElapsedHandshake, ElapsedConn, ElapsedSub]
            || {ClientId, {StartTs, ElapsedHandshake, ElapsedConn, ElapsedSub}} <- Data],
   ok = disk_log:log_terms(?QoELog, DLog).
