%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_persistent_session_ds_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-include_lib("emqx/src/emqx_persistent_session_ds.hrl").

-define(DEFAULT_KEYSPACE, default).
-define(DS_SHARD_ID, <<"local">>).
-define(DS_SHARD, {?DEFAULT_KEYSPACE, ?DS_SHARD_ID}).

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    TCApps = emqx_cth_suite:start(
        app_specs(),
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{tc_apps, TCApps} | Config].

end_per_suite(Config) ->
    TCApps = ?config(tc_apps, Config),
    emqx_cth_suite:stop(TCApps),
    ok.

init_per_testcase(TestCase, Config) when
    TestCase =:= t_session_subscription_idempotency;
    TestCase =:= t_session_unsubscription_idempotency
->
    Cluster = cluster(#{n => 1}),
    ClusterOpts = #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)},
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(Cluster, ClusterOpts),
    Nodes = emqx_cth_cluster:start(Cluster, ClusterOpts),
    [
        {cluster, Cluster},
        {node_specs, NodeSpecs},
        {cluster_opts, ClusterOpts},
        {nodes, Nodes}
        | Config
    ];
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(TestCase, Config) when
    TestCase =:= t_session_subscription_idempotency;
    TestCase =:= t_session_unsubscription_idempotency
->
    Nodes = ?config(nodes, Config),
    emqx_common_test_helpers:call_janitor(60_000),
    ok = emqx_cth_cluster:stop(Nodes),
    ok;
end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(60_000),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

cluster(#{n := N}) ->
    Spec = #{role => core, apps => app_specs()},
    lists:map(
        fun(M) ->
            Name = list_to_atom("ds_SUITE" ++ integer_to_list(M)),
            {Name, Spec}
        end,
        lists:seq(1, N)
    ).

app_specs() ->
    [
        emqx_durable_storage,
        {emqx, "persistent_session_store = {ds = true}"}
    ].

get_mqtt_port(Node, Type) ->
    {_IP, Port} = erpc:call(Node, emqx_config, get, [[listeners, Type, default, bind]]),
    Port.

get_all_iterator_ids(Node) ->
    Fn = fun(K, _V, Acc) -> [K | Acc] end,
    erpc:call(Node, fun() ->
        emqx_ds_storage_layer:foldl_iterator_prefix(?DS_SHARD, <<>>, Fn, [])
    end).

wait_nodeup(Node) ->
    ?retry(
        _Sleep0 = 500,
        _Attempts0 = 50,
        pong = net_adm:ping(Node)
    ).

wait_gen_rpc_down(_NodeSpec = #{apps := Apps}) ->
    #{override_env := Env} = proplists:get_value(gen_rpc, Apps),
    Port = proplists:get_value(tcp_server_port, Env),
    ?retry(
        _Sleep0 = 500,
        _Attempts0 = 50,
        false = emqx_common_test_helpers:is_tcp_server_available("127.0.0.1", Port)
    ).

start_client(Opts0 = #{}) ->
    Defaults = #{
        proto_ver => v5,
        properties => #{'Session-Expiry-Interval' => 300}
    },
    Opts = maps:to_list(emqx_utils_maps:deep_merge(Defaults, Opts0)),
    ct:pal("starting client with opts:\n  ~p", [Opts]),
    {ok, Client} = emqtt:start_link(Opts),
    on_exit(fun() -> catch emqtt:stop(Client) end),
    Client.

restart_node(Node, NodeSpec) ->
    ?tp(will_restart_node, #{}),
    ?tp(notice, "restarting node", #{node => Node}),
    true = monitor_node(Node, true),
    ok = erpc:call(Node, init, restart, []),
    receive
        {nodedown, Node} ->
            ok
    after 10_000 ->
        ct:fail("node ~p didn't stop", [Node])
    end,
    ?tp(notice, "waiting for nodeup", #{node => Node}),
    wait_nodeup(Node),
    wait_gen_rpc_down(NodeSpec),
    ?tp(notice, "restarting apps", #{node => Node}),
    Apps = maps:get(apps, NodeSpec),
    ok = erpc:call(Node, emqx_cth_suite, load_apps, [Apps]),
    _ = erpc:call(Node, emqx_cth_suite, start_apps, [Apps, NodeSpec]),
    %% have to re-inject this so that we may stop the node succesfully at the
    %% end....
    ok = emqx_cth_cluster:set_node_opts(Node, NodeSpec),
    ok = snabbkaffe:forward_trace(Node),
    ?tp(notice, "node restarted", #{node => Node}),
    ?tp(restarted_node, #{}),
    ok.

is_persistent_connect_opts(#{properties := #{'Session-Expiry-Interval' := EI}}) ->
    EI > 0.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_qos0(_Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    SubTopicFilter = <<"t/#">>,
    Subscriber = start_client(#{
        clientid => ClientId,
        properties => #{'Session-Expiry-Interval' => 30}
    }),
    {ok, _} = emqtt:connect(Subscriber),
    Publisher = start_client(#{
        clientid => "publisher", properties => #{'Session-Expiry-Interval' => 0}
    }),
    {ok, _} = emqtt:connect(Publisher),
    %% 1. Subscribe as QoS1:
    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(Subscriber, SubTopicFilter, qos1),
    %% 2. Publish QoS0 messages (they should be delivered as QoS0):
    ok = emqtt:publish(Publisher, <<"t/1">>, <<"hello1">>, 0),
    ok = emqtt:publish(Publisher, <<"t/2">>, <<"hello2">>, 0),
    ?assertMatch(
        [
            #{qos := 0, topic := <<"t/1">>, payload := <<"hello1">>},
            #{qos := 0, topic := <<"t/2">>, payload := <<"hello2">>}
        ],
        receive_messages(2)
    ).

t_non_persistent_session_subscription(_Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    SubTopicFilter = <<"t/#">>,
    ?check_trace(
        begin
            ?tp(notice, "starting", #{}),
            Client = start_client(#{
                clientid => ClientId,
                properties => #{'Session-Expiry-Interval' => 0}
            }),
            {ok, _} = emqtt:connect(Client),
            ?tp(notice, "subscribing", #{}),
            {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(Client, SubTopicFilter, qos2),

            ok = emqtt:stop(Client),

            ok
        end,
        fun(Trace) ->
            ct:pal("trace:\n  ~p", [Trace]),
            ?assertEqual([], ?of_kind(ds_session_subscription_added, Trace)),
            ok
        end
    ),
    ok.

t_session_subscription_idempotency(Config) ->
    [Node1Spec | _] = ?config(node_specs, Config),
    [Node1] = ?config(nodes, Config),
    Port = get_mqtt_port(Node1, tcp),
    SubTopicFilter = <<"t/+">>,
    ClientId = <<"myclientid">>,
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := persistent_session_ds_subscription_added},
                _NEvents0 = 1,
                #{?snk_kind := will_restart_node},
                _Guard0 = true
            ),
            ?force_ordering(
                #{?snk_kind := restarted_node},
                _NEvents1 = 1,
                #{?snk_kind := persistent_session_ds_open_iterators, ?snk_span := start},
                _Guard1 = true
            ),

            spawn_link(fun() -> restart_node(Node1, Node1Spec) end),

            ?tp(notice, "starting 1", #{}),
            Client0 = start_client(#{port => Port, clientid => ClientId}),
            {ok, _} = emqtt:connect(Client0),
            ?tp(notice, "subscribing 1", #{}),
            process_flag(trap_exit, true),
            catch emqtt:subscribe(Client0, SubTopicFilter, qos2),
            receive
                {'EXIT', {shutdown, _}} ->
                    ok
            after 100 -> ok
            end,
            process_flag(trap_exit, false),

            {ok, _} = ?block_until(#{?snk_kind := restarted_node}, 15_000),
            ?tp(notice, "starting 2", #{}),
            Client1 = start_client(#{port => Port, clientid => ClientId}),
            {ok, _} = emqtt:connect(Client1),
            ?tp(notice, "subscribing 2", #{}),
            {ok, _, [2]} = emqtt:subscribe(Client1, SubTopicFilter, qos2),

            ok = emqtt:stop(Client1),

            ok
        end,
        fun(Trace) ->
            ct:pal("trace:\n  ~p", [Trace]),
            SubTopicFilterWords = emqx_topic:words(SubTopicFilter),
            ?assertMatch(
                {ok, #{}, #{SubTopicFilterWords := #{}}},
                erpc:call(Node1, emqx_persistent_session_ds, session_open, [ClientId])
            )
        end
    ),
    ok.

%% Check that we close the iterators before deleting the iterator id entry.
t_session_unsubscription_idempotency(Config) ->
    [Node1Spec | _] = ?config(node_specs, Config),
    [Node1] = ?config(nodes, Config),
    Port = get_mqtt_port(Node1, tcp),
    SubTopicFilter = <<"t/+">>,
    ClientId = <<"myclientid">>,
    ?check_trace(
        begin
            ?force_ordering(
                #{
                    ?snk_kind := persistent_session_ds_subscription_delete,
                    ?snk_span := {complete, _}
                },
                _NEvents0 = 1,
                #{?snk_kind := will_restart_node},
                _Guard0 = true
            ),
            ?force_ordering(
                #{?snk_kind := restarted_node},
                _NEvents1 = 1,
                #{?snk_kind := persistent_session_ds_subscription_route_delete, ?snk_span := start},
                _Guard1 = true
            ),

            spawn_link(fun() -> restart_node(Node1, Node1Spec) end),

            ?tp(notice, "starting 1", #{}),
            Client0 = start_client(#{port => Port, clientid => ClientId}),
            {ok, _} = emqtt:connect(Client0),
            ?tp(notice, "subscribing 1", #{}),
            {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(Client0, SubTopicFilter, qos2),
            ?tp(notice, "unsubscribing 1", #{}),
            process_flag(trap_exit, true),
            catch emqtt:unsubscribe(Client0, SubTopicFilter),
            receive
                {'EXIT', {shutdown, _}} ->
                    ok
            after 100 -> ok
            end,
            process_flag(trap_exit, false),

            {ok, _} = ?block_until(#{?snk_kind := restarted_node}, 15_000),
            ?tp(notice, "starting 2", #{}),
            Client1 = start_client(#{port => Port, clientid => ClientId}),
            {ok, _} = emqtt:connect(Client1),
            ?tp(notice, "subscribing 2", #{}),
            {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(Client1, SubTopicFilter, qos2),
            ?tp(notice, "unsubscribing 2", #{}),
            {{ok, _, [?RC_SUCCESS]}, {ok, _}} =
                ?wait_async_action(
                    emqtt:unsubscribe(Client1, SubTopicFilter),
                    #{
                        ?snk_kind := persistent_session_ds_subscription_route_delete,
                        ?snk_span := {complete, _}
                    },
                    15_000
                ),

            ok = emqtt:stop(Client1),

            ok
        end,
        fun(Trace) ->
            ct:pal("trace:\n  ~p", [Trace]),
            ?assertMatch(
                {ok, #{}, Subs = #{}} when map_size(Subs) =:= 0,
                erpc:call(Node1, emqx_persistent_session_ds, session_open, [ClientId])
            ),
            ok
        end
    ),
    ok.

t_session_discard_persistent_to_non_persistent(_Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Params = #{
        client_id => ClientId,
        reconnect_opts =>
            #{
                clean_start => true,
                %% we set it to zero so that a new session is not created.
                properties => #{'Session-Expiry-Interval' => 0},
                proto_ver => v5
            }
    },
    do_t_session_discard(Params).

t_session_discard_persistent_to_persistent(_Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Params = #{
        client_id => ClientId,
        reconnect_opts =>
            #{
                clean_start => true,
                properties => #{'Session-Expiry-Interval' => 30},
                proto_ver => v5
            }
    },
    do_t_session_discard(Params).

do_t_session_discard(Params) ->
    #{
        client_id := ClientId,
        reconnect_opts := ReconnectOpts0
    } = Params,
    ReconnectOpts = ReconnectOpts0#{clientid => ClientId},
    SubTopicFilter = <<"t/+">>,
    ?check_trace(
        begin
            ?tp(notice, "starting", #{}),
            Client0 = start_client(#{
                clientid => ClientId,
                clean_start => false,
                properties => #{'Session-Expiry-Interval' => 30},
                proto_ver => v5
            }),
            {ok, _} = emqtt:connect(Client0),
            ?tp(notice, "subscribing", #{}),
            {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(Client0, SubTopicFilter, qos2),
            %% Store some matching messages so that streams and iterators are created.
            ok = emqtt:publish(Client0, <<"t/1">>, <<"1">>),
            ok = emqtt:publish(Client0, <<"t/2">>, <<"2">>),
            ?retry(
                _Sleep0 = 100,
                _Attempts0 = 50,
                true = map_size(emqx_persistent_session_ds:list_all_streams()) > 0
            ),
            ?retry(
                _Sleep0 = 100,
                _Attempts0 = 50,
                true = map_size(emqx_persistent_session_ds:list_all_iterators()) > 0
            ),
            ok = emqtt:stop(Client0),
            ?tp(notice, "disconnected", #{}),

            ?tp(notice, "reconnecting", #{}),
            %% we still have iterators and streams
            ?assert(map_size(emqx_persistent_session_ds:list_all_streams()) > 0),
            ?assert(map_size(emqx_persistent_session_ds:list_all_iterators()) > 0),
            Client1 = start_client(ReconnectOpts),
            {ok, _} = emqtt:connect(Client1),
            ?assertEqual([], emqtt:subscriptions(Client1)),
            case is_persistent_connect_opts(ReconnectOpts) of
                true ->
                    ?assertMatch(#{ClientId := _}, emqx_persistent_session_ds:list_all_sessions());
                false ->
                    ?assertEqual(#{}, emqx_persistent_session_ds:list_all_sessions())
            end,
            ?assertEqual(#{}, emqx_persistent_session_ds:list_all_subscriptions()),
            ?assertEqual([], emqx_persistent_session_ds_router:topics()),
            ?assertEqual(#{}, emqx_persistent_session_ds:list_all_streams()),
            ?assertEqual(#{}, emqx_persistent_session_ds:list_all_iterators()),
            ok = emqtt:stop(Client1),
            ?tp(notice, "disconnected", #{}),

            ok
        end,
        fun(Trace) ->
            ct:pal("trace:\n  ~p", [Trace]),
            ok
        end
    ),
    ok.

receive_messages(Count) ->
    receive_messages(Count, []).

receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            receive_messages(Count - 1, [Msg | Msgs]);
        _Other ->
            receive_messages(Count, Msgs)
    after 15000 ->
        Msgs
    end.
