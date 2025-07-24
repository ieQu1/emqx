%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_client_tests).

-compile(nowarn_export_all).
-compile(export_all).
-compile(noinline).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include("emqx_ds.hrl").
-include("../src/emqx_ds_client_internals.hrl").

-define(fake_shards, [<<"0">>, <<"12">>]).

%%================================================================================
%% Basic tests
%%================================================================================

%% Fake reference that can be created deterministically:
-define(watch_ref(SUBID, CTR), {w, SUBID, CTR}).
-define(sub_handle(SUBID, REF), {h, SUBID, REF}).
-define(sub_ref(SUBID, HANDLE), {s, SUBID, HANDLE}).

filter_effects_test() ->
    IsEven = fun(E) -> (E rem 2) =:= 0 end,
    GS = #cs{plan = [1, 2, 3, 4], retry = [5, 6, 7, 8]},
    ?assertMatch(
        #cs{plan = [2, 4], retry = [6, 8]},
        emqx_ds_client:filter_effects(IsEven, GS)
    ).

%%================================================================================
%% Host state
%%================================================================================

%% Test CBM state:
-record(test_host_state, {
    %% Current generations:
    generations = #{} :: #{{emqx_ds_client:sub_id(), emqx_ds:shard()} => emqx_ds:generation()},
    %% Saved replay positions:
    iterators = #{} :: #{
        {emqx_ds_client:sub_id(), emqx_ds:stream()} => emqx_ds:iteraator() | end_of_stream
    }
}).
-define(hs, emqx_ds_client_tests_hs).

-record(fake_stream, {shard, gen, id}).
-record(fake_iter, {stream, time}).

-record(test_ds_sub, {
    sref :: ?sub_ref(_, _),
    handle :: ?sub_handle(_, _),
    it :: #fake_iter{},
    seqno = 0 :: integer()
}).

get_current_generation(SubId, Shard, #test_host_state{generations = Gens}) ->
    maps:get({SubId, Shard}, Gens, 0).

on_advance_generation(
    SubId, Shard, NextGen, HS = #test_host_state{generations = Gens0}
) ->
    OldCurrent = get_current_generation(SubId, Shard, HS),
    ?tp(info, test_host_advance_generation, #{
        subid => SubId, shard => Shard, old => OldCurrent, new => NextGen
    }),
    ?assert(
        NextGen > OldCurrent,
        {"New generation should be greater than the old one", NextGen, '>', OldCurrent}
    ),
    Gens = Gens0#{{SubId, Shard} => NextGen},
    HS#test_host_state{
        generations = Gens
    }.

on_new_iterator(
    SubId, _Slab, Stream, It, HS = #test_host_state{iterators = Its}
) ->
    ?tp(test_host_new_iterator, #{subid => SubId, stream => Stream, it => It}),
    ?assertNot(maps:is_key({SubId, Stream}, Its), "Client should not re-create iterators"),
    {subscribe, host_set_iter(SubId, Stream, It, HS)}.

host_get_iter(SubId, Stream, #test_host_state{iterators = Its}) ->
    maps:get({SubId, Stream}, Its, undefined).

host_set_iter(SubId, Stream, It, HS = #test_host_state{iterators = Its}) ->
    HS#test_host_state{
        iterators = Its#{{SubId, Stream} => It}
    }.

on_unrecoverable_error(SubId, _Slab, Stream, Reason, HS = #test_host_state{}) ->
    ?tp(test_host_unrecoverable, #{subid => SubId, stream => Stream, reason => Reason}),
    HS.

on_subscription_down(SubId, _Slab, Stream, HS) ->
    ?tp(test_host_sub_down, #{subid => SubId, stream => Stream}),
    HS.

get_iterator(SubId, _Slab, Stream, #test_host_state{iterators = Its}) ->
    case Its of
        #{{SubId, Stream} := It} ->
            {subscribe, It};
        #{} ->
            undefined
    end.

%%================================================================================
%% Proper test
%%================================================================================

-define(test_sub_ids, [id1, id2]).

-define(err_get_streams, err_get_streams).
-define(err_make_iterator, err_make_iterator).
-define(err_subscribe, err_subscribe).

-type error_type() :: ?err_get_streams | ?err_make_iterator | ?err_subscribe.

-define(ws, emqx_ds_client_tests_ws).

%% "All the world's a stage", Shakespeare.
%%
%% State of the `fake_world' effect handler.
-record(world_stage, {
    %% Counters for making fake references:
    watch_ref_ctr = 0 :: integer(),
    sub_ref_ctr = 0 :: integer(),
    %% Active watches:
    watches = [] :: [?watch_ref(_, _)],
    %% Active subscriptions:
    ds_subs = [] :: [#test_ds_sub{}]
}).

%% Model state (updated by proper command generator):
-record(model_state, {
    %% Counter for creating unique IDs:
    counter = 0 :: integer(),
    current_generation = #{} :: #{emqx_ds:shard() => emqx_ds:generation()},
    streams = [] :: [#fake_stream{}],
    %% Mask of injected recoverable errors for the shards:
    err_rec = #{} :: #{{emqx_ds:shard(), error_type()} => true},
    subs = #{} :: #{emqx_ds_client:sub_id() => {_DB, _Topic, _Opts}},
    exists = false :: boolean(),
    runtime = {undefined, #test_host_state{}}
}).

current_gen(Shard, #model_state{current_generation = CG}) ->
    maps:get(Shard, CG, 0).

%%------------------------------------------------------------------------------
%% Proper generators
%%------------------------------------------------------------------------------

-define(call_wrapper(MS, FUN, ARGS), {call, ?MODULE, wrapper, [MS, FUN, ARGS]}).

gen_add_generation(MS) ->
    ?LET(
        {Shard, Delta},
        {oneof(?fake_shards), range(1, 4)},
        ?call_wrapper(MS, add_generation, [Shard, current_gen(Shard, MS) + Delta])
    ).

gen_del_generation(MS) ->
    ?LET(
        Shard,
        oneof(?fake_shards),
        ?LET(
            {Generation, Graceful},
            {range(-2, current_gen(Shard, MS)), boolean()},
            ?call_wrapper(MS, del_generation, [Shard, Generation, Graceful])
        )
    ).

gen_add_stream(MS = #model_state{counter = Ctr}) ->
    ?LET(
        Shard,
        oneof(?fake_shards),
        begin
            Stream = #fake_stream{shard = Shard, gen = current_gen(Shard, MS), id = Ctr},
            ?call_wrapper(MS, add_stream, [Stream])
        end
    ).

gen_new(MS) ->
    exactly(?call_wrapper(MS, fake_new, [])).

gen_destroy(MS = #model_state{}) ->
    exactly(?call_wrapper(MS, fake_destroy, [])).

gen_subscribe(MS) ->
    ?LET(
        Id,
        oneof(?test_sub_ids),
        ?call_wrapper(MS, fake_subscribe, [Id, test_db, [<<"test_topic">>]])
    ).

gen_unsubscribe(MS) ->
    ?LET(
        Id,
        oneof(?test_sub_ids),
        ?call_wrapper(MS, fake_unsubscribe, [Id])
    ).

gen_error_type() ->
    oneof([
        ?err_get_streams,
        ?err_make_iterator,
        ?err_subscribe
    ]).

gen_inject_error(MS) ->
    ?LET(
        {Shard, Mask},
        {oneof(?fake_shards), gen_error_type()},
        ?call_wrapper(MS, inject_error, [Shard, Mask])
    ).

gen_fix_error(MS) ->
    ?LET(
        {Shard, Mask},
        oneof(maps:keys(MS#model_state.err_rec)),
        ?call_wrapper(MS, fix_error, [Shard, Mask])
    ).

gen_fix_all_errors(MS) ->
    exactly(?call_wrapper(MS, fix_all_errors, [])).

gen_ds_sub_recoverable_error(MS) ->
    ?LET(
        Reason,
        oneof(['DOWN', ?err_rec(simulated)]),
        ?call_wrapper(MS, ds_sub_recoverable_error, [Reason])
    ).

gen_ds_sub_payloads(MS) ->
    ?LET(
        {BatchSize, SeqNoError},
        {
            range(0, 5),
            frequency([
                {5, 0},
                %% FIXME:
                {0, range(-3, 3)}
            ])
        },
        ?call_wrapper(MS, ds_publish_payloads, [BatchSize, SeqNoError])
    ).

%%------------------------------------------------------------------------------
%% Proper statem callbacks
%%------------------------------------------------------------------------------

initial_state() ->
    #model_state{}.

command(MS = #model_state{exists = false}) ->
    gen_new(MS);
command(MS = #model_state{err_rec = Errors, subs = Subs}) ->
    frequency(
        [{3, gen_fix_all_errors(MS)} || maps:size(Errors) > 0] ++
            [{3, gen_fix_error(MS)} || maps:size(Errors) > 0] ++
            [{2, gen_ds_sub_recoverable_error(MS)} || maps:size(Subs) > 0] ++
            [{2, gen_ds_sub_payloads(MS)} || maps:size(Subs) > 0] ++
            [
                {3, gen_inject_error(MS)},
                {1, gen_destroy(MS)},
                {3, gen_subscribe(MS)},
                {2, gen_unsubscribe(MS)},
                {5, gen_add_generation(MS)},
                {1, gen_del_generation(MS)},
                {5, gen_add_stream(MS)}
            ]
    ).

next_state(MS0, RuntimeState, ?call_wrapper(_, Fun, Args)) ->
    MS = next_state_(MS0, Fun, Args),
    MS#model_state{runtime = RuntimeState}.

next_state_(ModelState, fake_new, _) ->
    ModelState#model_state{
        exists = true
    };
next_state_(ModelState, fake_destroy, _) ->
    ModelState#model_state{
        exists = false,
        subs = #{}
    };
next_state_(MS = #model_state{subs = Subs}, fake_subscribe, [SubId, DB, Topic]) ->
    MS#model_state{
        subs = Subs#{SubId => {DB, Topic}}
    };
next_state_(MS = #model_state{subs = Subs}, fake_unsubscribe, [SubId]) ->
    MS#model_state{
        subs = maps:remove(SubId, Subs)
    };
next_state_(MS = #model_state{current_generation = CG}, add_generation, [Shard, Generation]) ->
    MS#model_state{
        current_generation = CG#{Shard => Generation}
    };
next_state_(MS = #model_state{streams = Streams0}, del_generation, [Shard, Generation, _Graceful]) ->
    Streams = lists:filter(
        fun(#fake_stream{shard = S, gen = G}) ->
            S =/= Shard orelse G > Generation
        end,
        Streams0
    ),
    MS#model_state{streams = Streams};
next_state_(MS = #model_state{streams = Streams, counter = Ctr}, add_stream, [Stream]) ->
    MS#model_state{
        counter = Ctr + 1,
        streams = [Stream | Streams]
    };
next_state_(MS = #model_state{err_rec = Errors}, inject_error, [Shard, ErrorType]) ->
    MS#model_state{
        err_rec = Errors#{{Shard, ErrorType} => true}
    };
next_state_(MS, ds_sub_recoverable_error, _) ->
    MS;
next_state_(MS, ds_publish_payloads, _) ->
    MS;
next_state_(MS = #model_state{err_rec = Errors}, fix_error, [Shard, ErrorType]) ->
    MS#model_state{
        err_rec = maps:remove({Shard, ErrorType}, Errors)
    };
next_state_(MS = #model_state{}, fix_all_errors, _) ->
    MS#model_state{
        err_rec = #{}
    }.

precondition(#model_state{subs = Subs}, ?call_wrapper(_, fake_subscribe, [SubId | _])) ->
    not maps:is_key(SubId, Subs);
precondition(#model_state{subs = Subs}, ?call_wrapper(_, fake_unsubscribe, [SubId | _])) ->
    maps:is_key(SubId, Subs);
precondition(_, _) ->
    true.

postcondition(PrevState, Call, Result) ->
    CurrentState = next_state(PrevState, Result, Call),
    prop_ownership(CurrentState),
    prop_active_subscriptions(CurrentState),
    prop_no_pending_when_healthy(CurrentState),
    prop_host_seen_all_streams(CurrentState),
    %% prop_host_generations(CurrentState, Call),
    true.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

proper_test_() ->
    {setup,
        fun() ->
            meck:new(emqx_ds, [no_history]),
            meck:expect(emqx_ds, list_shards, fun(_) -> ?fake_shards end)
        end,
        fun(_) ->
            meck:unload(emqx_ds)
        end,
        fun(_) ->
            {timeout, 120, [fun run_proper/0]}
        end}.

run_proper() ->
    ProperOpts = [
        {numtests, 200},
        {max_size, 100},
        {on_output, fun(Fmt, Args) -> io:format(user, Fmt, Args) end}
    ],
    ?assert(
        proper:quickcheck(
            ?forall_trace(
                Cmds,
                proper_statem:more_commands(
                    2,
                    proper_statem:commands(?MODULE)
                ),
                begin
                    put(?ws, #world_stage{}),
                    put(?hs, #test_host_state{}),
                    {_History, _State, Result} = proper_statem:run_commands(?MODULE, Cmds),
                    ?assertMatch(ok, Result),
                    aggregate(command_names(Cmds), true)
                end,
                []
            ),
            ProperOpts
        )
    ).

format_cmds(Cmds) ->
    lists:map(
        fun({set, _, ?call_wrapper(_, Fun, Args)}) ->
            io_lib:format("   ~p ~p~n", [Fun, Args])
        end,
        Cmds
    ).

pprint_mstate(MS = #model_state{runtime = {CS, HS}}) ->
    ?record_to_map(model_state, MS#model_state{
        runtime = #{
            world => ?record_to_map(world_stage, get(?ws)),
            client => emqx_ds_client:pprint_cs(CS),
            host => ?record_to_map(test_host_state, HS)
        }
    }).

subscribe__test() ->
    try
        %% Setup:
        meck:new(emqx_ds, [no_history]),
        meck:expect(emqx_ds, list_shards, fun(_) -> ?fake_shards end),
        %% Initial callback module state indicates that subscription
        %% id1 has replayed everything in the shard <<"12">> up to
        %% generation 32, which is current:
        HostState0 = #test_host_state{generations = #{{id1, <<"12">>} => 32}},
        GS0 = emqx_ds_client:new(?MODULE, #{}),
        %% Create first subscription:
        DB = test_db,
        TF = [<<"my_topic">>],
        {ok, GS1} = emqx_ds_client:subscribe_(
            GS0, #{id => id1, db => DB, topic => TF, start_time => 42}, HostState0
        ),
        #cs{
            cbm = ?MODULE,
            retry_tref = undefined,
            subs = Subs1,
            plan = Plan1,
            retry = Retry1
        } = GS1,
        ?assertEqual(
            [],
            Retry1,
            "No retries are expected"
        ),
        ?assertMatch(
            [{id1, #sub{db = DB, topic = TF, start_time = 42}}],
            maps:to_list(Subs1),
            "Subscription state"
        ),
        ?assertEqual(
            [
                #eff_watch_streams{sub_id = id1, db = DB, topic = TF},
                #eff_renew_streams{
                    sub_id = id1,
                    db = DB,
                    shard = <<"0">>,
                    topic = TF,
                    start_time = 42,
                    current_generation = 0
                },
                #eff_renew_streams{
                    sub_id = id1,
                    db = DB,
                    shard = <<"12">>,
                    topic = TF,
                    start_time = 42,
                    current_generation = 32
                }
            ],
            lists:reverse(Plan1)
        ),
        %% Try to create the second subscription with the same id:
        ?assertMatch(
            {error, already_exists},
            emqx_ds_client:subscribe_(GS1, #{id => id1, db => DB, topic => TF}, HostState0)
        )
    after
        meck:unload(emqx_ds)
    end.

%%------------------------------------------------------------------------------
%% Properties
%%------------------------------------------------------------------------------

%% This function verifies 1:1 relation between watches and
%% subscriptions owned by the client and those that exist in the fake
%% world. That is, there aren't any dangling or leaked subscriptions.
prop_ownership(#model_state{runtime = {CS, _}}) ->
    WS = #world_stage{watches = Watches, ds_subs = Subs} = get(?ws),
    case CS of
        undefined ->
            %% Client doesn't exist:
            snabbkaffe_diff:assert_lists_eq(
                [],
                Watches,
                #{comment => "Leaked watches"}
            ),
            snabbkaffe_diff:assert_lists_eq(
                [],
                Subs,
                #{comment => "Leaked DS subscriptions"}
            );
        #cs{new_streams_watches = OwnedWatches, ds_subs = OwnedSubs} ->
            %% Client exists:
            snabbkaffe_diff:assert_lists_eq(
                lists:sort(maps:keys(OwnedWatches)),
                lists:sort(Watches)
            ),
            ActiveSubRefs = [SRef || #test_ds_sub{sref = SRef} <- Subs],
            snabbkaffe_diff:assert_lists_eq(
                lists:sort(maps:keys(OwnedSubs)),
                lists:sort(ActiveSubRefs),
                #{
                    comment => #{
                        cs => emqx_ds_client:inspect(CS), ws => ?record_to_map(world_stage, WS)
                    }
                }
            )
    end,
    true.

%% This function verifies result of `emqx_ds_client:dispatch' function.
-spec prop_dispatch_result(_Message, emqx_ds_client:t(), _DispatchResult) -> ok.
prop_dispatch_result(
    #emqx_ds_client_retry{ref = Ref}, CS = #cs{ref = CRef, retry_tref = TRef}, Result
) ->
    case Result of
        {_, _, _} when is_reference(TRef), Ref =:= CRef ->
            %% Retry timer is active, client should update its state.
            ok;
        ignore ->
            %% Otherwise it should ignore the retry timers:
            ok;
        _ ->
            error({unexpected_retry, #{result => Result, ref => Ref, state => CS}})
    end;
prop_dispatch_result(#new_stream_event{subref = W}, CS, Result) ->
    case maps:is_key(W, CS#cs.new_streams_watches) of
        true ->
            ?assertMatch(
                {_, _, _},
                Result,
                """
                Client should update its state when it receives a new stream
                notification for an existing watch
                """
            );
        false ->
            ?assertMatch(
                ignore,
                Result,
                "Client should ignore unknown stream notifications"
            )
    end;
prop_dispatch_result({'DOWN', MRef, _, _, _}, CS, Result) ->
    case maps:is_key(MRef, CS#cs.ds_subs) of
        true ->
            ?assertMatch(
                {_, _, _},
                Result,
                "Client should update its state when a DS subscription dies"
            );
        false ->
            ?assertMatch(
                ignore,
                Result,
                "Client should ignore stray DOWN messages"
            )
    end;
prop_dispatch_result(Msg = #ds_sub_reply{ref = Ref}, CS, Result) ->
    IsSub = maps:is_key(Ref, CS#cs.ds_subs),
    case Result of
        {_, _, _} when IsSub ->
            ok;
        {data, _SubId, _Stream, #ds_sub_reply{}} when IsSub ->
            ok;
        ignore when not IsSub ->
            ok;
        _ ->
            error(
                {"Invalid response from dispatch_message function", #{
                    msg => Msg,
                    is_owned => IsSub,
                    state => emqx_ds_client:inspect(CS),
                    result => Result
                }}
            )
    end;
prop_dispatch_result(Message, Client, Result) ->
    error(
        {"Invalid response from dispatch_message function", #{
            msg => Message,
            state => emqx_ds_client:inspect(Client),
            result => Result
        }}
    ).

prop_host_seen_all_streams(#model_state{runtime = {undefined, _}}) ->
    %% Client doesn't exist, nothing to verify.
    true;
prop_host_seen_all_streams(#model_state{
    err_rec = ErrRec, runtime = {CS, HS}, streams = Streams
}) ->
    case maps:size(ErrRec) of
        0 ->
            %% When system is healthy state, the following should hold:
            %%
            %% 1. All streams for the existing subscriptions for the current
            %% generation should be active. Iterators should exist in the test
            %% host state.
            %%
            %% 2. All streams for the past generations should be fully
            %% replayed.
            %%
            %% 3. There should not be any record of streams for the
            %% future generations in the host state.
            #cs{subs = Subs} = CS,
            #test_host_state{iterators = HostIters} = HS,
            %% Run check for each subscription:
            maps:foreach(
                fun(SubId, _Sub) ->
                    %% Cache current generations for the subscription:
                    CurrentGens = maps:from_list(
                        [
                            {Shard, get_current_generation(SubId, Shard, HS)}
                         || Shard <- ?fake_shards
                        ]
                    ),
                    lists:foreach(
                        fun(Stream = #fake_stream{shard = Shard, gen = Gen}) ->
                            case maps:get(Shard, CurrentGens) of
                                Current when Gen > Current ->
                                    ?assertNot(
                                        maps:is_key({SubId, Stream}, HostIters),
                                        "Stream from a future generation, iterator should not exist"
                                    );
                                Current when Gen =:= Current ->
                                    ?assertMatch(
                                        #{{SubId, Stream} := _},
                                        HostIters,
                                        #{
                                            msg =>
                                                "Current generation, there should be an iterator or `end_of_stream'",
                                            sub_id => SubId,
                                            stream => Stream
                                        }
                                    );
                                Current when Gen < Current ->
                                    %% Note: test host doesn't clean up replayed streams.
                                    ?assertMatch(
                                        #{{SubId, Stream} := end_of_stream},
                                        HostIters,
                                        #{
                                            msg =>
                                                "Past generation, stream should be fully replayed",
                                            sub_id => SubId,
                                            stream => Stream
                                        }
                                    )
                            end
                        end,
                        Streams
                    )
                end,
                Subs
            ),
            true;
        _ ->
            %% There are injected errors. System is in unknown state.
            %% Skip verification until it recovers.
            true
    end.

%% When the system is healthy, all streams for the current generation
%% should be either fully replayed or active.
prop_no_pending_when_healthy(#model_state{err_rec = Errors, runtime = {CS, _}}) ->
    Healthy = maps:size(Errors) =:= 0,
    case CS of
        #cs{} when Healthy ->
            %% Client exists and the system is healthy:
            maps:foreach(
                fun({SubId, Shard}, #stream_cache{pending_iterator = Pending}) ->
                    ?assertMatch(
                        [],
                        Pending,
                        #{
                            msg => "All iterators should be present when the system is healthy",
                            subid => SubId,
                            shard => Shard
                        }
                    )
                end,
                CS#cs.streams
            );
        _ ->
            ok
    end.

%% There is 1:1 correspondence between active streams and DS subscriptions:
prop_active_subscriptions(#model_state{runtime = {undefined, _HS}}) ->
    ok;
prop_active_subscriptions(#model_state{runtime = {CS, _HS}}) ->
    #cs{ds_subs = DSSubs, streams = Streams} = CS,
    %% Collect all active streams with subscriptions:
    ActiveStreams = maps:fold(
        fun(_, #stream_cache{active = Active}, Acc) ->
            maps:fold(
                fun
                    (_, {_It, undefined}, Acc1) ->
                        Acc1;
                    (_, {_It, SubRef}, Acc1) ->
                        [SubRef | Acc1]
                end,
                Acc,
                Active
            )
        end,
        [],
        Streams
    ),
    %% Compare the result with the DS subs:
    snabbkaffe_diff:assert_lists_eq(
        lists:sort(ActiveStreams),
        lists:sort(maps:keys(DSSubs)),
        #{comment => emqx_ds_client:inspect(CS)}
    ).

%% Verify that when the system is healthy, after sending the data all
%% subscriptions registered by the host advance to the last
%% generations.
prop_host_generations(MS = #model_state{err_rec = Errors, runtime = {_, HS}}, Call) ->
    %% TODO: this doesn't work like this. The client can advance the
    %% generation, but unless it receives some data or end_of_stream,
    %% it won't reach the last generation.
    IsSystemHealthy = maps:size(Errors) =:= 0,
    RunCheck =
        case Call of
            ?call_wrapper(_, ds_publish_payloads, [_, 0]) when IsSystemHealthy ->
                %% Run this check after publishing some payloads with
                %% valid sequence numbers:
                true;
            _ ->
                false
        end,
    case RunCheck of
        true ->
            maps:foreach(
                fun({SubId, Shard}, HostGeneration) ->
                    ?assertEqual(
                        current_gen(Shard, MS),
                        HostGeneration,
                        #{sub_id => SubId, shard => Shard}
                    )
                end,
                HS#test_host_state.generations
            );
        false ->
            skip
    end,
    true.

%%------------------------------------------------------------------------------
%% Fake versions of commands
%%------------------------------------------------------------------------------

fake_new(#model_state{runtime = {_, HS}}) ->
    CS = emqx_ds_client:new(?MODULE, #{retry_interval => 10000000}),
    {CS, HS}.

fake_destroy(MS = #model_state{runtime = {CS0, HS}}) ->
    CS = emqx_ds_client:destroy_(CS0),
    setelement(
        1,
        execute_planned(MS#model_state{runtime = {CS, HS}}),
        undefined
    ).

fake_subscribe(MS = #model_state{runtime = {CS0, HS}}, SubId, DB, Topic) ->
    {ok, CS} = emqx_ds_client:subscribe_(
        CS0,
        #{id => SubId, db => DB, topic => Topic},
        HS
    ),
    execute_planned(MS#model_state{runtime = {CS, HS}}).

fake_unsubscribe(MS = #model_state{runtime = {CS0, HS0}}, SubId) ->
    {ok, GS, HS} = emqx_ds_client:unsubscribe_(CS0, SubId, HS0),
    execute_planned(MS#model_state{runtime = {GS, HS}}).

add_generation(#model_state{runtime = RS}, _Shard, _Generation) ->
    RS.

%% Dispatch 'DOWN' messages for all subscriptions to streams that no
%% longer exist:
del_generation(
    MS0 = #model_state{streams = Streams, runtime = RS0 = {_, _}}, _Shard, _Generation, Graceful
) ->
    EffHandler = fake_world(MS0),
    Reason =
        case Graceful of
            true -> ?err_unrec(generation_is_gone);
            false -> 'DOWN'
        end,
    lists:foldl(
        fun(#test_ds_sub{sref = SRef, it = #fake_iter{stream = Stream}}, RS) ->
            case lists:member(Stream, Streams) of
                true ->
                    RS;
                false ->
                    destroy_ds_sub(EffHandler, SRef, RS, Reason)
            end
        end,
        RS0,
        (get(?ws))#world_stage.ds_subs
    ).

ds_sub_recoverable_error(MS = #model_state{runtime = RS0 = {_, _}}, Reason) ->
    %% Emulate all DS subscriptions going down:
    #world_stage{ds_subs = DSSubs} = get(?ws),
    EffHandler = fake_world(MS),
    lists:foldl(
        fun(#test_ds_sub{sref = SRef}, RS) ->
            destroy_ds_sub(EffHandler, SRef, RS, Reason)
        end,
        RS0,
        DSSubs
    ).

destroy_ds_sub(EffHandler, SRef, {CS, HS}, Reason) ->
    WS0 = #world_stage{ds_subs = DSSubs0} = get(?ws),
    WS = WS0#world_stage{ds_subs = lists:keydelete(SRef, #test_ds_sub.sref, DSSubs0)},
    put(?ws, WS),
    Msg =
        case Reason of
            'DOWN' ->
                {'DOWN', SRef, process, self(), simulated};
            {error, _, _} ->
                %% Note: we send error with invalid seqno. The
                %% client should never check seqno for errors
                %% anyway, else it would end up with dangling
                %% subscriptions:
                #ds_sub_reply{
                    ref = SRef,
                    payload = Reason,
                    size = -1,
                    seqno = -1
                }
        end,
    dispatch_message(Msg, EffHandler, {CS, HS}).

add_stream(MS = #model_state{runtime = RS = {_CS, _HS}}, _Stream) ->
    #world_stage{watches = Watches} = get(?ws),
    Events = [#new_stream_event{subref = W} || W <- Watches],
    EffHandler = fake_world(MS),
    lists:foldl(
        fun(Msg, RSAcc) ->
            dispatch_message(Msg, EffHandler, RSAcc)
        end,
        RS,
        Events
    ).

ds_publish_payloads(MS = #model_state{runtime = RS0 = {_, _}}, BatchSize, SeqNoError) ->
    #world_stage{ds_subs = DSSubs0} = get(?ws),
    EffHandler = fake_world(MS),
    lists:foldl(
        fun(DSSub = #test_ds_sub{sref = SRef, it = It0, seqno = SeqNo0}, RS1) ->
            #fake_iter{stream = #fake_stream{shard = Shard, gen = Gen}} = It0,
            Msg =
                case current_gen(Shard, MS) > Gen of
                    true ->
                        SeqNo = SeqNo0 + 1 + SeqNoError,
                        #ds_sub_reply{
                            ref = SRef,
                            payload = {ok, end_of_stream},
                            size = 1,
                            seqno = SeqNo
                        };
                    false ->
                        SeqNo = SeqNo0 + BatchSize + SeqNoError,
                        %% TODO: make data more realistic:
                        It = It0,
                        TTVs = [],
                        #ds_sub_reply{
                            ref = SRef,
                            payload = {ok, It, TTVs},
                            size = BatchSize,
                            seqno = SeqNo
                        }
                end,
            ?tp(test_publish_message_to_sub, #{message => Msg}),
            update_seqno(DSSub, SeqNo),
            dispatch_message(Msg, EffHandler, RS1)
        end,
        RS0,
        DSSubs0
    ).

update_seqno(DSSub = #test_ds_sub{sref = SRef}, SeqNo) ->
    with_world(
        fun(WS = #world_stage{ds_subs = Subs}) ->
            {ok, WS#world_stage{
                ds_subs = lists:keyreplace(SRef, #test_ds_sub.sref, Subs, DSSub#test_ds_sub{
                    seqno = SeqNo
                })
            }}
        end
    ).

fix_all_errors(#model_state{runtime = RS}) ->
    RS.

inject_error(#model_state{runtime = RS}, _Shard, _Mask) ->
    RS.

fix_error(#model_state{runtime = RS}, _Shard, _Mask) ->
    RS.

%%------------------------------------------------------------------------------
%% Helper functions
%%------------------------------------------------------------------------------

wrapper(MS0, Fun, Args) ->
    %% Due to PropEr design, here MS0 is the model state _before_
    %% applying the effect. This is fairly inconvenient. Advance the
    %% model state to the current accoding to the symbolic execution
    %% rule:
    MS = next_state_(MS0, Fun, Args),
    ?tp("test_" ++ atom_to_list(Fun), #{args => Args}),
    %% Apply the function to the state and also verify the return
    %% value of the operation, it should be a valid runtime state
    %% triple:
    RS = {CS, #test_host_state{}} = apply(?MODULE, Fun, [MS | Args]),
    %% Emulate firing of the retry timer if the client exists:
    case CS of
        undefined ->
            RS;
        #cs{ref = Ref} ->
            %% Emulate retry timer firing:
            dispatch_message(
                #emqx_ds_client_retry{ref = Ref},
                fake_world(MS),
                RS
            )
    end.

dispatch_message(Message, EffectHandler, {CS0, HS0}) ->
    Result = emqx_ds_client:do_dispatch_message(Message, CS0, HS0),
    prop_dispatch_result(Message, CS0, Result),
    case Result of
        ignore ->
            {CS0, HS0};
        {data, SubId, Stream, Reply} ->
            case Reply of
                #ds_sub_reply{ref = Ref, payload = {ok, end_of_stream}} ->
                    HS1 = host_set_iter(SubId, Stream, end_of_stream, HS0),
                    {CS, HS} = emqx_ds_client:complete_stream_(CS0, Ref, HS1),
                    execute(EffectHandler, #cs.plan, CS, HS);
                #ds_sub_reply{} ->
                    {CS0, HS0}
            end;
        {Field, CS, HS} ->
            execute(EffectHandler, Field, CS, HS)
    end.

execute_planned(MS = #model_state{runtime = {CS, HS}}) ->
    execute(fake_world(MS), #cs.plan, CS, HS).

execute(EffectHandler, Field, CS0, HS0) ->
    {CS, HS} = emqx_ds_client:execute(
        Field,
        EffectHandler,
        fun emqx_ds_client:result_handler/4,
        CS0,
        HS0
    ),
    {CS, HS}.

%%------------------------------------------------------------------------------
%% Effect handler
%%------------------------------------------------------------------------------

%% Create an effect handler according to the state of the model
fake_world(#model_state{
    streams = Streams,
    err_rec = Errors
}) ->
    Err = fun(Shard, ErrorType) ->
        not maps:is_key({Shard, ErrorType}, Errors)
    end,
    fun
        (#eff_watch_streams{sub_id = SubId}) ->
            with_world(fun(Stage) ->
                #world_stage{watch_ref_ctr = Ctr, watches = Watches} = Stage,
                Watch = ?watch_ref(Ctr, SubId),
                {
                    Watch,
                    Stage#world_stage{watch_ref_ctr = Ctr + 1, watches = [Watch | Watches]}
                }
            end);
        (#eff_unwatch_streams{watch = Watch}) ->
            with_world(#world_stage.watches, fun(Watches) ->
                {
                    ok,
                    Watches -- [Watch]
                }
            end);
        (#eff_renew_streams{shard = Shard, current_generation = Gen}) ->
            with_world(fun(Stage) ->
                Result =
                    case Err(Shard, ?err_get_streams) of
                        true ->
                            Filtered = [
                                {{S, G}, Stream}
                             || Stream = #fake_stream{shard = S, gen = G} <- Streams,
                                S =:= Shard,
                                G >= Gen
                            ],
                            {Filtered, []};
                        false ->
                            {[], [?err_rec(simulated)]}
                    end,
                {Result, Stage}
            end);
        (#eff_make_iterator{stream = Stream, start_time = Time}) ->
            with_world(fun(Stage) ->
                #fake_stream{shard = Shard} = Stream,
                Result =
                    case Err(Shard, ?err_make_iterator) of
                        true ->
                            case lists:member(Stream, Streams) of
                                true ->
                                    {ok, #fake_iter{stream = Stream, time = Time}};
                                false ->
                                    ?err_unrec(no_such_stream)
                            end;
                        false ->
                            ?err_rec(simulated)
                    end,
                {Result, Stage}
            end);
        (#eff_ds_sub{sub_id = SubId, iterator = It}) ->
            with_world(fun(Stage) ->
                #fake_iter{stream = #fake_stream{shard = Shard}} = It,
                #world_stage{sub_ref_ctr = Ctr, ds_subs = DSSubs} = Stage,
                case Err(Shard, ?err_subscribe) of
                    true ->
                        Handle = ?sub_handle(SubId, Ctr),
                        SubRef = ?sub_ref(SubId, Handle),
                        DSSub = #test_ds_sub{
                            sref = SubRef,
                            handle = Handle,
                            it = It
                        },
                        {
                            {ok, Handle, SubRef},
                            Stage#world_stage{
                                sub_ref_ctr = Ctr + 1,
                                ds_subs = [DSSub | DSSubs]
                            }
                        };
                    false ->
                        {
                            ?err_rec(simulated),
                            Stage
                        }
                end
            end);
        (#eff_ds_unsub{handle = Handle}) ->
            with_world(#world_stage.ds_subs, fun(DSSubs0) ->
                %% Theoretically, it can fail too, but usually it means
                %% the subscription will expire via monitor:
                DSSubs = lists:keydelete(Handle, #test_ds_sub.handle, DSSubs0),
                {
                    ok,
                    DSSubs
                }
            end)
    end.

with_world(Fun) ->
    {Result, WS} =
        Fun(get(?ws)),
    put(?ws, WS),
    Result.

with_world(Field, Fun) ->
    with_world(
        fun(World) ->
            {Ret, NewVal} = Fun(element(Field, World)),
            {Ret, setelement(Field, World, NewVal)}
        end
    ).

with_host(Fun) ->
    {Result, HS} =
        Fun(get(?hs)),
    put(?hs, HS),
    Result.
