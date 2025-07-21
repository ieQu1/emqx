%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% This testcase verifies `execute/4' function
interpreter_test() ->
    %% Schedule 3 actions. Action is a tuple `{Id, Value}'.
    GS0 = emqx_ds_client:plan(
        {3, 2}, emqx_ds_client:plan({2, 2}, emqx_ds_client:plan({1, 1}, #cs{}))
    ),
    %% Simple effect handler:
    EffHandler = fun({_Id, Val}, EffHandlerState) ->
        {
            Val - 1,
            EffHandlerState + 1
        }
    end,
    %% Effect result handler:
    ResHandler = fun({Id, Val}, GS1, Acc, Result) ->
        {
            case Result of
                0 -> GS1;
                _ -> emqx_ds_client:plan({Id, Result}, GS1)
            end,
            [{Id, Val} | Acc]
        }
    end,
    EffHandlerState0 = 0,
    {EffHandlerState, GS, History} = emqx_ds_client:execute(
        #cs.plan, EffHandler, EffHandlerState0, ResHandler, GS0, []
    ),
    ?assertMatch(
        #cs{plan = [], retry = []},
        GS,
        "All planned actions should be executed"
    ),
    snabbkaffe_diff:assert_lists_eq(
        [{1, 1}, {2, 2}, {3, 2}, {2, 1}, {3, 1}],
        lists:reverse(History)
    ),
    ?assertEqual(
        EffHandlerState,
        length(History),
        "State of the effect handler should match the number of effects it handled"
    ).

%% This testcase verifies how `retry/2' function adds actions to the
%% plan and starts the retry timer. Then it verifies that dispatch
%% function correctly interprets the timeout message.
retry_test() ->
    %% Effect handler increments the counter in the state and always
    %% returns ok:
    EffHandler = fun(_Eff, EffHandlerState) -> {ok, EffHandlerState + 1} end,
    %% Result handler adds effects to the history:
    ResHandler = fun(Eff, GS, Acc, _Res) -> {GS, [Eff | Acc]} end,
    %% Create the state and add two retry actions:
    GS0 = emqx_ds_client:new(undefined, #{retry_interval => 10}),
    GS1 = emqx_ds_client:retry(2, emqx_ds_client:retry(1, GS0)),
    %% Verify the state:
    ?assertMatch(
        [2, 1],
        GS1#cs.retry,
        "Retry actions have been added"
    ),
    ?assert(
        is_reference(GS1#cs.retry_tref),
        "Retry timer has been started"
    ),
    %% Add more actions:
    GS2 = emqx_ds_client:retry(3, GS1),
    ?assertMatch(
        [3, 2, 1],
        GS2#cs.retry,
        "New retry action have been added"
    ),
    ?assertEqual(
        GS1#cs.retry_tref,
        GS2#cs.retry_tref,
        "Retry timer is the same"
    ),
    %% Receive retry message:
    receive
        Msg ->
            Res1 = emqx_ds_client:dispatch_message(EffHandler, 0, ResHandler, GS2, Msg, []),
            ?assertMatch(
                {3, #cs{retry = [], retry_tref = undefined}, [3, 2, 1]},
                Res1,
                #{msg => Msg, state => GS2}
            ),
            %% There are no duplicate or unexpected messages:
            receive
                Unexpected -> error({unexpected_message, Unexpected})
            after 30 -> ok
            end
    after 30 -> error(no_retry_message)
    end.

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

-record(fake_stream, {shard, gen, id}).
-record(fake_iter, {stream, time}).

get_current_generation(SubId, Shard, #test_host_state{generations = Gens}) ->
    maps:get({SubId, Shard}, Gens, 0).

on_advance_generation(
    SubId, Shard, NextGen, HS = #test_host_state{generations = Gens0}
) ->
    OldCurrent = get_current_generation(SubId, Shard, HS),
    ?tp(test_host_advance_generation, #{
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
    SubId, _Slab, Stream, It, HS0 = #test_host_state{iterators = Its}
) ->
    Key = {SubId, Stream},
    ?tp(test_host_new_iterator, #{subid => SubId, stream => Stream, it => It}),
    ?assertNot(maps:is_key(Key, Its), "Client should not re-create iterators"),
    HS = HS0#test_host_state{
        iterators = Its#{Key => It}
    },
    {subscribe, HS}.

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
%-define(test_sub_ids, [id1]).

-define(err_get_streams, err_get_streams).
-define(err_make_iterator, err_make_iterator).
-define(err_subscribe, err_subscribe).

-type error_type() :: ?err_get_streams | ?err_make_iterator | ?err_subscribe.

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
    ds_subs = [] :: [{?sub_ref(_, _), _It}]
}).

%% Model state (updated by proper command generator):
-record(model_state, {
    %% Counter for creating unique IDs:
    counter = 0 :: integer(),
    current_generation = #{} :: #{emqx_ds:shard() => emqx_ds:generation()},
    streams = [] :: [{{emqx_ds:slab()}, _Stream}],
    %% Mask of injected recoverable errors for the shards:
    err_rec = #{} :: #{{emqx_ds:shard(), error_type()} => true},
    subs = #{} :: #{emqx_ds_client:sub_id() => {_DB, _Topic, _Opts}},
    exists = false :: boolean(),
    runtime = {#world_stage{}, undefined, #test_host_state{}}
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

%%------------------------------------------------------------------------------
%% Proper statem callbacks
%%------------------------------------------------------------------------------

initial_state() ->
    #model_state{}.

command(MS = #model_state{exists = false}) ->
    gen_new(MS);
command(MS = #model_state{err_rec = Errors}) ->
    frequency(
        [{3, gen_fix_all_errors(MS)} || maps:size(Errors) > 0] ++
            [{3, gen_fix_error(MS)} || maps:size(Errors) > 0] ++
            [
                {3, gen_inject_error(MS)},
                {1, gen_destroy(MS)},
                {3, gen_subscribe(MS)},
                {2, gen_unsubscribe(MS)},
                {5, gen_add_generation(MS)},
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
next_state_(MS = #model_state{streams = Streams, counter = Ctr}, add_stream, [Stream]) ->
    MS#model_state{
        counter = Ctr + 1,
        streams = [Stream | Streams]
    };
next_state_(MS = #model_state{err_rec = Errors}, inject_error, [Shard, ErrorType]) ->
    MS#model_state{
        err_rec = Errors#{{Shard, ErrorType} => true}
    };
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
    prop_host_seen_all_streams(CurrentState).

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
                    {_History, _State, Result} = proper_statem:run_commands(?MODULE, Cmds),
                    ?assertMatch(ok, Result)
                    %% ?WHENFAIL(
                    %%     io:format(
                    %%         user,
                    %%         "Commands:~n~s~nState: ~p\nResult: ~p~n",
                    %%         [format_cmds(Cmds), pprint_mstate(State), Result]
                    %%     ),
                    %%     aggregate(command_names(Cmds), Result =:= ok)
                    %% )
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

pprint_mstate(MS = #model_state{runtime = {WS, CS, HS}}) ->
    ?record_to_map(model_state, MS#model_state{
        runtime = #{
            world => ?record_to_map(world_stage, WS),
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
prop_ownership(#model_state{runtime = {WS, GS, _}}) ->
    #world_stage{watches = Watches, ds_subs = Subs} = WS,
    case GS of
        undefined ->
            %% Client doesn't exist:
            snabbkaffe_diff:assert_lists_eq(
                [],
                Watches
            ),
            snabbkaffe_diff:assert_lists_eq(
                [],
                Subs
            );
        #cs{new_streams_watches = OwnedWatches, ds_subs = OwnedSubs} ->
            snabbkaffe_diff:assert_lists_eq(
                lists:sort(Watches),
                lists:sort(maps:keys(OwnedWatches))
            ),
            {ActiveSubRefs, _} = lists:unzip(Subs),
            snabbkaffe_diff:assert_lists_eq(
                lists:sort(ActiveSubRefs),
                lists:sort(maps:keys(OwnedSubs))
            )
    end,
    true.

%% This function verifies result of `emqx_ds_client:dispatch' function.
-spec prop_dispatch_result(_Message, #world_stage{}, emqx_ds_client:t(), _DispatchResult) -> ok.
prop_dispatch_result(
    #emqx_ds_client_retry{ref = Ref}, _WS, CS = #cs{ref = CRef, retry_tref = TRef}, Result
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
prop_dispatch_result(
    #new_stream_event{subref = W}, _WS, #cs{new_streams_watches = Watches}, Result
) ->
    case Watches of
        #{W := _} ->
            ?assertMatch(
                {_, _, _},
                Result,
                """
                Client should update its state when it receives a new stream
                notification for an existing watch
                """
            );
        #{} ->
            ?assertMatch(
                ignore,
                Result,
                "Client should ignore unknown stream notifications"
            )
    end.

prop_host_seen_all_streams(#model_state{runtime = {_, undefined, _}}) ->
    %% Client doesn't exist, nothing to verify.
    true;
prop_host_seen_all_streams(#model_state{
    err_rec = ErrRec, runtime = {_WS, CS, HS}, streams = Streams
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
                                        #{{SubId, Stream} := #fake_iter{}},
                                        HostIters,
                                        #{
                                            msg =>
                                                "Current generation, there should be an iterator",
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
prop_no_pending_when_healthy(#model_state{err_rec = Errors, runtime = {_WS, CS, _}}) ->
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
prop_active_subscriptions(#model_state{runtime = {_WS, undefined, _HS}}) ->
    ok;
prop_active_subscriptions(#model_state{runtime = {_WS, CS, _HS}}) ->
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

%%------------------------------------------------------------------------------
%% Fake versions of commands
%%------------------------------------------------------------------------------

fake_new(#model_state{runtime = {WS, _, HS}}) ->
    GS = emqx_ds_client:new(?MODULE, #{retry_interval => 10000000}),
    {WS, GS, HS}.

fake_destroy(MS = #model_state{runtime = {WS, GS0, HS}}) ->
    GS = emqx_ds_client:destroy_(GS0),
    setelement(
        2,
        execute(MS#model_state{runtime = {WS, GS, HS}}),
        undefined
    ).

fake_subscribe(MS = #model_state{runtime = {WS, GS0, HS}}, SubId, DB, Topic) ->
    {ok, GS} = emqx_ds_client:subscribe_(
        GS0,
        #{id => SubId, db => DB, topic => Topic},
        HS
    ),
    execute(MS#model_state{runtime = {WS, GS, HS}}).

fake_unsubscribe(MS = #model_state{runtime = {WS, GS0, HS0}}, SubId) ->
    {ok, GS, HS} = emqx_ds_client:unsubscribe_(GS0, SubId, HS0),
    execute(MS#model_state{runtime = {WS, GS, HS}}).

add_generation(#model_state{runtime = RS}, _Shard, _Generation) ->
    RS.

add_stream(MS = #model_state{runtime = RS = {WS, _CS, _HS}}, _Stream) ->
    #world_stage{watches = Watches} = WS,
    Events = [#new_stream_event{subref = W} || W <- Watches],
    EffHandler = fake_world(MS),
    lists:foldl(
        fun(Msg, RSAcc) ->
            fake_dispatch(EffHandler, Msg, RSAcc)
        end,
        RS,
        Events
    ).

fix_all_errors(#model_state{runtime = RS}) ->
    RS.

inject_error(#model_state{runtime = RS}, _Shard, _Mask) ->
    RS.
fix_error(#model_state{runtime = RS}, _Shard, _Mask) ->
    RS.

%% Execute plan with the fake effect handler:
execute(
    ModelState = #model_state{runtime = {WorldStage0, GS0, HostState0}}
) ->
    emqx_ds_client:execute(
        #cs.plan,
        fake_world(ModelState),
        WorldStage0,
        fun emqx_ds_client:result_handler/4,
        GS0,
        HostState0
    ).

%%------------------------------------------------------------------------------
%% Helper functions
%%------------------------------------------------------------------------------

fake_dispatch(EffHandler, Message, {WS, CS, HS}) ->
    Result = emqx_ds_client:dispatch_message(
        EffHandler,
        WS,
        fun emqx_ds_client:result_handler/4,
        CS,
        Message,
        HS
    ),
    prop_dispatch_result(Message, WS, CS, Result),
    case Result of
        ignore ->
            {WS, CS, HS};
        {_, _, _} ->
            Result
    end.

wrapper(MS0, Fun, Args) ->
    %% Due to PropEr design, here MS0 is the model state _before_
    %% applying the effect. This is fairly inconvenient. Advance the
    %% model state accoding to the symbolic execution rules without
    %% changing the runtime state:
    MS = next_state_(MS0, Fun, Args),
    ?tp("test_" ++ atom_to_list(Fun), #{args => Args}),
    %% Apply the function to the state and also verify the return
    %% value of the operation, it should be a valid runtime state
    %% triple:
    RS =
        {#world_stage{}, CS, #test_host_state{}} =
        apply(?MODULE, Fun, [MS | Args]),
    %% Emulate firing of the retry timer if the client exists:
    case CS of
        undefined ->
            RS;
        #cs{ref = Ref} ->
            %% Emulate retry timer:
            EffHandler = fake_world(MS),
            fake_dispatch(
                EffHandler,
                #emqx_ds_client_retry{ref = Ref},
                RS
            )
    end.

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
        (#eff_watch_streams{sub_id = SubId}, Stage) ->
            #world_stage{watch_ref_ctr = Ctr, watches = Watches} = Stage,
            Watch = ?watch_ref(Ctr, SubId),
            {
                Watch,
                Stage#world_stage{watch_ref_ctr = Ctr + 1, watches = [Watch | Watches]}
            };
        (#eff_unwatch_streams{watch = Watch}, Stage) ->
            #world_stage{watches = Watches} = Stage,
            {
                ok,
                Stage#world_stage{watches = Watches -- [Watch]}
            };
        (#eff_renew_streams{shard = Shard, current_generation = Gen}, Stage) ->
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
            {Result, Stage};
        (#eff_make_iterator{stream = Stream, start_time = Time}, Stage) ->
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
            {Result, Stage};
        (#eff_ds_sub{sub_id = SubId, iterator = It}, Stage) ->
            #fake_iter{stream = #fake_stream{shard = Shard}} = It,
            #world_stage{sub_ref_ctr = Ctr, ds_subs = DSSubs} = Stage,
            case Err(Shard, ?err_subscribe) of
                true ->
                    Handle = ?sub_handle(SubId, Ctr),
                    SubRef = ?sub_ref(SubId, Handle),
                    {
                        {ok, Handle, SubRef},
                        Stage#world_stage{sub_ref_ctr = Ctr + 1, ds_subs = [{SubRef, It} | DSSubs]}
                    };
                false ->
                    %% Simulate unrecoverable errors too?
                    {
                        ?err_rec(simulated),
                        Stage
                    }
            end;
        (#eff_ds_unsub{handle = Handle}, Stage) ->
            %% Theoretically, it can fail too, but usually it means
            %% the subscription will expire via monitor:
            #world_stage{ds_subs = DSSubs0} = Stage,
            DSSubs = lists:filter(
                fun({?sub_ref(_SubId, H), _It}) ->
                    H =/= Handle
                end,
                DSSubs0
            ),
            {
                ok,
                Stage#world_stage{ds_subs = DSSubs}
            }
    end.
