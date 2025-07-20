%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_client).
-moduledoc """
A helper module that simplifies the task of subscribing to a topic via DS.
It takes care of monitoring DS streams, creating iterators, advancing the generations
and dealing with transient failures.
It acts as a supervisor for individual DS stream subscriptions.

This module can manage multiple topic subscriptions.
It doesn't spawn new processes, and is designed to be embedded in
a host process that handles the business logic.
""".

%% API:
-export([new/2, destroy/2, subscribe/3, unsubscribe/3, dispatch_message/3]).

-export_type([sub_id/0, t/0, sub_options/0]).

-include("emqx_ds.hrl").
-include("emqx_ds_client_internals.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-ifdef(TEST).
-compile(nowarn_export_all).
-compile(export_all).
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-doc "Global options for the client.".
-type client_opts() :: #{
    retry_interval => non_neg_integer()
}.

-doc "Unique identifier of the subscription.".
-type sub_id() :: term().

-type sub_options() :: #{
    id := sub_id(),
    db := emqx_ds:db(),
    topic := emqx_ds:topic_filter(),
    start_time => emqx_ds:time(),
    ds_sub_opts => emqx_ds:sub_opts()
}.

-type sub() :: #sub{}.
-type subs() :: #{sub_id() => sub()}.

-type watches() :: #{emqx_ds_new_streams:watch() => sub_id()}.

-type ds_sub() :: #ds_sub{}.
-type ds_subs() :: #{emqx_ds:sub_ref() => ds_sub()}.

-type stream_cache() :: #stream_cache{}.
-type streams() :: #{{sub_id(), emqx_ds:shard()} => stream_cache()}.

-type effect() ::
    #eff_renew_streams{}
    | #eff_watch_streams{}
    | #eff_unwatch_streams{}
    | #eff_make_iterator{}
    | #eff_ds_sub{}
    | #eff_ds_unsub{}.
-type effects() :: [effect()].

-doc """
Global state of the client. It encapsulatees states of all active subscriptions.
""".
-opaque t() :: #cs{}.

-type effect_handler(EffectHandlerState) :: fun(
    (effect(), EffectHandlerState) -> {_Result, EffectHandlerState}
).
-type result_handler() :: fun((effect(), t(), Acc, _Result) -> {t(), Acc}).

%%================================================================================
%% Callbacks
%%================================================================================

-callback get_current_generation(sub_id(), emqx_ds:shard(), _HostState) -> emqx_ds:generation().

-callback on_advance_generation(sub_id(), emqx_ds:shard(), emqx_ds:generation(), HostState) ->
    HostState.

-callback get_iterator(sub_id(), emqx_ds:slab(), emqx_ds:stream(), _HostState) ->
    {ok, emqx_ds:iterator() | end_of_stream} | undefined.

-callback del_iterator(sub_id(), eqmqx_ds:slab(), emqx_ds:stream(), HostState) -> HostState.

-callback on_new_iterator(
    sub_id(), emqx_ds:slab(), emqx_ds:stream(), emqx_ds:iterator(), HostState
) ->
    {subscribe | ignore, HostState}.

-callback on_make_iterator_fail(sub_id(), emqx_ds:slab(), emqx_ds:stream(), _Error, HostState) ->
    HostState.

%%================================================================================
%% API functions
%%================================================================================

-spec new(module(), client_opts()) -> t().
new(CBM, UserOpts) ->
    Options = maps:merge(
        #{
            retry_interval => 5_000
        },
        UserOpts
    ),
    #cs{cbm = CBM, options = Options}.

-spec destroy(t(), HostState) -> HostState.
destroy(GS, HostState0) ->
    {_, _, HostState} = execute(destroy_(GS), HostState0),
    HostState.

-spec subscribe(t(), sub_options(), HostState) ->
    {ok, t(), HostState} | {error, badarg | already_exists}.
subscribe(GS0, UserOpts = #{id := _, db := _, topic := _}, HostState0) ->
    case subscribe_(GS0, UserOpts, HostState0) of
        {ok, GS1} ->
            {GS, HostState} = execute(GS1, HostState0),
            {ok, GS, HostState};
        Error ->
            Error
    end.

-spec unsubscribe(t(), sub_id(), HostState) -> {ok, t(), HostState} | {error, not_found}.
unsubscribe(GS0, SubId, HostState0) ->
    case unsubscribe_(GS0, SubId, HostState0) of
        {ok, GS1, HostState1} ->
            {GS, HostState} = execute(GS1, HostState1),
            {ok, GS, HostState};
        Error ->
            Error
    end.

-doc """
Generally, all messages received by the process should be passed into this function.

If atom `ignore` is returned, the message was not addressed to the client and should be processed elsewhere.
""".
-spec dispatch_message(t(), term(), HostState) ->
    ignore | {t(), HostState} | {data, sub_id(), #ds_sub_reply{}}.
dispatch_message(GS0, Message, HostState0) ->
    Result = dispatch_message(
        fun real_world/2, undefined, fun result_handler/4, GS0, Message, HostState0
    ),
    case Result of
        {data, SubId, Data, _EffHandlerState} ->
            {data, SubId, Data};
        {_EffHandlerState, GS, HostState} ->
            {GS, HostState};
        ignore ->
            ignore
    end.

%%================================================================================
%% Internal functions
%%================================================================================

%% NOTE: this module is implemented using plan-execute pattern to
%% simplify error handling and testing

-spec dispatch_message(
    effect_handler(EffectHandlerState),
    EffectHandlerState,
    result_handler(),
    t(),
    _Message,
    HostState
) ->
    {EffectHandlerState, t(), HostState}
    | ignore
    | {data, sub_id(), #ds_sub_reply{}, EffectHandlerState}.
dispatch_message(
    EffHandler,
    EffHandlerState,
    ResultHandler,
    GS = #cs{ref = Ref, retry_tref = TRef},
    #emqx_ds_client_retry{ref = Ref},
    HostState
) when is_reference(TRef) ->
    execute(
        #cs.retry,
        EffHandler,
        EffHandlerState,
        ResultHandler,
        GS#cs{retry_tref = undefined},
        HostState
    );
dispatch_message(
    EffHandler,
    EffHandlerState,
    ResHandler,
    GS0 = #cs{new_streams_watches = Watches, subs = Subs},
    #new_stream_event{subref = Watch},
    HostState
) ->
    case Watches of
        #{Watch := SubId} ->
            #{SubId := Sub} = Subs,
            GS = renew_streams_(GS0, SubId, Sub, HostState),
            execute(#cs.plan, EffHandler, EffHandlerState, ResHandler, GS, HostState);
        #{} ->
            ignore
    end;
dispatch_message(_, _, _, _, _, _) ->
    ignore.

-spec destroy_(t()) -> t().
destroy_(GS0 = #cs{retry_tref = TRef}) ->
    %% Cancel retry timer:
    _ = is_reference(TRef) andalso erlang:cancel_timer(TRef),
    %% Drop all pending effects:
    GS = GS0#cs{
        retry_tref = undefined,
        plan = [],
        retry = []
    },
    %% Free all subscriptions:
    release_objects_(undefined, GS).

%% Schedule release of subscriptions and watches owned by `RelSubId'
%% or release everything when `RelSubId' = `undefined':
-spec release_objects_(sub_id() | undefined, t()) -> t().
release_objects_(RelSubId, GS0 = #cs{subs = Subs, new_streams_watches = Watches, ds_subs = DSSubs}) ->
    %% Remove watches:
    GS = maps:fold(
        fun
            (Watch, SubId, Acc) when
                SubId =:= RelSubId; RelSubId =:= undefined
            ->
                #{SubId := #sub{db = DB}} = Subs,
                plan(#eff_unwatch_streams{db = DB, watch = Watch}, Acc);
            (_, _, Acc) ->
                Acc
        end,
        GS0,
        Watches
    ),
    %% Remove subscriptions:
    maps:fold(
        fun
            (SubRef, #ds_sub{id = SubId, handle = Handle}, Acc) when
                SubId =:= RelSubId; RelSubId =:= undefined
            ->
                #{SubId := #sub{db = DB}} = Subs,
                plan(#eff_ds_unsub{ref = SubRef, db = DB, handle = Handle}, Acc);
            (_, _, Acc) ->
                Acc
        end,
        GS,
        DSSubs
    ).

-spec subscribe_(t(), sub_options(), _HostState) -> {ok, t()} | {error, badarg | already_exists}.
subscribe_(_, #{id := undefined}, _) ->
    {error, badarg};
subscribe_(GS0 = #cs{subs = Subs0}, UserOpts = #{id := SubId, db := DB, topic := Topic}, HostState) ->
    %% Check uniqueness of the subscription id:
    case Subs0 of
        #{SubId := _} ->
            {error, already_exists};
        _ ->
            %% Derive optional subscription options:
            StartTime = maps:get(start_time, UserOpts, 0),
            DSSubOpts = maps:get(ds_sub_opts, UserOpts, #{max_unacked => 1000}),
            %% Register new subscription:
            Sub = #sub{
                db = DB,
                topic = Topic,
                start_time = StartTime,
                ds_sub_opts = DSSubOpts
            },
            GS1 = GS0#cs{subs = Subs0#{SubId => Sub}},
            %% Plan subscription to the stream events followed by stream renewal:
            GS = plan(#eff_watch_streams{sub_id = SubId, db = DB, topic = Topic}, GS1),
            {ok, renew_streams_(GS, SubId, Sub, HostState)}
    end.

-spec unsubscribe_(t(), sub_id(), HostState) -> {ok, t(), HostState} | {error, not_found}.
unsubscribe_(GS0 = #cs{subs = Subs0, new_streams_watches = Watches}, SubId, HostState) ->
    case maps:take(SubId, Subs0) of
        {#sub{db = DB}, Subs} ->
            %% 1. Remove subscription from the registered list:
            GS1 = GS0#cs{subs = Subs},
            %% 2. Remove all previously scheduled events that belong to the subscription:
            GS2 = filter_effects(fun(Eff) -> eff_subid(Eff) =/= SubId end, GS1),
            %% 3. Remove new stream watch if exists:
            GS3 =
                case emqx_utils_maps:find_key(SubId, Watches) of
                    {ok, Watch} ->
                        plan(#eff_unwatch_streams{db = DB, watch = Watch}, GS2);
                    undefined ->
                        GS2
                end,
            %% 4. Remove all DS subscriptions that belong to this SubId:
            GS = maps:fold(
                fun
                    (SubRef, #ds_sub{id = Id, handle = Handle}, Acc) when Id =:= SubId ->
                        plan(#eff_ds_unsub{ref = SubRef, db = DB, handle = Handle}, Acc);
                    (_SubRef, #ds_sub{}, Acc) ->
                        Acc
                end,
                GS3,
                GS3#cs.ds_subs
            ),
            %% 5. Remove all streams that belong to this SubId:
            {ok, GS, HostState};
        error ->
            {error, not_found}
    end.

-spec renew_streams_(t(), sub_id(), sub(), _HostState) -> t().
renew_streams_(
    GS = #cs{cbm = CBM},
    SubId,
    #sub{
        db = DB,
        topic = Topic,
        start_time = StartTime
    },
    HostState
) ->
    lists:foldl(
        fun(Shard, Acc) ->
            Gen = get_current_generation(CBM, SubId, Shard, HostState),
            plan(
                #eff_renew_streams{
                    sub_id = SubId,
                    db = DB,
                    shard = Shard,
                    topic = Topic,
                    start_time = StartTime,
                    current_generation = Gen
                },
                Acc
            )
        end,
        GS,
        emqx_ds:list_shards(DB)
    ).

%%------------------------------------------------------------------------------
%% Effect handler
%%------------------------------------------------------------------------------

-spec result_handler(effect(), t(), _Result, HostState) -> {t(), HostState}.
result_handler(
    #eff_watch_streams{sub_id = SubId},
    GS0 = #cs{new_streams_watches = Watches},
    HostState,
    Watch
) ->
    GS = GS0#cs{new_streams_watches = Watches#{Watch => SubId}},
    {GS, HostState};
result_handler(
    #eff_unwatch_streams{watch = Watch},
    GS0 = #cs{new_streams_watches = Watches},
    HostState,
    _Result
) ->
    GS = GS0#cs{new_streams_watches = maps:remove(Watch, Watches)},
    {GS, HostState};
result_handler(
    Eff = #eff_renew_streams{sub_id = SubId, shard = Shard},
    GS,
    HostState,
    {Streams, Errors}
) ->
    case Errors of
        [] ->
            update_streams(GS, SubId, Shard, Streams, HostState);
        _ ->
            {
                retry(Errors, Eff, GS),
                HostState
            }
    end;
result_handler(Eff = #eff_make_iterator{}, GS0, HostState0, Result) ->
    case Result of
        {ok, It} ->
            handle_add_iterator(Eff, GS0, HostState0, It);
        ?err_rec(Err) ->
            {
                retry(Err, Eff, GS0),
                HostState0
            };
        ?err_unrec(Err) ->
            handle_make_iterator_fail(Eff, GS0, HostState0, Err)
    end;
result_handler(Eff = #eff_ds_sub{}, GS, HostState, Result) ->
    case Result of
        {ok, Handle, SubRef} ->
            {
                handle_new_sub(Eff, GS, Handle, SubRef),
                HostState
            };
        ?err_rec(Err) ->
            {
                retry(Err, Eff, GS),
                HostState
            }
    end;
result_handler(#eff_ds_unsub{ref = Ref}, GS, HostState, _Result) ->
    #cs{ds_subs = DSSubs} = GS,
    {
        GS#cs{ds_subs = maps:remove(Ref, DSSubs)},
        HostState
    }.

handle_new_sub(Eff, GS0, Handle, SubRef) ->
    #eff_ds_sub{sub_id = SubId, db = DB, slab = Slab, stream = Stream, iterator = It} = Eff,
    GS = #cs{ds_subs = DSSubs} = activate_stream(GS0, SubId, Slab, Stream, It, SubRef),
    DSSub = #ds_sub{
        id = SubId,
        handle = Handle,
        stream = Stream
    },
    GS#cs{
        ds_subs = DSSubs#{SubRef => DSSub}
    }.

handle_add_iterator(Eff, GS0, HostState0, It) ->
    #cs{cbm = CBM, subs = Subs} = GS0,
    #eff_make_iterator{sub_id = SubId, db = DB, slab = Slab, stream = Stream} = Eff,
    %% FIXME: add stram to active
    GS = activate_stream(GS0, SubId, Slab, Stream, It, undefined),
    case on_new_iterator(CBM, SubId, Slab, Stream, It, HostState0) of
        {subscribe, HostState} ->
            #{SubId := #sub{ds_sub_opts = SubOpts}} = Subs,
            {
                plan(
                    #eff_ds_sub{
                        sub_id = SubId,
                        db = DB,
                        slab = Slab,
                        stream = Stream,
                        iterator = It,
                        sub_options = SubOpts
                    },
                    GS
                ),
                HostState
            };
        {ignore, HostState} ->
            {
                GS,
                HostState
            }
    end.

-doc """
Handle unrecoverable errors that happen during creation of iterators.
""".
handle_make_iterator_fail(Eff, GS = #cs{cbm = CBM}, HostState, Err) ->
    #eff_make_iterator{
        sub_id = SubId, db = DB, slab = Slab, stream = Stream, topic = Topic, start_time = StartTime
    } = Eff,
    ?tp(error, emqx_ds_client_make_iterator_fail, #{
        unrecoverable => Err,
        sub_id => SubId,
        db => DB,
        slab => Slab,
        topic => Topic,
        stream => Stream,
        start_time => StartTime
    }),
    {
        forget_stream(GS, SubId, Slab, Stream),
        on_make_iterator_fail(CBM, SubId, Slab, Stream, Err, HostState)
    }.

-spec real_world(effect(), undefined) -> {_Result, undefined}.
real_world(#eff_watch_streams{db = DB, topic = Topic}, State) ->
    {ok, Watch} = emqx_ds_new_streams:watch(DB, Topic),
    {Watch, State};
real_world(#eff_unwatch_streams{db = DB, watch = Watch}, State) ->
    Result = emqx_ds_new_streams:unwatch(DB, Watch),
    {Result, State};
real_world(
    #eff_renew_streams{
        db = DB,
        shard = Shard,
        topic = Topic,
        start_time = StartTime,
        current_generation = Gen
    },
    State
) ->
    Result = emqx_ds:get_streams(DB, Topic, StartTime, #{shard => Shard, generation_min => Gen}),
    {Result, State};
real_world(#eff_make_iterator{db = DB, stream = Stream, topic = TF, start_time = StartTime}, State) ->
    Result = emqx_ds:make_iterator(DB, Stream, TF, StartTime),
    {Result, State}.

%%------------------------------------------------------------------------------
%% Stream management
%%------------------------------------------------------------------------------

-spec activate_stream(
    t(),
    sub_id(),
    emqx_ds:slab(),
    emqx_ds:stream(),
    emqx_ds:iterator(),
    emqx_ds:sub_ref() | undefined
) ->
    t().
activate_stream(GS = #cs{streams = Streams}, SubId, {Shard, _Gen}, Stream, Iterator, MaybeSubRef) ->
    Key = {SubId, Shard},
    #{Key := Cache0} = Streams,
    #stream_cache{pending_iterator = Pending, active = Active} = Cache0,
    Cache = Cache0#stream_cache{
        pending_iterator = Pending -- [Stream],
        active = Active#{stream => {Iterator, MaybeSubRef}}
    },
    GS#cs{streams = Streams#{Key => Cache}}.

-spec forget_stream(t(), sub_id(), emqx_ds:slab(), emqx_ds:stream()) -> t().
forget_stream(GS = #cs{streams = Streams}, SubId, {Shard, Gen}, Stream) ->
    Key = {SubId, Shard},
    case Streams of
        #{Key := Cache0} ->
            #stream_cache{
                pending_iterator = Pending, active = Active, replayed = Replayed, future = Future
            } = Cache0,
            Cache = Cache0#stream_cache{
                pending_iterator = Pending -- [Stream],
                active = maps:remove(Stream, Active),
                replayed = maps:remove(Stream, Replayed),
                future = gb_sets:delete({Gen, Stream}, Future)
            },
            GS#cs{streams = Streams#{Key => Cache}};
        #{} ->
            GS
    end.

-spec update_streams(
    t(), sub_id(), emqx_ds:shard(), [{emqx_ds:slab(), emqx_ds:stream()}], HostState
) ->
    {t(), HostState}.
update_streams(GS0 = #cs{cbm = CBM, streams = Streams0}, SubId, Shard, Streams, HostState) ->
    Key = {SubId, Shard},
    %% Get the existing cache of create an initialize the empty record:
    case Streams0 of
        #{Key := StreamCache0} ->
            ok;
        #{} ->
            StreamCache0 = #stream_cache{
                current_gen = get_current_generation(CBM, SubId, Shard, HostState)
            }
    end,
    %% Update the cache:
    {GS1, StreamCache} = do_update_streams(
        GS0,
        HostState,
        SubId,
        Shard,
        StreamCache0,
        Streams
    ),
    GS2 = GS1#cs{streams = Streams0#{Key => StreamCache}},
    %% Should we advance some generations?
    maybe_advance_generations(GS2, HostState).

-spec do_update_streams(t(), _HostState, sub_id(), emqx_ds:shard(), stream_cache(), [
    {emqx_ds:slab(), emqx_ds:stream()}
]) ->
    stream_cache().
do_update_streams(GS0, HostState, SubId, Shard, Cache0, Streams) ->
    lists:foldl(
        fun({{_Shard, Generation}, Stream}, {AccGS, AccCache}) ->
            add_stream_to_cache(AccGS, HostState, SubId, Shard, AccCache, Generation, Stream)
        end,
        {GS0, Cache0},
        Streams
    ).

add_stream_to_cache(
    GS,
    _HostState,
    _SubId,
    _Shard,
    Cache = #stream_cache{current_gen = Current},
    Generation,
    _Stream
) when Generation < Current ->
    %% Should not happen:
    {
        GS,
        Cache
    };
add_stream_to_cache(
    GS,
    _HostState,
    _SubId,
    _Shard,
    Cache = #stream_cache{current_gen = Current, future = Future0},
    Generation,
    Stream
) when Generation > Current ->
    %% This is a stream we'll replay in the future:
    Future = gb_sets:add_element({Generation, Stream}, Future0),
    {
        GS,
        Cache#stream_cache{future = Future}
    };
add_stream_to_cache(
    GS0, HostState, SubId, Shard, Cache0 = #stream_cache{current_gen = Current}, Generation, Stream
) when Generation =:= Current ->
    #stream_cache{
        pending_iterator = Pending,
        active = Active,
        replayed = Replayed
    } = Cache0,
    %% First handle the most likely case when the stream is already
    %% active, then check if the stream is already replayed, then
    %% check if the stream is pending for creation of the iterator:
    case
        maps:is_key(Stream, Active) orelse maps:is_key(Stream, Replayed) orelse
            lists:member(Stream, Pending)
    of
        true ->
            %% This stream is already known:
            {GS0, Cache0};
        false ->
            %% This stream is new (for us). Does the host already have the iterator?
            case get_iterator(GS0#cs.cbm, SubId, {Shard, Generation}, Stream, HostState) of
                {ok, end_of_stream} ->
                    %% This is a known replayed stream:
                    Cache = Cache0#stream_cache{replayed = Replayed#{Stream => true}},
                    {GS0, Cache};
                {ok, It} ->
                    %% This is a known in-progress stream:
                    #sub{db = DB, ds_sub_opts = DSSubOpts} = maps:get(
                        SubId, GS0#cs.subs
                    ),
                    Cache = Cache0#stream_cache{active = Active#{Stream => {It, undefined}}},
                    GS = plan(
                        #eff_ds_sub{
                            sub_id = SubId,
                            db = DB,
                            slab = {Shard, Generation},
                            stream = Stream,
                            iterator = It,
                            sub_options = DSSubOpts
                        },
                        GS0
                    ),
                    {GS, Cache};
                undefined ->
                    %% Schedule creation of the iterator:
                    #sub{db = DB, topic = Topic, start_time = StartTime} = maps:get(
                        SubId, GS0#cs.subs
                    ),
                    GS = plan(
                        #eff_make_iterator{
                            sub_id = SubId,
                            db = DB,
                            slab = {Shard, Generation},
                            stream = Stream,
                            topic = Topic,
                            start_time = StartTime
                        },
                        GS0
                    ),
                    Cache = Cache0#stream_cache{pending_iterator = [Stream | Pending]},
                    {GS, Cache}
            end
    end.

maybe_advance_generations(GS0, HostState) ->
    maps:fold(
        fun(Key, Cache0, {GSAcc, HSAcc}) ->
            maybe_advance_generation(Key, Cache0, GSAcc, HSAcc)
        end,
        {GS0, HostState},
        GS0#cs.streams
    ).

maybe_advance_generation(
    Key = {SubId, Shard}, Cache0, GS0 = #cs{cbm = CBM, streams = Streams, subs = Subs}, HostState0
) ->
    case is_fully_replayed(Cache0) of
        false ->
            %% Generation is not fully replayed:
            {GS0, HostState0};
        {true, NextGen, StreamsOfNextGen, Future} ->
            %% Advance generation:
            #{SubId := #sub{db = DB, topic = Topic, start_time = StartTime}} = Subs,
            %% Here we don't ask the host if it has the iterator: it
            %% should not, otherwise replay order would be violated.
            Cache = #stream_cache{
                current_gen = NextGen,
                pending_iterator = StreamsOfNextGen,
                future = Future
            },
            %% Schedule creation of iterators for the new streams:
            GS2 = lists:foldl(
                fun(Stream, GS1) ->
                    plan(
                        #eff_make_iterator{
                            sub_id = SubId,
                            db = DB,
                            slab = {Shard, NextGen},
                            stream = Stream,
                            topic = Topic,
                            start_time = StartTime
                        },
                        GS1
                    )
                end,
                GS0,
                StreamsOfNextGen
            ),
            GS = GS2#cs{streams = Streams#{Key := Cache}},
            HostState = on_advance_generation(CBM, SubId, Shard, NextGen, HostState0),
            {GS, HostState}
    end.

-spec is_fully_replayed(stream_cache()) ->
    {true, emqx_ds:generation(), [emqx_ds:stream(), ...],
        gb_sets:set({emqx_ds:generation(), emqx_ds:stream()})}
    | false.
is_fully_replayed(#stream_cache{
    current_gen = Current, pending_iterator = Pending, active = Active, future = Future0
}) ->
    maybe
        [] ?= Pending,
        0 ?= maps:size(Active),
        {NextGen, Streams, Future} ?= pop_future_streams(Current, undefined, Future0, []),
        {true, NextGen, Streams, Future}
    else
        _ ->
            false
    end.

pop_future_streams(Current, NextGen, Future0, Acc) ->
    case gb_sets:is_empty(Future0) of
        true when NextGen =:= undefined ->
            %% There are no cached streams with greater generation than `Current'
            undefined;
        true ->
            %% Next generation is also the last known one. We've
            %% consumed all of its streams:
            {NextGen, Acc, Future0};
        false ->
            {{Gen, Stream}, Future} = gb_sets:take_smallest(Future0),
            case is_integer(NextGen) of
                false when Gen > Current ->
                    %% Found the next generation to replay:
                    pop_future_streams(Current, Gen, Future, [Stream]);
                true when Gen =:= NextGen ->
                    pop_future_streams(Current, Gen, Future, [Stream | Acc]);
                true when Gen > NextGen ->
                    %% Reached the end of NextGen:
                    {NextGen, Acc, Future0}
            end
    end.

%%------------------------------------------------------------------------------
%% Functions for manipulating the plan:
%%------------------------------------------------------------------------------

-spec plan(effect(), t()) -> t().
plan(Effect, GS = #cs{plan = Plan}) ->
    GS#cs{plan = [Effect | Plan]}.

-doc """
A wrapper of `retry/2` that prints a message before adding effect to the retry queue.
""".
-spec retry(_Reason, effect(), t()) -> t().
retry(Reason, Effect, GS) ->
    ?tp(info, emqx_ds_client_retry, #{action => Effect, reason => Reason}),
    retry(Effect, GS).

-spec retry(effect(), t()) -> t().
retry(Effect, GS0 = #cs{retry = Retry, ref = Ref, options = #{retry_interval := RetryInterval}}) ->
    GS = GS0#cs{retry = [Effect | Retry]},
    case GS0#cs.retry_tref of
        undefined ->
            TRef = erlang:send_after(RetryInterval, self(), #emqx_ds_client_retry{ref = Ref}),
            GS#cs{retry_tref = TRef};
        TRef when is_reference(TRef) ->
            GS
    end.

-spec eff_subid(effect()) -> sub_id() | undefined.
eff_subid(#eff_renew_streams{sub_id = SubId}) -> SubId;
eff_subid(#eff_watch_streams{sub_id = SubId}) -> SubId;
eff_subid(#eff_unwatch_streams{}) -> undefined;
eff_subid(#eff_ds_sub{sub_id = SubId}) -> SubId;
eff_subid(#eff_ds_unsub{}) -> undefined.

-spec filter_effects(fun((effect()) -> boolean()), t()) -> t().
filter_effects(Pred, GS = #cs{plan = Plan, retry = Retry}) ->
    GS#cs{plan = lists:filter(Pred, Plan), retry = lists:filter(Pred, Retry)}.

%%------------------------------------------------------------------------------
%% The interpreter:
%%------------------------------------------------------------------------------

-doc """

This module is architected as following:

- It's assumed that all APIs are executed in a process called host.
  Host keeps the state of the client, and it has its own state that is unknown to us.

- Host interacts with the client via API calls, such as `subscribe`, `unsubscribe`, `ack`, etc.

- Client interacts with the host via callbacks defined in this module.
  Callbacks can query and mutate state of the host.
  Host state (or relevant parts of thereof) is threaded through the client logic.

The logic is split up between three types of functions:

- Planners
- Effect handler
- Result handler

Planners are triggered by the API calls (`subscribe`, `unsubscribe`, `destroy`, etc.).
Planners create sequences of effects that are fed into the interpreter (`execute`).

The interpreter feeds the effects into the effect handler, which evaluates them,
returns the result and updates its own state (`EffHandlerState`).
The results are then fed into the result handler,
which can further mutate client and host states, and schedule more effects if needed.

Effects are stored in the client state in two queues: `plan` and `retry`.
Effects in the `plan` queue are executed immediately,
while `retry` effects are executed on retry timeout.

This architecture isolates all interactions with the real world in the effect handler,
which is a trivial wrapper of DS API,
and allows to implement all complex logic as pure functions in the planners and result handler.

It also allows to create a fake effect handler for property-based testing.

On the flip side, we now have to deal with three separate states:

- Host state
- Client state
- Effect handler state

The good news is that the last one is only used for testing.
`real_world` effect handler ignores its state.

""".

-spec execute(t(), HostState) -> {t(), HostState}.
execute(GS, HostState) ->
    execute(#cs.plan, fun real_world/2, undefined, fun result_handler/4, GS, HostState).

-spec execute(
    integer(), effect_handler(EffHandlerState), EffHandlerState, result_handler(), t(), HostState
) -> {EffHandlerState, t(), HostState}.
execute(Field, EffectHandler, EffHandlerState0, ResultHandler, GS0, Acc0) ->
    case element(Field, GS0) of
        [] ->
            %% No planned effects:
            {EffHandlerState0, GS0, Acc0};
        Effects ->
            %% Clear effects in the state:
            GS1 = erlang:setelement(Field, GS0, []),
            {EffHandlerState, GS, Acc} = do_execute(
                EffectHandler, EffHandlerState0, ResultHandler, lists:reverse(Effects), GS1, Acc0
            ),
            execute(Field, EffectHandler, EffHandlerState, ResultHandler, GS, Acc)
    end.

-spec do_execute(
    effect_handler(EffHandlerState), EffHandlerState, result_handler(), [effect()], t(), HostState
) -> {EffHandlerState, t(), HostState}.
do_execute(_, EffHandlerState, _, [], GS, Acc) ->
    {EffHandlerState, GS, Acc};
do_execute(EffectHandler, EffHandlerState0, ResultHandler, [Effect | Effects], GS0, Acc0) ->
    {Result, EffHandlerState} = EffectHandler(Effect, EffHandlerState0),
    %% io:format(user, "Exec ~p -> ~p~n", [Effect, Result]),
    {GS, Acc} = ResultHandler(Effect, GS0, Acc0, Result),
    do_execute(EffectHandler, EffHandlerState, ResultHandler, Effects, GS, Acc).

%%------------------------------------------------------------------------------
%% Callback module wrappers:
%%------------------------------------------------------------------------------

-spec get_current_generation(module(), sub_id(), emqx_ds:shard(), _HostState) ->
    emqx_ds:generation().
get_current_generation(CBM, SubId, Shard, HostState) ->
    CBM:get_current_generation(SubId, Shard, HostState).

-spec get_iterator(module(), sub_id(), emqx_ds:slab(), emqx_ds:stream(), _HostState) ->
    {ok, emqx_ds:iterator() | end_of_stream} | undefined.
get_iterator(CBM, SubId, Slab, Stream, HostState) ->
    CBM:get_iterator(SubId, Slab, Stream, HostState).

-spec on_advance_generation(module(), sub_id(), emqx_ds:shard(), emqx_ds:generation(), HostState) ->
    HostState.
on_advance_generation(CBM, SubId, Shard, NewCurrentGeneration, HostState) ->
    CBM:on_advance_generation(SubId, Shard, NewCurrentGeneration, HostState).

-spec on_new_iterator(
    module(), sub_id(), emqx_ds:slab(), emqx_ds:stream(), emqx_ds:iterator(), HostState
) ->
    {subscribe | ignore, HostState}.
on_new_iterator(CBM, SubId, Slab, Stream, Iterator, HostState) ->
    CBM:on_new_iterator(SubId, Slab, Stream, Iterator, HostState).

-spec on_make_iterator_fail(
    module(), sub_id(), emqx_ds:slab(), emqx_ds:stream(), _Error, HostState
) ->
    HostState.
on_make_iterator_fail(CBM, SubId, Slab, Stream, Error, HostState) ->
    CBM:on_make_iterator_fail(SubId, Slab, Stream, Error, HostState).
