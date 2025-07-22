%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_DS_CLIENT_INTERNALS_HRL).
-define(EMQX_DS_CLIENT_INTERNALS_HRL, true).

-record(sub, {
    db :: emqx_ds:db(),
    topic :: emqx_ds:topic_filter(),
    start_time :: emqx_ds:time(),
    ds_sub_opts :: emqx_ds:sub_opts()
}).

-define(ds_sub_a_seqno, 1).
-define(ds_sub_a_stuck, 2).
-define(ds_sub_a_lagging, 3).

-record(ds_sub, {
    id :: emqx_ds_client:sub_id(),
    slab :: emqx_ds:slab(),
    db :: emqx_ds:db(),
    stream :: emqx_ds:stream(),
    handle :: emqx_ds:subscription_handle(),
    %% SeqNo, Stuck, Lagging:
    vars = atomics:new(3, [{signed, false}]) :: atomics:atomics_ref()
}).

-record(stream_cache, {
    current_gen :: emqx_ds:generation(),
    %% Streams that belong to the current generation:
    %%    Streams that are known, but currently lack the iterator:
    pending_iterator = [] :: [emqx_ds:stream()],
    %%    Streams that have the iterator:
    active = #{} :: #{emqx_ds:stream() => {emqx_ds:iterator(), emqx_ds:sub_ref() | undefined}},
    %%    Streams that have been fully replayed:
    replayed = #{} :: #{emqx_ds:stream() => true},
    %% Streams that belong to the future generations (sorted by generation):
    future = gb_sets:new() :: gb_sets:set({emqx_ds:generation(), emqx_ds:stream()})
}).

%% Client state
-record(cs, {
    ref = make_ref() :: reference(),
    cbm :: module(),
    options :: #{retry_interval := non_neg_integer()},
    %% Retry:
    retry_tref :: reference() | undefined,
    %% "Logical" subs that hold data for each subscription created by `subscribe' API:
    subs = #{} :: emqx_ds_client:subs(),
    streams = #{} :: emqx_ds_client:streams(),
    new_streams_watches = #{} :: emqx_ds_client:watches(),
    ds_subs = #{} :: emqx_ds_client:ds_subs(),
    plan = [] :: emqx_ds_client:effects(),
    retry = [] :: emqx_ds_client:effects()
}).

%% Effects:
-record(eff_renew_streams, {
    sub_id :: emqx_ds_client:sub_id(),
    db :: emqx_ds:db(),
    shard :: emqx_ds:shard(),
    topic :: emqx_ds:topic_filter(),
    start_time :: emqx_ds:time(),
    current_generation :: emqx_ds:generation()
}).

-record(eff_watch_streams, {
    sub_id :: emqx_ds_client:sub_id(),
    db :: emqx_ds:db(),
    topic :: emqx_ds:topic_filter()
}).

-record(eff_unwatch_streams, {
    db :: emqx_ds:db(),
    watch :: emqx_ds_new_streams:watch()
}).

-record(eff_make_iterator, {
    sub_id :: emqx_ds_client:sub_id(),
    db :: emqx_ds:db(),
    slab :: emqx_ds:slab(),
    stream :: emqx_ds:stream(),
    topic :: emqx_ds:topic_filter(),
    start_time :: emqx_ds:time()
}).

-record(eff_ds_sub, {
    sub_id :: emqx_ds_client:sub_id(),
    db :: emqx_ds:db(),
    slab :: emqx_ds:slab(),
    stream :: emqx_ds:stram(),
    iterator :: emqx_ds:iterator(),
    sub_options :: emqx_ds:sub_opts()
}).

-record(eff_ds_unsub, {
    ref :: reference(),
    db :: emqx_ds:db(),
    handle :: emqx_ds:subscription_handle()
}).

-define(record_to_map(RECORD, VALUE),
    (fun(Val) ->
        Fields = record_info(fields, RECORD),
        [_Tag | Values] = tuple_to_list(Val),
        maps:from_list(lists:zip(Fields, Values))
    end)(
        VALUE
    )
).

-endif.
