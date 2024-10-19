%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This process is responsible for processing async poll requests
%% from the consumers. It is designed as a generic, drop-in "frontend"
%% that can be integrated into any DS backend (builtin or external).
%%
%% It serves as a pool for such requests, limiting the number of
%% queries running in parallel. In addition, it tries to group
%% "coherent" poll requests together, so they can be fulfilled as a
%% group ("coherent beam").
%%
%% By "coherent" we mean requests to scan overlapping key ranges of
%% the same DS stream. Grouping requests helps to reduce the number of
%% storage queries and conserve throughput of the EMQX backplane
%% network. This should help in the situations when the majority of
%% clients are up to date and simply wait for the new data.
%%
%% Beamformer works as following:
%%
%% - Initially, requests are added to the "pending" queue.
%%
%% - Beamformer process spins in a "fulfill loop" that takes requests
%% from the pending queue one at a time, and tries to fulfill them
%% normally by quering the storage.
%%
%% - When storage returns a non-empty batch, an unrecoverable error or
%% `end_of_stream', beamformer searches for pending poll requests that
%% may be coherent with the current one. All matching requests are
%% then packed into "beams" (one per destination node) and sent out
%% accodingly.
%%
%% - If the query returns an empty batch, beamformer moves the request
%% to the "wait" queue. Poll requests just linger there until they
%% time out, or until beamformer receives a matching stream event from
%% the storage.
%%
%% The storage backend can send events to the beamformer by calling
%% `shard_event' function.
%%
%% Storage event processing logic is following: if beamformer finds
%% waiting poll requests matching the event, it queries the storage
%% for a batch of data. If batch is non-empty, requests are served
%% exactly as described above. If batch is empty again, request is
%% moved back to the wait queue.

%% WARNING: beamformer makes some implicit assumptions about the
%% storage layout:
%%
%% - For each topic filter and stream, there's a bijection between
%% iterator and the message key
%%
%% - Quering a stream with non-wildcard topic-filter is equivalent to
%% quering it with a wildcard topic filter and dropping messages in
%% postprocessing, e.g.:
%%
%% ```
%% next("foo/bar", StartTime) ==
%%   filter(λ msg. msg.topic == "foo/bar",
%%         next("#", StartTime))
%% '''
-module(emqx_ds_beamformer).

-behaviour(gen_server).

%% API:
-export([poll/5, shard_event/2]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([start_link/4, do_dispatch/1]).

-export_type([
    opts/0,
    beam/2, beam/0,
    return_addr/1,
    unpack_iterator_result/1,
    event_topic/0,
    stream_scan_return/0,
    iterator_type/0
]).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-include("emqx_ds.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

%% `event_topic' and `event_topic_filter' types are structurally (but
%% not semantically) equivalent to their `emqx_ds' counterparts.
%%
%% These types are only used for matching of events against waiting
%% poll requests. Values of these types are never exposed outside.
%% Hence the backend can, for example, compress topics used in the
%% events and iterators.
-type event_topic() :: emqx_ds:topic().
-type event_topic_filter() :: emqx_ds:topic_filter().

-type opts() :: #{
    n_workers := non_neg_integer()
}.

%% Request:

-type return_addr(ItKey) :: {reference(), ItKey}.

-record(poll_req, {
    stream,
    topic_filter,
    start_key,
    %% Node from which the poll request originates:
    node,
    %% Information about the process that created the request:
    return_addr,
    %% Iterator:
    it,
    %% Callback that filters messages that belong to the request:
    msg_matcher,
    opts,
    deadline
}).

-type poll_req(ItKey, Iterator) ::
    #poll_req{
        stream :: _Stream,
        topic_filter :: event_topic_filter(),
        start_key :: emqx_ds:message_key(),
        node :: node(),
        return_addr :: return_addr(ItKey),
        it :: Iterator,
        msg_matcher :: match_messagef(),
        opts :: emqx_ds:poll_opts(),
        deadline :: integer()
    }.

%% Response:

-type dispatch_mask() :: bitstring().

-record(beam, {iterators, pack, misc = #{}}).

-opaque beam(ItKey, Iterator) ::
    #beam{
        iterators :: [{return_addr(ItKey), Iterator}],
        pack ::
            [{emqx_ds:message_key(), dispatch_mask(), emqx_types:message()}]
            | end_of_stream
            | emqx_ds:error(_),
        misc :: #{}
    }.

-type beam() :: beam(_ItKey, _Iterator).

-type stream_scan_return() ::
    {ok, emqx_ds:message_key(), [{emqx_ds:message_key(), emqx_types:message()}]}
    | {ok, end_of_stream}
    | emqx_ds:error(_).

-record(s, {
    module :: module(),
    metrics_id,
    shard,
    name,
    old_queue :: ets:tid(),
    pending_request_limit :: non_neg_integer(),
    wait_queue :: ets:tid(),
    batch_size :: non_neg_integer()
}).

-type s() :: #s{}.

-record(shard_event, {
    events :: [{_Stream, event_topic()}]
}).

-define(fulfill_loop, beamformer_fulfill_loop).
-define(housekeeping_loop, housekeeping_loop).

-type iterator_type() :: committed | committed_fresh | future.

-type oldq_key() :: {_Stream, event_topic(), emqx_ds:message_key()}.

%% -type freshq_key() :: {_Stream, emqx_ds:message_key()}.

%%================================================================================
%% Callbacks
%%================================================================================

-type match_messagef() :: fun((emqx_ds:message_key(), emqx_types:message()) -> boolean()).

-type unpack_iterator_result(Stream) :: #{
    stream := Stream,
    topic_filter := event_topic_filter(),
    last_seen_key := emqx_ds:message_key(),
    timestamp := emqx_ds:time(),
    message_matcher := match_messagef()
}.

-callback unpack_iterator(_Shard, _Iterator) ->
    unpack_iterator_result(_Stream) | emqx_ds:error(_).

-callback update_iterator(_Shard, Iterator, emqx_ds:message_key()) ->
    emqx_ds:make_iterator_result(Iterator).

-callback scan_stream(_Shard, _Stream, _TopicFilter, _StartKey, _BatchSize :: non_neg_integer()) ->
    stream_scan_return().

%% Once poll request is received by the beamformer process, it asks
%% the backend to classify it. Then beamformer will fulfill it using
%% one of three different strategies:
%%
%% 1. `future' iterators point beyond the tip of the stream. They will
%% be fulfilled by new data. Poll requests for such iterators are sent
%% to the waiting queue, where they stay put awaiting the event.
%%
%% 2. `committed_fresh' iterators point to the existing data that was
%% written recently (as is decided by the backend). Beamformer assumes
%% that _most_ of poll requests are concentrated near the tip of the
%% stream and volume of "fresh" data is limited. As such, it tries to
%% reuse fresh data aggressively: it scans the stream using wildcard
%% topic filter (bypassing any backend-internal filtering based on
%% topic filter) in hope that the data can be reused.
%%
%% 3. `committed' iterators point to the exising data that was written
%% long time ago2. Beamformer uses pessimistic strategy when fulfilling
%% "old" poll requests. It doesn't try to agressively reuse the data,
%% and instead it relies on the backend's internal filtering.
%%
%% Note: marking iterators as `committed_fresh' is an optional
%% optimization. The backend MAY only use `future' and `committed'
%% types.
%%
%% The backend MUST send shard events for all changes that happen
%% after it reported matching iterator as `future'.
%%
%% Note: this is a separate callback (and not part of
%% `unpack_iterator') to avoid race condition when iterator is
%% classified as `future' (and added to the waiting queue) and sending
%% the event waking it up.
%%
%% Note: this callback is executed for every incoming poll request,
%% possibly multiple times!
-callback classify_iterator(_Shard, _Iterator) -> {ok, iterator_type()} | emqx_ds:error().

%%================================================================================
%% API functions
%%================================================================================

%% @doc Submit a poll request
-spec poll(node(), return_addr(_ItKey), _Shard, _Iterator, emqx_ds:poll_opts()) ->
    ok.
poll(Node, ReturnAddr, Shard, Iterator, Opts = #{timeout := Timeout}) ->
    CBM = emqx_ds_beamformer_sup:cbm(Shard),
    case CBM:unpack_iterator(Shard, Iterator) of
        #{
            stream := Stream,
            topic_filter := TF,
            last_seen_key := DSKey,
            timestamp := Timestamp,
            message_matcher := MsgMatcher
        } ->
            Deadline = erlang:monotonic_time(millisecond) + Timeout,
            ?tp(beamformer_poll, #{
                shard => Shard, key => DSKey, timeout => Timeout, deadline => Deadline
            }),
            %% Try to maximize likelyhood of sending similar iterators to the
            %% same worker:
            Token = {Stream, Timestamp div 10_000_000},
            Worker = gproc_pool:pick_worker(
                emqx_ds_beamformer_sup:pool(Shard),
                Token
            ),
            %% Make request:
            Req = #poll_req{
                stream = Stream,
                topic_filter = TF,
                start_key = DSKey,
                node = Node,
                return_addr = ReturnAddr,
                it = Iterator,
                opts = Opts,
                deadline = Deadline,
                msg_matcher = MsgMatcher
            },
            emqx_ds_builtin_metrics:inc_poll_requests(shard_metrics_id(Shard), 1),
            %% Currently we implement backpressure by ignoring transient
            %% errors (gen_server timeouts, `too_many_requests'), and just
            %% letting poll requests expire at the higher level. This should
            %% hold back the caller.
            try gen_server:call(Worker, Req, Timeout) of
                ok -> ok;
                {error, recoverable, too_many_requests} -> ok
            catch
                exit:timeout ->
                    ok
            end;
        Err = {error, _, _} ->
            Beam = #beam{
                iterators = [{ReturnAddr, Iterator}],
                pack = Err
            },
            send_out(Node, Beam)
    end.

%% @doc This internal API notifies the beamformer that new data is
%% available for reading in the storage.
%%
%% Backends should call this function after committing data to the
%% storage, so waiting long poll requests can be fulfilled.
%%
%% Types of arguments to this function are dependent on the backend.
%%
%% - `_Shard': most DS backends have some notion of shard. Beamformer
%% uses this fact to partition poll requests, but otherwise it treats
%% shard as an opaque value.
%%
%% - `_Stream': backend- and layout-specific type of stream.
%% Beamformer uses exact matching on streams when it searches for
%% similar requests and when it matches events. Otherwise it's an
%% opaque type.
%%
%% - `event_topic()': When beamformer receives stream events, it
%% selects waiting events with matching stream AND
%% `event_topic_filter'.
-spec shard_event(_Shard, [{_Stream, event_topic()}]) -> ok.
shard_event(Shard, Events) ->
    Workers = gproc_pool:active_workers(emqx_ds_beamformer_sup:pool(Shard)),
    lists:foreach(
        fun({_, Pid}) ->
            Pid ! #shard_event{events = Events}
        end,
        Workers
    ).

%%================================================================================
%% behavior callbacks
%%================================================================================

init([CBM, ShardId, Name, _Opts]) ->
    process_flag(trap_exit, true),
    Pool = emqx_ds_beamformer_sup:pool(ShardId),
    gproc_pool:add_worker(Pool, Name),
    gproc_pool:connect_worker(Pool, Name),
    S = #s{
        module = CBM,
        shard = ShardId,
        metrics_id = shard_metrics_id(ShardId),
        name = Name,
        old_queue = oldq_new(),
        wait_queue = emqx_ds_beamformer_waitq:new(),
        pending_request_limit = cfg_pending_request_limit(),
        batch_size = cfg_batch_size()
    },
    self() ! ?housekeeping_loop,
    {ok, S}.

handle_call(
    Req = #poll_req{},
    _From,
    S0 = #s{old_queue = OldQueue, wait_queue = WaitingTab, metrics_id = Metrics}
) ->
    %% FIXME
    %% this is a potentially costly operation
    PQLen = ets:info(OldQueue, size),
    WQLen = ets:info(WaitingTab, size),
    emqx_ds_builtin_metrics:set_pendingq_len(Metrics, PQLen),
    emqx_ds_builtin_metrics:set_waitq_len(Metrics, WQLen),
    case PQLen + WQLen >= S0#s.pending_request_limit of
        true ->
            emqx_ds_builtin_metrics:inc_poll_requests_dropped(Metrics, 1),
            Reply = {error, recoverable, too_many_requests},
            {reply, Reply, S0};
        false ->
            {Result, S} = enqueue(Req, S0),
            {reply, Result, S}
    end;
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

enqueue(
    Req = #poll_req{stream = Stream, topic_filter = TopicFilter, it = It0, return_addr = Id},
    S = #s{old_queue = OldQ, wait_queue = WaitingTab, module = CBM, shard = ShardId}
) ->
    %% Check if the request belongs to committed range:
    case CBM:classify_iterator(ShardId, It0) of
        future ->
            %% logger:warning("enqueue w~p~n  ~p", [HighWM, Req]),
            emqx_ds_beamformer_waitq:insert(Stream, TopicFilter, Id, Req, WaitingTab),
            {ok, S};
        _ ->
            %% logger:warning("enqueue p~p~n  ~p", [HighWM, Req]),
            oldq_push(OldQ, Req),
            ensure_fulfill_loop(),
            {ok, S}
    end.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(#shard_event{events = Events}, S) ->
    ?tp(debug, emqx_ds_beamformer_event, #{events => Events}),
    {noreply, maybe_fulfill_waiting(S, Events)};
handle_info(?fulfill_loop, S0) ->
    put(?fulfill_loop, false),
    S = fulfill_pending(S0),
    {noreply, S};
handle_info(?housekeeping_loop, S0) ->
    %% Reload configuration according from environment variables:
    S1 = S0#s{
        batch_size = cfg_batch_size(),
        pending_request_limit = cfg_pending_request_limit()
    },
    S = cleanup(S1),
    erlang:send_after(cfg_housekeeping_interval(), self(), ?housekeeping_loop),
    {noreply, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, #s{shard = ShardId, name = Name}) ->
    Pool = emqx_ds_beamformer_sup:pool(ShardId),
    gproc_pool:disconnect_worker(Pool, Name),
    gproc_pool:remove_worker(Pool, Name),
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link(module(), _Shard, integer(), opts()) -> {ok, pid()}.
start_link(Mod, ShardId, Name, Opts) ->
    gen_server:start_link(?MODULE, [Mod, ShardId, Name, Opts], []).

%% @doc RPC target: split the beam and dispatch replies to local
%% consumers.
-spec do_dispatch(beam()) -> ok.
do_dispatch(Beam = #beam{}) ->
    lists:foreach(
        fun({{Alias, ItKey}, Result}) ->
            Alias ! #poll_reply{ref = Alias, userdata = ItKey, payload = Result}
        end,
        split(Beam)
    ).

%%================================================================================
%% Internal functions
%%================================================================================

-spec ensure_fulfill_loop() -> ok.
ensure_fulfill_loop() ->
    case get(?fulfill_loop) of
        true ->
            ok;
        _ ->
            put(?fulfill_loop, true),
            self() ! ?fulfill_loop,
            ok
    end.

-spec cleanup(s()) -> s().
cleanup(S = #s{old_queue = OldQueue, wait_queue = WaitingTab, metrics_id = Metrics}) ->
    do_cleanup(Metrics, OldQueue),
    do_cleanup(Metrics, WaitingTab),
    S.

do_cleanup(Metrics, Tab) ->
    Now = erlang:monotonic_time(millisecond),
    MS = {#poll_req{_ = '_', deadline = '$1'}, [{'<', '$1', Now}], [true]},
    NDeleted = ets:select_delete(Tab, [MS]),
    emqx_ds_builtin_metrics:inc_poll_requests_expired(Metrics, NDeleted).

-spec fulfill_pending(s()) -> s().
fulfill_pending(S = #s{old_queue = OldQueue}) ->
    %% debug_pending(S),
    case oldq_find_oldest(OldQueue, 100) of
        undefined ->
            S;
        {Stream, TopicFilter, StartKey} ->
            ?tp(emqx_ds_beamformer_fulfill_pending, #{stream => Stream, topic => TopicFilter, start_key => StartKey}),
            %% The function MUST destructively consume all requests
            %% matching stream and MsgKey to avoid infinite loop:
            do_fulfill_pending(S, Stream, TopicFilter, StartKey),
            ensure_fulfill_loop(),
            S
    end.

do_fulfill_pending(
    S = #s{
        shard = Shard,
        module = CBM,
        old_queue = OldQueue,
        batch_size = BatchSize
    },
    Stream, TopicFilter, StartKey
) ->
    OnMatch = fun(_) -> ok end,
    %% Here we only group requests with exact match of the topic
    %% filter:
    GetF = oldq_pop(OldQueue, Stream, TopicFilter),
    Result = CBM:scan_stream(Shard, Stream, TopicFilter, StartKey, BatchSize),
    form_beams(S, GetF, OnMatch, move_to_waiting(S), StartKey, Result).

maybe_fulfill_waiting(S, []) ->
    S;
maybe_fulfill_waiting(
    S = #s{wait_queue = WaitingTab, module = CBM, shard = Shard, batch_size = BatchSize},
    [{Stream, UpdatedTopic} | Rest]
) ->
    case find_waiting(Stream, UpdatedTopic, WaitingTab) of
        undefined ->
            ?tp(emqx_ds_beamformer_fulfill_waiting, #{
                stream => Stream, topic => UpdatedTopic, candidates => undefined
            }),
            maybe_fulfill_waiting(S, Rest);
        {Candidates, TopicFilter, StartKey} ->
            ?tp(emqx_ds_beamformer_fulfill_waiting, #{
                stream => Stream,
                topic => UpdatedTopic,
                candidates => Candidates,
                start_time => StartKey
            }),
            GetF = fun(Key) -> maps:get(Key, Candidates, []) end,
            OnNomatch = fun(_) -> ok end,
            OnMatch = fun(Reqs) ->
                lists:foreach(
                    fun(#poll_req{stream = Str, topic_filter = TF, return_addr = Id}) ->
                            emqx_ds_beamformer_waitq:delete(Str, TF, Id, WaitingTab)
                    end,
                    Reqs
                )
            end,
            Result = CBM:scan_stream(Shard, Stream, TopicFilter, StartKey, BatchSize),
            case form_beams(S, GetF, OnMatch, OnNomatch, StartKey, Result) of
                true -> maybe_fulfill_waiting(S, [{Stream, UpdatedTopic} | Rest]);
                false -> maybe_fulfill_waiting(S, Rest)
            end
    end.

move_to_waiting(#s{wait_queue = WaitingTab}) ->
    fun(NoMatch) ->
        lists:foreach(
            fun(Req = #poll_req{stream = Stream, topic_filter = TopicFilter, return_addr = Id}) ->
                emqx_ds_beamformer_waitq:insert(Stream, TopicFilter, Id, Req, WaitingTab)
            end,
            NoMatch
        )
    end.

find_waiting(Stream, Topic, Tab) ->
    case emqx_ds_beamformer_waitq:matches(Stream, Topic, Tab) of
        [] ->
            undefined;
        [Fst | _] = Matches ->
            %% 1. Find all poll requests that match the topic of the
            %% event
            %%
            %% 2. Find most common topic filter for all these events
            %%
            %% 3. Find the smallest DS key
            lists:foldl(
                fun(Req, {Acc, AccTopic, AccKey}) ->
                    #poll_req{topic_filter = TF, start_key = StartKey} = Req,
                    {
                        map_pushl(StartKey, Req, Acc),
                        common_topic_filter(AccTopic, TF),
                        min(AccKey, StartKey)
                    }
                end,
                {#{}, Fst#poll_req.topic_filter, Fst#poll_req.start_key},
                Matches
            )
    end.

common_topic_filter([], []) ->
    [];
common_topic_filter(['#'], _) ->
    ['#'];
common_topic_filter(_, ['#']) ->
    ['#'];
common_topic_filter(['+' | L1], [_ | L2]) ->
    ['+' | common_topic_filter(L1, L2)];
common_topic_filter([_ | L1], ['+' | L2]) ->
    ['+' | common_topic_filter(L1, L2)];
common_topic_filter([A | L1], [A | L2]) ->
    [A | common_topic_filter(L1, L2)].

map_pushl(Key, Elem, Map) ->
    maps:update_with(Key, fun(L) -> [Elem | L] end, [Elem], Map).

%% @doc Split beam into individual batches
-spec split(beam(ItKey, Iterator)) -> [{ItKey, emqx_ds:next_result(Iterator)}].
split(#beam{iterators = Its, pack = end_of_stream}) ->
    [{ItKey, {ok, end_of_stream}} || {ItKey, _Iter} <- Its];
split(#beam{iterators = Its, pack = {error, _, _} = Err}) ->
    [{ItKey, Err} || {ItKey, _Iter} <- Its];
split(#beam{iterators = Its, pack = Pack}) ->
    split(Its, Pack, 0, []).

%% This function checks pending requests in the `FromTab' and either
%% dispatches them as a beam or passes them to `OnNomatch' callback if
%% there's nothing to dispatch.
-spec form_beams(
    s(),
    fun((emqx_ds:message_key()) -> [Req]),
    fun(([Req]) -> _),
    fun(([Req]) -> _),
    emqx_ds:message_key(),
    stream_scan_return()
) -> boolean() when Req :: poll_req(_ItKey, _It).
form_beams(S, GetF, OnMatch, OnNomatch, StartKey, {ok, EndKey, Batch}) ->
    do_form_beams(S, GetF, OnMatch, OnNomatch, StartKey, EndKey, Batch);
form_beams(#s{metrics_id = Metrics}, GetF, OnMatch, _OnNomatch, StartKey, Result) ->
    Pack =
        case Result of
            Err = {error, _, _} -> Err;
            {ok, end_of_stream} -> end_of_stream
        end,
    MatchReqs = GetF(StartKey),
    %% Report metrics:
    NFulfilled = length(MatchReqs),
    NFulfilled > 0 andalso
        begin
            emqx_ds_builtin_metrics:inc_poll_requests_fulfilled(Metrics, NFulfilled),
            emqx_ds_builtin_metrics:observe_sharing(Metrics, NFulfilled)
        end,
    %% Execute callbacks:
    OnMatch(MatchReqs),
    %% Split matched requests by destination node:
    ReqsByNode = maps:groups_from_list(
        fun(#poll_req{node = Node}) -> Node end,
        fun(#poll_req{return_addr = RAddr, it = It}) ->
            {RAddr, It}
        end,
        MatchReqs
    ),
    %% Pack requests into beams and serve them:
    maps:foreach(
        fun(Node, Its) ->
            Beam = #beam{
                pack = Pack,
                iterators = Its
            },
            send_out(Node, Beam)
        end,
        ReqsByNode
    ),
    NFulfilled > 0.

-spec do_form_beams(
    s(),
    fun((emqx_ds:message_key()) -> [Req]),
    fun(([Req]) -> _),
    fun(([Req]) -> _),
    emqx_ds:message_key(),
    emqx_ds:message_key(),
    [{emqx_ds:message_key(), emqx_types:message()}]
) -> boolean() when Req :: poll_req(_ItKey, _It).
do_form_beams(
    #s{metrics_id = Metrics, module = CBM, shard = Shard},
    GetF,
    OnMatch,
    OnNomatch,
    StartKey,
    EndKey,
    Batch
) ->
    %% Find iterators that match the start message of the batch (to
    %% handle iterators freshly created by `emqx_ds:make_iterator'):
    Candidates0 = GetF(StartKey),
    %% Search for iterators where `last_seen_key' is equal to key of
    %% any message in the batch:
    Candidates = lists:foldl(
        fun({Key, _Msg}, Acc) ->
            GetF(Key) ++ Acc
        end,
        Candidates0,
        Batch
    ),
    %% Find what poll requests _actually_ have data in the batch. It's
    %% important not to send empty batches to the consumers, so they
    %% don't come back immediately, creating a busy loop:
    {MatchReqs, NoMatchReqs} = filter_candidates(Candidates, Batch),
    ?tp(emqx_ds_beamformer_form_beams, #{match => MatchReqs, no_match => NoMatchReqs}),
    %% Report metrics:
    NFulfilled = length(MatchReqs),
    NFulfilled > 0 andalso
        begin
            emqx_ds_builtin_metrics:inc_poll_requests_fulfilled(Metrics, NFulfilled),
            emqx_ds_builtin_metrics:observe_sharing(Metrics, NFulfilled)
        end,
    %% Execute callbacks:
    OnMatch(MatchReqs),
    OnNomatch(NoMatchReqs),
    %% Split matched requests by destination node:
    ReqsByNode = maps:groups_from_list(fun(#poll_req{node = Node}) -> Node end, MatchReqs),
    %% Pack requests into beams and serve them:
    UpdateIterator = fun(Iterator, NextKey) ->
        CBM:update_iterator(Shard, Iterator, NextKey)
    end,
    maps:foreach(
        fun(Node, Reqs) ->
            Beam = pack(UpdateIterator, EndKey, Reqs, Batch),
            send_out(Node, Beam)
        end,
        ReqsByNode
    ),
    NFulfilled > 0.

-spec pack(
    fun((Iterator, emqx_ds:message_key()) -> Iterator),
    emqx_ds:message_key(),
    [{ItKey, Iterator}],
    [{emqx_ds:message_key(), emqx_types:message()}]
) -> beam(ItKey, Iterator).
pack(UpdateIterator, NextKey, Reqs, Batch) ->
    Pack = [{Key, mk_mask(Reqs, Elem), Msg} || Elem = {Key, Msg} <- Batch],
    UpdatedIterators =
        lists:map(
            fun(#poll_req{it = It0, return_addr = RAddr}) ->
                {ok, It} = UpdateIterator(It0, NextKey),
                {RAddr, It}
            end,
            Reqs
        ),
    #beam{
        iterators = UpdatedIterators,
        pack = Pack
    }.

split([], _Pack, _N, Acc) ->
    Acc;
split([{ItKey, It} | Rest], Pack, N, Acc0) ->
    Msgs = [
        {MsgKey, Msg}
     || {MsgKey, Mask, Msg} <- Pack,
        is_member(N, Mask)
    ],
    case Msgs of
        [] -> logger:warning("Empty batch ~p", [ItKey]);
        _ -> ok
    end,
    Acc = [{ItKey, {ok, It, Msgs}} | Acc0],
    split(Rest, Pack, N + 1, Acc).

-spec is_member(non_neg_integer(), dispatch_mask()) -> boolean().
is_member(N, Mask) ->
    <<_:N, Val:1, _/bitstring>> = Mask,
    Val =:= 1.

-spec mk_mask([poll_req(_ItKey, _Iterator)], {emqx_ds:message_key(), emqx_types:message()}) ->
    dispatch_mask().
mk_mask(Reqs, Elem) ->
    mk_mask(Reqs, Elem, <<>>).

mk_mask([], _Elem, Acc) ->
    Acc;
mk_mask([#poll_req{msg_matcher = Matcher} | Rest], {Key, Message} = Elem, Acc) ->
    Val =
        case Matcher(Key, Message) of
            true -> 1;
            false -> 0
        end,
    mk_mask(Rest, Elem, <<Acc/bitstring, Val:1>>).

filter_candidates(Reqs, Messages) ->
    lists:partition(
        fun(#poll_req{msg_matcher = Matcher}) ->
            lists:any(
                fun({MsgKey, Msg}) -> Matcher(MsgKey, Msg) end,
                Messages
            )
        end,
        Reqs
    ).

send_out(Node, Beam) ->
    ?tp(debug, beamformer_out, #{
        dest_node => Node,
        beam => Beam
    }),
    %% gen_rpc currently doesn't optimize local casts:
    case node() of
        Node ->
            erlang:spawn(?MODULE, do_dispatch, [Beam]),
            ok;
        _ ->
            emqx_ds_beamsplitter_proto_v1:dispatch(Node, Beam)
    end.

shard_metrics_id({DB, Shard}) ->
    emqx_ds_builtin_metrics:shard_metric_id(DB, Shard).

%%%%%% Old queue:

oldq_new() ->
    ets:new(pending_polls, [duplicate_bag, private, {keypos, 1}]).

oldq_push(OldQueue, Req = #poll_req{stream = S, topic_filter = TF, start_key = Key}) ->
    ets:insert(OldQueue, {{S, TF, Key}, Req}).

oldq_pop(OldQueue, Stream, TopicFilter) ->
    fun(MsgKey) ->
            [Req || {_, Req} <- ets:take(OldQueue, {Stream, TopicFilter, MsgKey})]
    end.

%% It's always worth trying to fulfill the oldest requests first,
%% because they have a better chance of producing a batch that
%% overlaps with other pending requests.
%%
%% This function implements a heuristic that tries to find such poll
%% request. It simply compares the keys (and nothing else) within a
%% small sample of pending polls, and picks request with the smallest
%% key as the starting point.
-spec oldq_find_oldest(ets:tid(), pos_integer()) -> oldq_key().
oldq_find_oldest(Tab, SampleSize) ->
    MS = {{'$1', '_'}, [], ['$1']},
    case ets:select(Tab, [MS], SampleSize) of
        '$end_of_table' ->
            undefined;
        {[Fst | Rest], _Cont} ->
            %% Find poll request key with the minimum start_key:
            lists:foldl(
                fun(E, Acc) ->
                    case element(3, E) < element(3, Acc) of
                        true -> E;
                        false -> Acc
                    end
                end,
                Fst,
                Rest
            )
    end.

%% Dynamic config (currently it's global for all DBs):

cfg_pending_request_limit() ->
    application:get_env(emqx_durable_storage, poll_pending_request_limit, 100_000).

cfg_batch_size() ->
    application:get_env(emqx_durable_storage, poll_batch_size, 100).

cfg_housekeeping_interval() ->
    application:get_env(emqx_durable_storage, beamformer_housekeeping_interval, 1000).

%%================================================================================
%% Tests
%%================================================================================

-ifdef(TEST).

is_member_test_() ->
    [
        ?_assert(is_member(0, <<1:1>>)),
        ?_assertNot(is_member(0, <<0:1>>)),
        ?_assertNot(is_member(5, <<0:10>>)),

        ?_assert(is_member(0, <<255:8>>)),
        ?_assert(is_member(7, <<255:8>>)),

        ?_assertNot(is_member(7, <<0:8, 1:1, 0:10>>)),
        ?_assert(is_member(8, <<0:8, 1:1, 0:10>>)),
        ?_assertNot(is_member(9, <<0:8, 1:1, 0:10>>))
    ].

pack_test_() ->
    UpdateIterator = fun(It0, NextKey) ->
        {ok, setelement(2, It0, NextKey)}
    end,
    Raddr = raddr,
    NextKey = <<"42">>,
    Req1 = #poll_req{
        return_addr = Raddr,
        msg_matcher = fun(_, _) -> true end,
        it = {it1, <<"0">>}
    },
    Req2 = #poll_req{
        return_addr = Raddr,
        msg_matcher = fun(_, _) -> false end,
        it = {it2, <<"1">>}
    },
    Req3 = #poll_req{
        return_addr = Raddr,
        msg_matcher = fun(_, _) -> true end,
        it = {it3, <<"2">>}
    },
    %% Messages:
    M1 = {<<"1">>, #message{id = <<"1">>}},
    M2 = {NextKey, #message{id = <<"2">>}},
    Reqs = [Req1, Req2, Req3],
    [
        ?_assertMatch(
            #beam{
                iterators = [],
                pack = [
                    {<<"1">>, <<>>, #message{id = <<"1">>}},
                    {Next, <<>>, #message{id = <<"2">>}}
                ]
            },
            pack(UpdateIterator, NextKey, [], [M1, M2])
        ),
        ?_assertMatch(
            #beam{
                iterators = [
                    {Raddr, {it1, NextKey}}, {Raddr, {it2, NextKey}}, {Raddr, {it3, NextKey}}
                ],
                pack = [
                    {<<"1">>, <<1:1, 0:1, 1:1>>, #message{id = <<"1">>}},
                    {NextKey, <<1:1, 0:1, 1:1>>, #message{id = <<"2">>}}
                ]
            },
            pack(UpdateIterator, NextKey, Reqs, [M1, M2])
        )
    ].

split_test_() ->
    Always = fun(_It, _Msg) -> true end,
    M1 = {<<"1">>, #message{id = <<"1">>}},
    M2 = {<<"2">>, #message{id = <<"2">>}},
    M3 = {<<"3">>, #message{id = <<"3">>}},
    Its = [{<<"it1">>, it1}, {<<"it2">>, it2}],
    Beam1 = #beam{
        iterators = Its,
        pack = [
            {<<"1">>, <<1:1, 0:1>>, element(2, M1)},
            {<<"2">>, <<0:1, 1:1>>, element(2, M2)},
            {<<"3">>, <<1:1, 1:1>>, element(2, M3)}
        ]
    },
    [{<<"it1">>, Result1}, {<<"it2">>, Result2}] = lists:sort(split(Beam1)),
    [
        ?_assertMatch(
            {ok, it1, [M1, M3]},
            Result1
        ),
        ?_assertMatch(
            {ok, it2, [M2, M3]},
            Result2
        )
    ].

-endif.
