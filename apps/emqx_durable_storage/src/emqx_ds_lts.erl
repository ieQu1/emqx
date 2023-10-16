%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ds_lts).

%% API:
-export([
    trie_create/1, trie_create/0, trie_restore/2, topic_key/3, match_topics/2, lookup_topic_key/2
]).

%% Debug:
-export([trie_next/3, trie_insert/3, dump_to_dot/2]).

-export_type([options/0, static_key/0, trie/0]).

-include_lib("stdlib/include/ms_transform.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

%% End Of Topic
-define(EOT, []).
-define(PLUS, '+').

-type edge() :: binary() | ?EOT | ?PLUS.

%% Fixed size binary
-type static_key() :: non_neg_integer().

-define(PREFIX, prefix).
-type state() :: static_key() | ?PREFIX.

-type varying() :: [binary() | ?PLUS].

-type msg_storage_key() :: {static_key(), varying()}.

-type threshold_fun() :: fun((non_neg_integer()) -> non_neg_integer()).

-type persist_callback() :: fun((_Key, _Val) -> ok).

-type options() ::
    #{
        persist_callback => persist_callback(),
        static_key_size => pos_integer()
    }.

-record(trie, {
    persist :: persist_callback(),
    static_key_size :: pos_integer(),
    trie :: ets:tid(),
    stats :: ets:tid()
}).

-opaque trie() :: #trie{}.

-record(trans, {
    key :: {state(), edge()},
    next :: state()
}).

%%================================================================================
%% API funcions
%%================================================================================

%% @doc Create an empty trie
-spec trie_create(options()) -> trie().
trie_create(UserOpts) ->
    Defaults = #{
        persist_callback => fun(_, _) -> ok end,
        static_key_size => 8
    },
    #{
        persist_callback := Persist,
        static_key_size := StaticKeySize
    } = maps:merge(Defaults, UserOpts),
    Trie = ets:new(trie, [{keypos, #trans.key}, set, public]),
    Stats = ets:new(stats, [{keypos, 1}, set, public]),
    #trie{
        persist = Persist,
        static_key_size = StaticKeySize,
        trie = Trie,
        stats = Stats
    }.

-spec trie_create() -> trie().
trie_create() ->
    trie_create(#{}).

%% @doc Restore trie from a dump
-spec trie_restore(options(), [{_Key, _Val}]) -> trie().
trie_restore(Options, Dump) ->
    Trie = trie_create(Options),
    lists:foreach(
        fun({{StateFrom, Token}, StateTo}) ->
            trie_insert(Trie, StateFrom, Token, StateTo)
        end,
        Dump
    ),
    Trie.

%% @doc Lookup the topic key. Create a new one, if not found.
-spec topic_key(trie(), threshold_fun(), [binary()]) -> msg_storage_key().
topic_key(Trie, ThresholdFun, Tokens) ->
    do_topic_key(Trie, ThresholdFun, 0, ?PREFIX, Tokens, []).

%% @doc Return an exisiting topic key if it exists.
-spec lookup_topic_key(trie(), [binary()]) -> {ok, msg_storage_key()} | undefined.
lookup_topic_key(Trie, Tokens) ->
    do_lookup_topic_key(Trie, ?PREFIX, Tokens, []).

%% @doc Return list of keys of topics that match a given topic filter
-spec match_topics(trie(), [binary() | '+' | '#']) ->
    [msg_storage_key()].
match_topics(Trie, TopicFilter) ->
    do_match_topics(Trie, ?PREFIX, [], TopicFilter).

%% @doc Dump trie to graphviz format for debugging
-spec dump_to_dot(trie(), file:filename()) -> ok.
dump_to_dot(#trie{trie = Trie, stats = Stats}, Filename) ->
    L = ets:tab2list(Trie),
    {Nodes0, Edges} =
        lists:foldl(
            fun(#trans{key = {From, Label}, next = To}, {AccN, AccEdge}) ->
                Edge = {From, To, Label},
                {[From, To] ++ AccN, [Edge | AccEdge]}
            end,
            {[], []},
            L
        ),
    Nodes =
        lists:map(
            fun(Node) ->
                case ets:lookup(Stats, Node) of
                    [{_, NChildren}] -> ok;
                    [] -> NChildren = 0
                end,
                {Node, NChildren}
            end,
            lists:usort(Nodes0)
        ),
    {ok, FD} = file:open(Filename, [write]),
    Print = fun
        (?PREFIX) -> "prefix";
        (NodeId) -> binary:encode_hex(NodeId)
    end,
    io:format(FD, "digraph {~n", []),
    lists:foreach(
        fun({Node, NChildren}) ->
            Id = Print(Node),
            io:format(FD, "  \"~s\" [label=\"~s : ~p\"];~n", [Id, Id, NChildren])
        end,
        Nodes
    ),
    lists:foreach(
        fun({From, To, Label}) ->
            io:format(FD, "  \"~s\" -> \"~s\" [label=\"~s\"];~n", [Print(From), Print(To), Label])
        end,
        Edges
    ),
    io:format(FD, "}~n", []),
    file:close(FD).

%%================================================================================
%% Internal exports
%%================================================================================

-spec trie_next(trie(), state(), binary() | ?EOT) -> {Wildcard, state()} | undefined when
    Wildcard :: boolean().
trie_next(#trie{trie = Trie}, State, ?EOT) ->
    case ets:lookup(Trie, {State, ?EOT}) of
        [#trans{next = Next}] -> {false, Next};
        [] -> undefined
    end;
trie_next(#trie{trie = Trie}, State, Token) ->
    case ets:lookup(Trie, {State, ?PLUS}) of
        [#trans{next = Next}] ->
            {true, Next};
        [] ->
            case ets:lookup(Trie, {State, Token}) of
                [#trans{next = Next}] -> {false, Next};
                [] -> undefined
            end
    end.

-spec trie_insert(trie(), state(), edge()) -> {Updated, state()} when
    NChildren :: non_neg_integer(),
    Updated :: false | NChildren.
trie_insert(Trie, State, Token) ->
    trie_insert(Trie, State, Token, get_id_for_key(Trie, State, Token)).

%%================================================================================
%% Internal functions
%%================================================================================

-spec trie_insert(trie(), state(), edge(), state()) -> {Updated, state()} when
    NChildren :: non_neg_integer(),
    Updated :: false | NChildren.
trie_insert(#trie{trie = Trie, stats = Stats, persist = Persist}, State, Token, NewState) ->
    Key = {State, Token},
    Rec = #trans{
        key = Key,
        next = NewState
    },
    case ets:insert_new(Trie, Rec) of
        true ->
            ok = Persist(Key, NewState),
            Inc =
                case Token of
                    ?EOT -> 0;
                    ?PLUS -> 0;
                    _ -> 1
                end,
            NChildren = ets:update_counter(Stats, State, {2, Inc}, {State, 0}),
            {NChildren, NewState};
        false ->
            [#trans{next = NextState}] = ets:lookup(Trie, Key),
            {false, NextState}
    end.

-spec get_id_for_key(trie(), state(), edge()) -> static_key().
get_id_for_key(#trie{static_key_size = Size}, _State, _Token) ->
    %% Requirements for the return value:
    %%
    %% It should be globally unique for the `{State, Token}` pair. Other
    %% than that, there's no requirements. The return value doesn't even
    %% have to be deterministic, since the states are saved in the trie.
    %%
    %% The generated value becomes the ID of the topic in the durable
    %% storage. Its size should be relatively small to reduce the
    %% overhead of storing messages.
    %%
    %% If we want to impress computer science crowd, sorry, I mean to
    %% minimize storage requirements, we can even employ Huffman coding
    %% based on the frequency of messages.
    <<Int:(Size * 8)>> = crypto:strong_rand_bytes(Size),
    Int.

%% erlfmt-ignore
-spec do_match_topics(trie(), state(), [binary() | '+'], [binary() | '+' | '#']) ->
          list().
do_match_topics(Trie, State, Varying, []) ->
    case trie_next(Trie, State, ?EOT) of
        {false, Static} -> [{Static, lists:reverse(Varying)}];
        undefined -> []
    end;
do_match_topics(Trie, State, Varying, ['#']) ->
    Emanating = emanating(Trie, State, ?PLUS),
    lists:flatmap(
        fun
            ({?EOT, Static}) ->
                [{Static, lists:reverse(Varying)}];
            ({?PLUS, NextState}) ->
                do_match_topics(Trie, NextState, [?PLUS | Varying], ['#']);
            ({_, NextState}) ->
                do_match_topics(Trie, NextState, Varying, ['#'])
        end,
        Emanating
    );
do_match_topics(Trie, State, Varying, [Level | Rest]) ->
    Emanating = emanating(Trie, State, Level),
    lists:flatmap(
        fun
            ({?EOT, _NextState}) ->
                [];
            ({?PLUS, NextState}) ->
                do_match_topics(Trie, NextState, [Level | Varying], Rest);
            ({_, NextState}) ->
                do_match_topics(Trie, NextState, Varying, Rest)
        end,
        Emanating
    ).

-spec do_lookup_topic_key(trie(), state(), [binary()], [binary()]) ->
    {ok, msg_storage_key()} | undefined.
do_lookup_topic_key(Trie, State, [], Varying) ->
    case trie_next(Trie, State, ?EOT) of
        {false, Static} ->
            {ok, {Static, lists:reverse(Varying)}};
        undefined ->
            undefined
    end;
do_lookup_topic_key(Trie, State, [Tok | Rest], Varying) ->
    case trie_next(Trie, State, Tok) of
        {true, NextState} ->
            do_lookup_topic_key(Trie, NextState, Rest, [Tok | Varying]);
        {false, NextState} ->
            do_lookup_topic_key(Trie, NextState, Rest, Varying);
        undefined ->
            undefined
    end.

do_topic_key(Trie, _, _, State, [], Varying) ->
    %% We reached the end of topic. Assert: Trie node that corresponds
    %% to EOT cannot be a wildcard.
    {_, false, Static} = trie_next_(Trie, State, ?EOT),
    {Static, lists:reverse(Varying)};
do_topic_key(Trie, ThresholdFun, Depth, State, [Tok | Rest], Varying0) ->
    % TODO: it's not necessary to call it every time.
    Threshold = ThresholdFun(Depth),
    Varying =
        case trie_next_(Trie, State, Tok) of
            {NChildren, _, _DiscardState} when is_integer(NChildren), NChildren > Threshold ->
                %% Number of children for the trie node reached the
                %% threshold, we need to insert wildcard here:
                {_, NextState} = trie_insert(Trie, State, ?PLUS),
                [Tok | Varying0];
            {_, false, NextState} ->
                Varying0;
            {_, true, NextState} ->
                %% This topic level is marked as wildcard in the trie,
                %% we need to add it to the varying part of the key:
                [Tok | Varying0]
        end,
    do_topic_key(Trie, ThresholdFun, Depth + 1, NextState, Rest, Varying).

-spec trie_next_(trie(), state(), binary() | ?EOT) -> {New, Wildcard, state()} when
    New :: false | non_neg_integer(),
    Wildcard :: boolean().
trie_next_(Trie, State, Token) ->
    case trie_next(Trie, State, Token) of
        {Wildcard, NextState} ->
            {false, Wildcard, NextState};
        undefined ->
            {Updated, NextState} = trie_insert(Trie, State, Token),
            {Updated, false, NextState}
    end.

%% @doc Return all edges emanating from a node:
%% erlfmt-ignore
-spec emanating(trie(), state(), edge()) -> [{edge(), state()}].
emanating(#trie{trie = Tab}, State, ?PLUS) ->
    ets:select(
        Tab,
        ets:fun2ms(
            fun(#trans{key = {S, Edge}, next = Next}) when S == State ->
                {Edge, Next}
            end
        )
    );
emanating(#trie{trie = Tab}, State, ?EOT) ->
    case ets:lookup(Tab, {State, ?EOT}) of
        [#trans{next = Next}] -> [{?EOT, Next}];
        [] -> []
    end;
emanating(#trie{trie = Tab}, State, Bin) when is_binary(Bin) ->
    [
        {Edge, Next}
     || #trans{key = {_, Edge}, next = Next} <-
            ets:lookup(Tab, {State, ?PLUS}) ++
                ets:lookup(Tab, {State, Bin})
    ].

%%================================================================================
%% Tests
%%================================================================================

-ifdef(TEST).

trie_basic_test() ->
    T = trie_create(),
    ?assertMatch(undefined, trie_next(T, ?PREFIX, <<"foo">>)),
    {1, S1} = trie_insert(T, ?PREFIX, <<"foo">>),
    ?assertMatch({false, S1}, trie_insert(T, ?PREFIX, <<"foo">>)),
    ?assertMatch({false, S1}, trie_next(T, ?PREFIX, <<"foo">>)),

    ?assertMatch(undefined, trie_next(T, ?PREFIX, <<"bar">>)),
    {2, S2} = trie_insert(T, ?PREFIX, <<"bar">>),
    ?assertMatch({false, S2}, trie_insert(T, ?PREFIX, <<"bar">>)),

    ?assertMatch(undefined, trie_next(T, S1, <<"foo">>)),
    ?assertMatch(undefined, trie_next(T, S1, <<"bar">>)),
    {1, S11} = trie_insert(T, S1, <<"foo">>),
    {2, S12} = trie_insert(T, S1, <<"bar">>),
    ?assertMatch({false, S11}, trie_next(T, S1, <<"foo">>)),
    ?assertMatch({false, S12}, trie_next(T, S1, <<"bar">>)),

    ?assertMatch(undefined, trie_next(T, S11, <<"bar">>)),
    {1, S111} = trie_insert(T, S11, <<"bar">>),
    ?assertMatch({false, S111}, trie_next(T, S11, <<"bar">>)).

lookup_key_test() ->
    T = trie_create(),
    {_, S1} = trie_insert(T, ?PREFIX, <<"foo">>),
    {_, S11} = trie_insert(T, S1, <<"foo">>),
    %% Topics don't match until we insert ?EOT:
    ?assertMatch(
        undefined,
        lookup_topic_key(T, [<<"foo">>])
    ),
    ?assertMatch(
        undefined,
        lookup_topic_key(T, [<<"foo">>, <<"foo">>])
    ),
    {_, S10} = trie_insert(T, S1, ?EOT),
    {_, S110} = trie_insert(T, S11, ?EOT),
    ?assertMatch(
        {ok, {S10, []}},
        lookup_topic_key(T, [<<"foo">>])
    ),
    ?assertMatch(
        {ok, {S110, []}},
        lookup_topic_key(T, [<<"foo">>, <<"foo">>])
    ),
    %% The rest of keys still don't match:
    ?assertMatch(
        undefined,
        lookup_topic_key(T, [<<"bar">>])
    ),
    ?assertMatch(
        undefined,
        lookup_topic_key(T, [<<"bar">>, <<"foo">>])
    ).

wildcard_lookup_test() ->
    T = trie_create(),
    {1, S1} = trie_insert(T, ?PREFIX, <<"foo">>),
    %% Plus doesn't increase the number of children
    {0, S11} = trie_insert(T, S1, ?PLUS),
    {1, S111} = trie_insert(T, S11, <<"foo">>),
    %% ?EOT doesn't increase the number of children
    {0, S1110} = trie_insert(T, S111, ?EOT),
    ?assertMatch(
        {ok, {S1110, [<<"bar">>]}},
        lookup_topic_key(T, [<<"foo">>, <<"bar">>, <<"foo">>])
    ),
    ?assertMatch(
        {ok, {S1110, [<<"quux">>]}},
        lookup_topic_key(T, [<<"foo">>, <<"quux">>, <<"foo">>])
    ),
    ?assertMatch(
        undefined,
        lookup_topic_key(T, [<<"foo">>])
    ),
    ?assertMatch(
        undefined,
        lookup_topic_key(T, [<<"foo">>, <<"bar">>])
    ),
    ?assertMatch(
        undefined,
        lookup_topic_key(T, [<<"foo">>, <<"bar">>, <<"bar">>])
    ),
    ?assertMatch(
        undefined,
        lookup_topic_key(T, [<<"bar">>, <<"foo">>, <<"foo">>])
    ),
    {_, S10} = trie_insert(T, S1, ?EOT),
    ?assertMatch(
        {ok, {S10, []}},
        lookup_topic_key(T, [<<"foo">>])
    ).

%% erlfmt-ignore
topic_key_test() ->
    T = trie_create(),
    try
        Threshold = 3,
        ThresholdFun = fun(0) -> 1000;
                          (_) -> Threshold
                       end,
        %% Test that bottom layer threshold is high:
        lists:foreach(
          fun(I) ->
                  {_, []} = test_key(T, ThresholdFun, [I, 99, 99, 99])
          end,
          lists:seq(1, 10)),
        %% Test adding children on the 2nd level:
        lists:foreach(
          fun(I) ->
                  case test_key(T, ThresholdFun, [1, I, 1]) of
                      {_, []} when I < Threshold ->
                          ok;
                      {_, [Var]} ->
                          ?assertEqual(Var, integer_to_binary(I))
                  end
          end,
          lists:seq(1, 100)),
        %% This doesn't affect 2nd level with a different prefix:
        {_, []} = test_key(T, ThresholdFun, [2, 1, 1]),
        %% Now create another level of +:
        lists:foreach(
          fun(I) ->
                  case test_key(T, ThresholdFun, [1, 42, 1, I, 42]) of
                      {_, [<<"42">>]} when I =< Threshold -> %% TODO: off by 1 error
                          ok;
                      {_, [<<"42">>, Var]} ->
                          ?assertEqual(Var, integer_to_binary(I));
                      Ret ->
                          error({Ret, I})
                  end
          end,
          lists:seq(1, 100))
    after
        dump_to_dot(T, filename:join("_build", atom_to_list(?FUNCTION_NAME) ++ ".dot"))
    end.

%% erlfmt-ignore
topic_match_test() ->
    T = trie_create(),
    try
        Threshold = 2,
        ThresholdFun = fun(0) -> 1000;
                          (_) -> Threshold
                       end,
        {S1, []} = test_key(T, ThresholdFun, [1]),
        {S11, []} = test_key(T, ThresholdFun, [1, 1]),
        {S12, []} = test_key(T, ThresholdFun, [1, 2]),
        {S111, []} = test_key(T, ThresholdFun, [1, 1, 1]),
        %% Match concrete topics:
        assert_match_topics(T, [1], [{S1, []}]),
        assert_match_topics(T, [1, 1], [{S11, []}]),
        assert_match_topics(T, [1, 1, 1], [{S111, []}]),
        %% Match topics with +:
        assert_match_topics(T, [1, '+'], [{S11, []}, {S12, []}]),
        assert_match_topics(T, [1, '+', 1], [{S111, []}]),
        %% Match topics with #:
        assert_match_topics(T, [1, '#'], [{S1, []}, {S11, []}, {S12, []}, {S111, []}]),
        assert_match_topics(T, [1, 1, '#'], [{S11, []}, {S111, []}]),
        %% Now add learned wildcards:
        {S21, []} = test_key(T, ThresholdFun, [2, 1]),
        {S22, []} = test_key(T, ThresholdFun, [2, 2]),
        {S2_, [<<"3">>]} = test_key(T, ThresholdFun, [2, 3]),
        {S2_11, [_]} = test_key(T, ThresholdFun, [2, 1, 1, 1]),
        {S2_12, [_]} = test_key(T, ThresholdFun, [2, 1, 1, 2]),
        {S2_1_, [_, _]} = test_key(T, ThresholdFun, [2, 1, 1, 3]),
        %% Check matching:
        assert_match_topics(T, [2, 2],
                            [{S22, []}, {S2_, [<<"2">>]}]),
        assert_match_topics(T, [2, '+'],
                            [{S22, []}, {S21, []}, {S2_, ['+']}]),
        assert_match_topics(T, [2, 1, 1, 2],
                            [{S2_12, [<<"1">>]},
                             {S2_1_, [<<"1">>, <<"2">>]}]),
        assert_match_topics(T, [2, '#'],
                            [{S21, []}, {S22, []},
                             {S2_, ['+']},
                             {S2_11, ['+']}, {S2_12, ['+']},
                             {S2_1_, ['+', '+']}]),
        ok
    after
        dump_to_dot(T, filename:join("_build", atom_to_list(?FUNCTION_NAME) ++ ".dot"))
    end.

-define(keys_history, topic_key_history).

%% erlfmt-ignore
assert_match_topics(Trie, Filter0, Expected) ->
    Filter = lists:map(fun(I) when is_integer(I) -> integer_to_binary(I);
                          (I) -> I
                       end,
                       Filter0),
    Matched = match_topics(Trie, Filter),
    ?assertMatch( #{missing := [], unexpected := []}
                , #{ missing    => Expected -- Matched
                   , unexpected => Matched -- Expected
                   }
                , Filter
                ).

%% erlfmt-ignore
test_key(Trie, Threshold, Topic0) ->
    Topic = [integer_to_binary(I) || I <- Topic0],
    Ret = topic_key(Trie, Threshold, Topic),
    Ret = topic_key(Trie, Threshold, Topic), %% Test idempotency
    case get(?keys_history) of
        undefined  -> OldHistory = #{};
        OldHistory -> ok
    end,
    %% Test that the generated keys are always unique for the topic:
    History = maps:update_with(
                Ret,
                fun(Old) ->
                        case Old =:= Topic of
                            true  -> Old;
                            false -> error(#{'$msg' => "Duplicate key!", key => Ret, old_topic => Old, new_topic => Topic})
                        end
                end,
                Topic,
                OldHistory),
    put(?keys_history, History),
    {ok, Ret} = lookup_topic_key(Trie, Topic),
    Ret.

-endif.
