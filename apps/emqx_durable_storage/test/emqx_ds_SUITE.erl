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
-module(emqx_ds_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% A simple smoke test that verifies that opening the DB doesn't crash
t_00_smoke_open(_Config) ->
    ?assertMatch(ok, emqx_ds:open_db(<<"DB1">>, #{})),
    ?assertMatch(ok, emqx_ds:open_db(<<"DB1">>, #{})).

%% A simple smoke test that verifies that storing the messages doesn't
%% crash
t_01_smoke_store(_Config) ->
    DB = <<"default">>,
    ?assertMatch(ok, emqx_ds:open_db(DB, #{})),
    Msg = message(<<"foo/bar">>, <<"foo">>, 0),
    ?assertMatch({ok, _}, emqx_ds:store_batch(DB, [Msg])).

%% A simple smoke test that verifies that getting the list of streams
%% doesn't crash and that iterators can be opened.
t_02_smoke_get_streams_start_iter(_Config) ->
    DB = <<"default">>,
    ?assertMatch(ok, emqx_ds:open_db(DB, #{})),
    StartTime = 0,
    [Stream1 | _] = emqx_ds:get_streams(DB, ['#'], StartTime),
    ?assertMatch({ok, _Iter}, emqx_ds:open_iterator(Stream1, StartTime)).

message(Topic, Payload, PublishedAt) ->
    #message{
        topic = Topic,
        payload = Payload,
        timestamp = PublishedAt,
        id = emqx_guid:gen()
    }.

%% CT callbacks

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [mria, emqx_durable_storage],
        #{work_dir => ?config(priv_dir, Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(TC, Config) ->
    application:ensure_all_started(emqx_durable_storage),
    Config.

end_per_testcase(TC, _Config) ->
    ok = application:stop(emqx_durable_storage).
