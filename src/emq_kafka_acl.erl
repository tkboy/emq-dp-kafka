%%--------------------------------------------------------------------
%% Copyright (c) 2018 Wen Jing<wenjing2016@gmail.com>, All Rights Reserved.
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

%%--------------------------------------------------------------------
%% 此模块来自emq_plugin_template，其仅仅做访问控制，与kafka无关。
%% 因可能有其他用途，暂时保留
%%--------------------------------------------------------------------
-module(emq_kafka_acl).

-include_lib("emqttd/include/emqttd.hrl").

%% ACL callbacks
-export([init/1, check_acl/2, reload_acl/1, description/0]).

init(Opts) ->
    {ok, Opts}.

check_acl({Client, PubSub, Topic}, _Opts) ->
    io:format("~p: ~p ~p ~p~n", [?MODULE, Client, PubSub, Topic]),
    allow.

reload_acl(_Opts) ->
    ok.

description() -> "ACL Demo Module".
