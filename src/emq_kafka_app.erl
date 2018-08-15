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

-module(emq_kafka_app).

-behaviour(application).

%% Application callbacks
%% application 行为的回调，这也是整个插件启停的入口
-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
	% start supervise
	% 启动监督树
    {ok, Sup} = emq_kafka_sup:start_link(),
	% register auth module
	% 注册自定义的认证模块
    ok = emqttd_access_control:register_mod(auth, emq_kafka_auth, []),
	% register acl module
	% 注册自定义的访问控制模块
    ok = emqttd_access_control:register_mod(acl, emq_kafka_acl, []),
	% load kafka module
	% 加载kafka模块
    emq_kafka:load(application:get_all_env()),
	% return OK
	% 返回OK
    {ok, Sup}.

stop(_State) ->
    ok = emqttd_access_control:unregister_mod(auth, emq_kafka_auth),
    ok = emqttd_access_control:unregister_mod(acl, emq_kafka_acl),
    emq_kafka:unload().
