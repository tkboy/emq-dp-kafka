PROJECT = emq_dp_kafka
PROJECT_DESCRIPTION = EMQ DataPoint Kafka Bridge
PROJECT_VERSION = 2.3.10

# 依赖项
DEPS = ekaf
dep_ekaf = https://github.com/helpshift/ekaf 8967b8a0cf650c80656d48fcdcd1e1b3c8d8d6a7

BUILD_DEPS = emqttd cuttlefish
dep_emqttd = git https://github.com/emqtt/emqttd master
dep_cuttlefish = git https://github.com/emqtt/cuttlefish

# ERLC_OPTS += +debug_info
# ERLC_OPTS += +'{parse_transform, lager_transform}'

# NO_AUTOPATCH = cuttlefish

COVER = true

include erlang.mk

app:: rebar.config

app.config::
	./deps/cuttlefish/cuttlefish -l info -e etc/ -c etc/emq_dp_kafka.conf -i priv/emq_dp_kafka.schema -d data
