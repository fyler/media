ERLANG_ROOT := $(shell erl -eval 'io:format("~s", [code:root_dir()])' -s init stop -noshell)
APPNAME = media
ERL_LIBS:=apps:deps


ERL=erl +A 4 +K true
ifeq (,$(wildcard ./rebar))
	REBAR := $(shell which rebar)
else
	REBAR := ./rebar
endif


all: deps compile

update:
	git pull

deps:
	@$(REBAR) get-deps

compile:
	@$(REBAR) compile

release: clean compile
	@$(REBAR) generate force=1

clean:
	@$(REBAR) clean

run:
	ERL_LIBS=apps:..:deps erl -embedded -args_file files/vm.args -sasl errlog_type error -sname media -boot start_sasl -s media -config files/app.config
