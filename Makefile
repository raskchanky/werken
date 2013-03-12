REBAR=`which rebar || ./rebar`
.PHONY: test rel deps ct
all: deps compile
deps:
	@$(REBAR) get-deps
compile:
	@$(REBAR) compile
test:
	@$(REBAR) skip_deps=true eunit
clean:
	@$(REBAR) clean
rel: deps compile
	@$(REBAR) generate force=1
test.spec:
	cat test.spec.in | sed -e 's,@PATH@,$(PWD),' > $(PWD)/test.spec
ct: test.spec compile
	@$(REBAR) skip_deps=true ct
