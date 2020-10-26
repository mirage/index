.PHONY: all clean test doc examples bench

all:
	dune build

test:
	dune runtest

examples:
	dune build @examples

clean:
	dune clean

doc:
	dune build @doc

bench:
	@dune exec -- ./bench/bench.exe --json --minimal
